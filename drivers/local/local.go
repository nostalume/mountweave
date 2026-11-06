package local

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	core "github.com/nostalume/mountweave"
)

// Options configures private storage and native complete-file publication.
// MaxStagingBytes bounds this instance's reserved unpublished logical bytes.
// MaxConcurrentCommits limits only native publication commits per instance;
// zero adds no limit. Waiting retains staging bytes and honors cancellation.
type Options struct {
	StagingDirectory     string
	MaxStagingBytes      int64
	MaxConcurrentCommits int
}

type Driver struct {
	root          *os.Root
	mu            sync.Mutex
	failedFiles   []*os.File
	cleanupErr    error
	staging       *os.Root
	stagingParent *os.File
	stagingLimit  int64
	stagingBytes  int64
	failedStages  []*stagedFile
	commits       chan struct{}
	names         nameLocks
}

func New(directory string, options ...Options) (_ *Driver, err error) {
	var opts Options
	if len(options) > 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if len(options) == 1 {
		opts = options[0]
	}
	if opts.MaxConcurrentCommits < 0 {
		return nil, core.Fail(core.InvalidRequest, errors.New("commit capacity must not be negative"))
	}
	if opts.MaxStagingBytes < 0 || (opts.StagingDirectory == "") != (opts.MaxStagingBytes == 0) {
		return nil, core.Fail(core.InvalidRequest, errors.New("staging directory and positive byte budget must be configured together"))
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	d := &Driver{root: root, stagingLimit: opts.MaxStagingBytes}
	if opts.MaxConcurrentCommits > 0 {
		d.commits = make(chan struct{}, opts.MaxConcurrentCommits)
	}
	if opts.StagingDirectory == "" {
		return d, nil
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, d.Close())
		}
	}()
	served, err := canonicalDirectory(directory)
	if err != nil {
		return nil, err
	}
	staging, err := canonicalDirectory(opts.StagingDirectory)
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(served, staging)
	if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator)) && !filepath.IsAbs(relative) {
		return nil, core.Fail(core.InvalidRequest, errors.New("staging must be outside the served root"))
	}
	d.staging, err = os.OpenRoot(staging)
	if err != nil {
		return nil, err
	}
	d.stagingParent, err = d.staging.OpenFile(".", readFlags(), 0)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func canonicalDirectory(name string) (string, error) {
	name, err := filepath.EvalSymlinks(name)
	if err != nil {
		return "", err
	}
	return filepath.Abs(name)
}
func (d *Driver) Close() error {
	err := d.root.Close()
	if d.stagingParent != nil {
		err = errors.Join(err, d.stagingParent.Close())
	}
	if d.staging != nil {
		err = errors.Join(err, d.staging.Close())
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return errors.Join(err, d.cleanupErr)
}
func localError(err error) error {
	if err == nil {
		return nil
	}
	var public *core.Error
	if errors.As(err, &public) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, fs.ErrNotExist) {
		return core.Fail(core.NotFound, err)
	}
	if errors.Is(err, fs.ErrPermission) {
		return core.Fail(core.UpstreamAccessDenied, err)
	}
	return core.Fail(core.UpstreamFailure, err)
}

func metadata(name string, info fs.FileInfo) (core.Metadata, error) {
	m := core.Metadata{Name: name, Kind: core.File}
	t := info.ModTime().UTC()
	if !t.IsZero() {
		m.ModifiedAt = &t
	}
	if info.IsDir() {
		m.Kind = core.Directory
	} else if info.Mode().IsRegular() {
		n := info.Size()
		m.Size = &n
	} else {
		return core.Metadata{}, core.Fail(core.UnrepresentableName, errors.New("only regular files and directories are readable"))
	}
	return m, nil
}
func leaf(rel string) string {
	if rel == "/" {
		return ""
	}
	return path.Base(rel)
}
func (d *Driver) Stat(ctx context.Context, relAddress core.NativeAddress, selections ...core.ReadSelection) (meta core.Metadata, err error) {
	if len(selections) > 1 {
		return meta, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if selection.Version != "" {
		return meta, core.Fail(core.UnsupportedOperation, nil)
	}
	defer func() {
		if err == nil {
			err = selection.Check(meta.Evidence)
		}
	}()
	return d.statPath(ctx, relAddress, inspectChild)
}

func (d *Driver) Lstat(ctx context.Context, address core.NativeAddress) (core.Metadata, error) {
	return d.statPath(ctx, address, leafChild)
}

func (d *Driver) statPath(ctx context.Context, address core.NativeAddress, mode childMode) (meta core.Metadata, err error) {
	if address.Namespace() != core.TreeNamespace {
		return meta, core.Fail(core.UnsupportedOperation, nil)
	}
	if err = ctx.Err(); err != nil {
		return meta, err
	}
	if address.Value() == "/" {
		info, err := d.root.Stat(".")
		if err != nil {
			return meta, localError(err)
		}
		return metadata("", info)
	}
	parent, name, err := d.openParent(ctx, address.Value())
	if err != nil {
		return meta, err
	}
	defer func() { err = errors.Join(err, d.closeFile(parent)) }()
	return d.statChild(ctx, parent, name, mode)
}

func (d *Driver) statChild(ctx context.Context, parent *os.File, name string, mode childMode) (core.Metadata, error) {
	file, info, err := d.openChild(parent, name, mode)
	if err != nil {
		return core.Metadata{}, localError(err)
	}
	if err = errors.Join(d.closeFile(file), ctx.Err()); err != nil {
		return core.Metadata{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return core.Metadata{Name: name, Kind: core.SymbolicLink}, nil
	}
	return metadata(name, info)
}
func (d *Driver) Open(ctx context.Context, relAddress core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Binding == core.PinnedVersion {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	wanted := options.Range
	if options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if relAddress.Namespace() != core.TreeNamespace {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	rel := relAddress.Value()
	if options.Mode == core.LiveEdit {
		return d.edit(ctx, rel, options)
	}
	file, info, err := d.readFile(ctx, rel)
	if err != nil {
		return core.Opened{}, localError(err)
	}
	fail := func(err error) (core.Opened, error) { return core.Opened{}, errors.Join(err, d.closeFile(file)) }
	meta, err := metadata(leaf(rel), info)
	if err != nil {
		return fail(err)
	}
	if meta.Kind != core.File {
		return fail(core.Fail(core.NotFile, nil))
	}
	if err := options.Selection.Check(meta.Evidence); err != nil {
		return fail(err)
	}
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	if options.Mode == core.BoundRead {
		return core.Opened{Metadata: meta, File: &boundFile{file: file, driver: d, name: leaf(rel), readable: true}, Binding: core.BoundObject}, nil
	}
	length := info.Size()
	opened := core.Opened{Metadata: meta, Body: file, Length: &length}
	if wanted != nil {
		actual, err := wanted.Resolve(length)
		if err != nil {
			return fail(err)
		}
		count := actual.End - actual.Start + 1
		opened.Length = &count
		opened.Range = &actual
		opened.Body = &section{Reader: io.NewSectionReader(file, actual.Start, count), file: file}
	}
	return opened, nil
}

// boundFile retains the actual fd. The original name is response metadata, not
// an address used to reopen or refresh this resource.
type boundFile struct {
	file                 *os.File
	driver               *Driver
	name                 string
	readable, appendOnly bool
}

func (f *boundFile) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	if !f.readable {
		return 0, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n, err := f.file.ReadAt(p, offset)
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
	}
	return n, err
}

func (f *boundFile) Stat(ctx context.Context) (core.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return core.Metadata{}, err
	}
	info, err := f.file.Stat()
	if err != nil {
		return core.Metadata{}, localError(err)
	}
	return metadata(f.name, info)
}

func (f *boundFile) Close() error { return f.driver.closeFile(f.file) }

func (d *Driver) readFile(ctx context.Context, rel string) (*os.File, os.FileInfo, error) {
	if rel == "/" {
		return nil, nil, core.Fail(core.NotFile, nil)
	}
	parent, name, err := d.openParent(ctx, rel)
	if err != nil {
		return nil, nil, err
	}
	file, info, err := d.openChild(parent, name, readChild)
	closeErr := d.closeFile(parent)
	if err != nil || closeErr != nil {
		if file != nil {
			closeErr = errors.Join(closeErr, d.closeFile(file))
		}
		return nil, nil, errors.Join(localError(err), closeErr)
	}
	return file, info, nil
}

type section struct {
	io.Reader
	file *os.File
}

func (s *section) Close() error { return s.file.Close() }
func (d *Driver) Enumerate(ctx context.Context, relAddress core.NativeQuery) (core.Iterator, error) {
	if relAddress.Address().Namespace() != core.TreeNamespace || relAddress.Delimiter() != "" || relAddress.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	file, err := d.openDirectory(ctx, relAddress.Address().Value())
	if err != nil {
		return nil, err
	}
	return &iterator{driver: d, file: file}, nil
}

type iterator struct {
	driver *Driver
	file   *os.File
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	return i.driver.readDirectory(ctx, i.file, n)
}

func (i *iterator) Close() error { return i.driver.closeFile(i.file) }

var _ core.Driver = (*Driver)(nil)
