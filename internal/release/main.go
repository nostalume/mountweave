// Package main prepares and verifies releases of modules in this repository.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
)

type requirement struct {
	Path    string
	Version string
}

type releaseModule struct {
	Dir, Path, Go, Version, Tag string
	Requires                    []requirement
}

type plan struct {
	Base    string
	Modules []releaseModule
}

type version [3]int

func (v version) String() string {
	return fmt.Sprintf("v%d.%d.%d", v[0], v[1], v[2])
}

func parseVersion(text string) (version, bool) {
	var v version
	parts := strings.Split(strings.TrimPrefix(text, "v"), ".")
	if len(parts) != len(v) {
		return v, false
	}
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return v, false
		}
		v[i] = n
	}
	return v, v[0] <= 1 && v.String() == text
}

type tag struct {
	name, commit string
	version      version
}

type repository struct {
	dir, head string
	modules   map[string]releaseModule
	byPath    map[string]string
	tags      map[string]bool
	latest    map[string]tag
	dirs      []string
}

const usage = "usage: release plan [-all] [-bump patch|minor] [directory@version ...] | prepare PLAN | verify PLAN"

var goEnv = []string{"GOWORK=off", "GOTOOLCHAIN=local"}
var goTool = func() string {
	name := "go"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(runtime.GOROOT(), "bin", name)
}()

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	repo, err := readRepository(ctx)
	if err != nil {
		return err
	}
	switch args[0] {
	case "plan":
		options := flag.NewFlagSet("plan", flag.ContinueOnError)
		all := options.Bool("all", false, "include every tracked module")
		bump := options.String("bump", "", "version increment for modules with a release")
		if err := options.Parse(args[1:]); err != nil {
			return err
		}
		if err := repo.checkManifests(ctx); err != nil {
			return err
		}
		targets, err := repo.targets(ctx, *all, *bump, options.Args())
		if err != nil {
			return err
		}
		p, err := repo.buildPlan(repo.head, targets)
		if err != nil {
			return err
		}
		output := json.NewEncoder(os.Stdout)
		output.SetIndent("", "  ")
		return output.Encode(p)
	case "prepare", "verify":
		if len(args) != 2 {
			return errors.New(usage)
		}
		p, err := loadPlan(args[1])
		if err != nil {
			return err
		}
		return repo.apply(ctx, args[0], p)
	default:
		return errors.New(usage)
	}
}

func command(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && len(exit.Stderr) != 0 {
			return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(exit.Stderr)))
		}
		return nil, fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return output, nil
}

func (r *repository) git(ctx context.Context, args ...string) ([]byte, error) {
	return command(ctx, r.dir, nil, "git", args...)
}

func readRepository(ctx context.Context) (*repository, error) {
	r := &repository{
		modules: make(map[string]releaseModule), byPath: make(map[string]string),
		tags: make(map[string]bool), latest: make(map[string]tag),
	}
	root, err := r.git(ctx, "rev-parse", "--show-toplevel")
	if err != nil {
		return nil, err
	}
	r.dir = strings.TrimSpace(string(root))
	head, err := r.git(ctx, "rev-parse", "HEAD")
	if err != nil {
		return nil, err
	}
	r.head = strings.TrimSpace(string(head))
	files, err := r.git(ctx, "ls-files", "-z", "--", "go.mod", ":(glob)**/go.mod")
	if err != nil {
		return nil, err
	}
	names := splitPaths(files)
	modules := make([]releaseModule, len(names))
	failures := make([]error, len(names))
	slots := make(chan struct{}, 8)
	var pending sync.WaitGroup
	for i, name := range names {
		slots <- struct{}{}
		pending.Add(1)
		go func() {
			defer pending.Done()
			defer func() { <-slots }()
			modules[i], failures[i] = r.readModule(ctx, name)
		}()
	}
	pending.Wait()
	for i, module := range modules {
		if failures[i] != nil {
			return nil, failures[i]
		}
		if _, exists := r.byPath[module.Path]; exists {
			return nil, fmt.Errorf("duplicate module path %s", module.Path)
		}
		r.byPath[module.Path], r.modules[module.Dir] = module.Dir, module
		r.dirs = append(r.dirs, module.Dir)
	}
	rootModule, ok := r.modules["."]
	if !ok {
		return nil, errors.New("missing root go.mod")
	}
	for dir, module := range r.modules {
		if dir != "." && module.Path != rootModule.Path+"/"+dir {
			return nil, fmt.Errorf("%s: module path %s does not match its directory", dir, module.Path)
		}
	}
	tags, err := r.git(ctx, "for-each-ref", "--format=%(refname:short)%00%(objectname)%00%(*objectname)", "refs/tags")
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(tags)), "\n") {
		fields := strings.Split(strings.TrimSuffix(line, "\r"), "\x00")
		if len(fields) != 3 {
			continue
		}
		name, commit := fields[0], fields[1]
		if fields[2] != "" {
			commit = fields[2]
		}
		r.tags[name] = true
		dir := path.Dir(name)
		if _, ok := r.modules[dir]; !ok {
			continue
		}
		if v, ok := parseVersion(path.Base(name)); ok {
			latest, exists := r.latest[dir]
			if !exists || slices.Compare(v[:], latest.version[:]) > 0 {
				r.latest[dir] = tag{name, commit, v}
			}
		}
	}
	return r, nil
}

func (r *repository) readModule(ctx context.Context, name string) (releaseModule, error) {
	data, err := command(ctx, r.dir, goEnv, goTool, "mod", "edit", "-json", name)
	if err != nil {
		return releaseModule{}, err
	}
	var manifest struct {
		Module  struct{ Path string }
		Go      string
		Require []requirement
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return releaseModule{}, fmt.Errorf("%s: %w", name, err)
	}
	if manifest.Module.Path == "" || manifest.Go == "" {
		return releaseModule{}, fmt.Errorf("%s: module path and Go version are required", name)
	}
	slices.SortFunc(manifest.Require, func(a, b requirement) int { return strings.Compare(a.Path, b.Path) })
	return releaseModule{Dir: path.Dir(name), Path: manifest.Module.Path, Go: manifest.Go, Requires: manifest.Require}, nil
}

func splitPaths(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	return strings.Split(strings.TrimSuffix(string(data), "\x00"), "\x00")
}

func (r *repository) checkManifests(ctx context.Context) error {
	if _, err := r.git(ctx, "diff", "--quiet", r.head, "--", "go.mod", ":(glob)**/go.mod"); err != nil {
		return fmt.Errorf("go.mod files must match the committed revision: %w", err)
	}
	return nil
}

func (r *repository) targets(ctx context.Context, all bool, bump string, specs []string) (map[string]string, error) {
	targets := make(map[string]string)
	if len(specs) != 0 {
		if all || bump != "" {
			return nil, errors.New("explicit versions cannot be combined with -all or -bump")
		}
		for _, spec := range specs {
			dir, v, ok := strings.Cut(spec, "@")
			if !ok || targets[dir] != "" {
				return nil, fmt.Errorf("invalid or duplicate release target %q", spec)
			}
			targets[dir] = v
		}
		return targets, nil
	}
	if bump != "patch" && bump != "minor" {
		return nil, errors.New("choose -bump patch or -bump minor, or specify each target version")
	}
	changes := make(map[string]map[string]bool)
	for _, dir := range r.dirs {
		previous, exists := r.latest[dir]
		if !exists {
			targets[dir] = "v0.1.0"
			continue
		}
		if !all {
			changed, ok := changes[previous.commit]
			if !ok {
				files, err := r.git(ctx, "diff", "--name-only", "--no-renames", "-z", previous.commit, r.head)
				if err != nil {
					return nil, err
				}
				changed = make(map[string]bool)
				for _, file := range splitPaths(files) {
					owner := path.Dir(file)
					for r.modules[owner].Path == "" {
						owner = path.Dir(owner)
					}
					changed[owner] = true
				}
				changes[previous.commit] = changed
			}
			if !changed[dir] {
				continue
			}
		}
		next := previous.version
		if bump == "minor" {
			next[1]++
			next[2] = 0
		} else {
			next[2]++
		}
		targets[dir] = next.String()
	}
	return targets, nil
}

func (r *repository) buildPlan(base string, targets map[string]string) (plan, error) {
	p := plan{Base: base, Modules: []releaseModule{}}
	for dir, text := range targets {
		_, exists := r.modules[dir]
		_, valid := parseVersion(text)
		if !exists || !valid || r.tags[path.Join(dir, text)] {
			return p, fmt.Errorf("invalid or already released module version %s@%s", dir, text)
		}
	}
	visited := make(map[string]bool)
	var visit func(string) error
	visit = func(dir string) error {
		if visited[dir] || targets[dir] == "" {
			return nil
		}
		visited[dir] = true
		module := r.modules[dir]
		module.Version, module.Tag = targets[dir], path.Join(dir, targets[dir])
		module.Requires = nil
		for _, dep := range r.modules[dir].Requires {
			depDir, internal := r.byPath[dep.Path]
			if !internal {
				if strings.HasPrefix(dep.Path, r.modules["."].Path+"/") {
					return fmt.Errorf("%s requires unknown project module %s", module.Path, dep.Path)
				}
				continue
			}
			if v := targets[depDir]; v != "" {
				dep.Version = v
				if err := visit(depDir); err != nil {
					return err
				}
			} else if !r.tags[path.Join(depDir, dep.Version)] {
				return fmt.Errorf("%s requires unavailable %s@%s; include %s in the batch", module.Path, dep.Path, dep.Version, depDir)
			}
			module.Requires = append(module.Requires, dep)
		}
		p.Modules = append(p.Modules, module)
		return nil
	}
	for _, dir := range r.dirs {
		if err := visit(dir); err != nil {
			return p, err
		}
	}
	return p, nil
}

func loadPlan(name string) (plan, error) {
	var p plan
	data, err := os.ReadFile(name)
	if err != nil {
		return p, err
	}
	if err := json.Unmarshal(data, &p); err != nil {
		return p, err
	}
	if _, err := hex.DecodeString(p.Base); err != nil || len(p.Base) != 40 && len(p.Base) != 64 {
		return p, errors.New("plan must identify a source commit")
	}
	return p, nil
}

func (r *repository) apply(ctx context.Context, operation string, p plan) error {
	if len(p.Modules) == 0 {
		return errors.New("release plan is empty")
	}
	if operation == "prepare" && r.head != p.Base {
		return errors.New("HEAD has changed since planning; generate a new plan")
	}
	if operation == "verify" {
		if _, err := r.git(ctx, "merge-base", "--is-ancestor", p.Base, r.head); err != nil {
			return fmt.Errorf("candidate must descend from the planned commit: %w", err)
		}
		if err := r.checkManifests(ctx); err != nil {
			return err
		}
	}
	targets := make(map[string]string)
	for _, module := range p.Modules {
		targets[module.Dir] = module.Version
	}
	expected, err := r.buildPlan(p.Base, targets)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(p, expected) {
		return errors.New("plan differs from the current module graph; generate a new plan")
	}
	for _, module := range p.Modules {
		var updates []string
		for _, dep := range r.modules[module.Dir].Requires {
			for _, want := range module.Requires {
				if dep.Path == want.Path && dep.Version != want.Version {
					updates = append(updates, "-require="+want.Path+"@"+want.Version)
				}
			}
		}
		if len(updates) == 0 {
			continue
		}
		name := filepath.Join(module.Dir, "go.mod")
		if operation == "verify" {
			return fmt.Errorf("%s has pending dependency updates; prepare and commit the candidate", name)
		}
		args := append([]string{"mod", "edit"}, updates...)
		if _, err := command(ctx, r.dir, goEnv, goTool, append(args, name)...); err != nil {
			return err
		}
	}
	if operation == "verify" {
		if err := verify(ctx, r.dir, r.head, r.modules["."].Path, p); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Verified %d modules from %s on %s with %s.\n", len(p.Modules), r.head, runtime.GOOS, runtime.Version())
		return nil
	}
	fmt.Fprintf(os.Stdout, "Prepared %d modules from %s. Review and commit the candidate before verification.\n", len(p.Modules), p.Base)
	return nil
}
