package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type moduleChecksum struct {
	Path, Version, Sum, GoModSum string
}

func verify(ctx context.Context, repo, revision, project string, p plan) (err error) {
	temp, err := os.MkdirTemp("", "mountweave-verify-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(temp)) }()
	mirror := filepath.Join(temp, "source.git")
	if _, err := command(ctx, repo, nil, "git", "clone", "--mirror", "--shared", "--quiet", "--", repo, mirror); err != nil {
		return err
	}
	var refs strings.Builder
	for _, module := range p.Modules {
		fmt.Fprintf(&refs, "create refs/tags/%s %s\n", module.Tag, revision)
	}
	git := exec.CommandContext(ctx, "git", "update-ref", "--stdin")
	git.Dir = mirror
	git.Stdin = strings.NewReader(refs.String())
	if out, err := git.CombinedOutput(); err != nil {
		return fmt.Errorf("create temporary tags: %w\n%s", err, out)
	}

	env, err := candidateEnvironment(ctx, repo, temp, mirror, project)
	if err != nil {
		return err
	}
	sums, err := candidateChecksums(ctx, repo, revision, project)
	if err != nil {
		return err
	}
	versions := make(map[string]string, len(p.Modules))
	for _, module := range p.Modules {
		versions[module.Path] = module.Version
	}

	jobs := make(chan int)
	failures := make([]error, len(p.Modules))
	checksums := make([]moduleChecksum, len(p.Modules))
	var workers sync.WaitGroup
	for range min(4, runtime.GOMAXPROCS(0), len(p.Modules)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				module := p.Modules[i]
				dir := filepath.Join(temp, strconv.Itoa(i))
				err := verifyModule(ctx, dir, env, sums, project, versions, module)
				if err == nil {
					checksums[i], err = candidateChecksum(ctx, dir, env, module)
				}
				if err != nil {
					failures[i] = fmt.Errorf("%s@%s: %w", module.Path, module.Version, err)
				}
			}
		}()
	}
	for i := range p.Modules {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	if err := errors.Join(failures...); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Revision, GOOS, Go string
		Modules            []moduleChecksum
	}{revision, runtime.GOOS, runtime.Version(), checksums})
}

func candidateChecksum(ctx context.Context, dir string, env []string, module releaseModule) (moduleChecksum, error) {
	var checksum moduleChecksum
	data, err := command(ctx, dir, env, goTool, "mod", "download", "-json", module.Path+"@"+module.Version)
	if err != nil {
		return checksum, err
	}
	if err := json.Unmarshal(data, &checksum); err != nil {
		return checksum, err
	}
	if checksum.Path != module.Path || checksum.Version != module.Version || checksum.Sum == "" || checksum.GoModSum == "" {
		return checksum, errors.New("download did not report checksums for the candidate")
	}
	return checksum, nil
}

func candidateEnvironment(ctx context.Context, repo, temp, mirror, project string) ([]string, error) {
	data, err := command(ctx, repo, goEnv, goTool, "env", "-json", "GOMODCACHE", "GOPROXY", "GONOPROXY", "GONOSUMDB")
	if err != nil {
		return nil, err
	}
	var settings struct{ GOMODCACHE, GOPROXY, GONOPROXY, GONOSUMDB string }
	if err := json.Unmarshal(data, &settings); err != nil {
		return nil, err
	}
	proxy := settings.GOPROXY
	// Reuse already downloaded dependencies. The isolated Go cache still
	// checks their hashes using go.sum or the configured checksum database.
	if cached := filepath.Join(settings.GOMODCACHE, "cache", "download"); filepath.IsAbs(cached) {
		proxy = candidateFileURL(cached) + "," + proxy
	}
	env := append([]string{}, goEnv...)
	env = append(env,
		"GO111MODULE=on",
		"GOFLAGS=-modcacherw",
		"GOMODCACHE="+filepath.Join(temp, "modules"),
		"GOPROXY="+proxy,
		"GONOPROXY="+strings.Trim(project+","+settings.GONOPROXY, ","),
		"GONOSUMDB="+strings.Trim(project+","+settings.GONOSUMDB, ","),
		"GIT_TERMINAL_PROMPT=0",
	)
	// Extend any inherited process-scoped Git configuration.
	count, _ := strconv.Atoi(os.Getenv("GIT_CONFIG_COUNT"))
	if count < 0 {
		count = 0
	}
	for _, item := range [][2]string{
		{"protocol.file.allow", "always"},
		{"url." + candidateFileURL(mirror) + ".insteadOf", "https://" + project},
		{"url." + candidateFileURL(mirror) + ".insteadOf", "https://" + project + ".git"},
	} {
		index := strconv.Itoa(count)
		env = append(env, "GIT_CONFIG_KEY_"+index+"="+item[0], "GIT_CONFIG_VALUE_"+index+"="+item[1])
		count++
	}
	return append(env, "GIT_CONFIG_COUNT="+strconv.Itoa(count)), nil
}

func candidateFileURL(path string) string {
	path = filepath.ToSlash(path)
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return (&url.URL{Scheme: "file", Path: path}).String()
}

func candidateChecksums(ctx context.Context, repo, revision, project string) ([]byte, error) {
	files, err := command(ctx, repo, nil, "git", "ls-tree", "-r", "--name-only", "-z", revision)
	if err != nil {
		return nil, err
	}
	var sums bytes.Buffer
	for _, file := range strings.Split(string(files), "\x00") {
		if filepath.Base(file) != "go.sum" {
			continue
		}
		data, err := command(ctx, repo, nil, "git", "show", revision+":"+file)
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			path, _, _ := strings.Cut(line, " ")
			if line != "" && !candidateProject(path, project) {
				sums.WriteString(line + "\n")
			}
		}
	}
	return sums.Bytes(), nil
}

func candidateProject(path, project string) bool {
	return path == project || strings.HasPrefix(path, project+"/")
}

func verifyModule(ctx context.Context, dir string, env []string, sums []byte, project string, versions map[string]string, module releaseModule) error {
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	content := fmt.Sprintf("module releasecheck.invalid/host\n\ngo %s\n\nrequire %s %s\n", module.Go, module.Path, module.Version)
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(content), 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "go.sum"), sums, 0600); err != nil {
		return err
	}
	before, err := candidateBuildList(ctx, dir, env, project, module.Go, versions)
	if err != nil {
		return err
	}
	if before[module.Path] != module.Version {
		return fmt.Errorf("resolved %s at %q, want %s", module.Path, before[module.Path], module.Version)
	}
	for _, dep := range module.Requires {
		if before[dep.Path] == "" {
			return fmt.Errorf("build list omitted declared dependency %s", dep.Path)
		}
	}
	if _, err := command(ctx, dir, env, goTool, "build", "-mod=mod", "-p=2", module.Path+"/..."); err != nil {
		return err
	}
	after, err := candidateBuildList(ctx, dir, env, project, module.Go, versions)
	if err != nil {
		return err
	}
	if !maps.Equal(before, after) {
		return fmt.Errorf("build changed project dependencies: %v -> %v", before, after)
	}
	return nil
}

func candidateBuildList(ctx context.Context, dir string, env []string, project, minimumGo string, versions map[string]string) (map[string]string, error) {
	output, err := command(ctx, dir, env, goTool, "list", "-mod=mod", "-m", "-json", "all")
	if err != nil {
		return nil, err
	}
	selected := make(map[string]string)
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var mod struct {
			Path, Version, GoVersion string
			Main                     bool
			Replace                  *struct{}
		}
		err := decoder.Decode(&mod)
		if errors.Is(err, io.EOF) {
			return selected, nil
		}
		if err != nil {
			return nil, err
		}
		if mod.Replace != nil {
			return nil, fmt.Errorf("%s has an active replacement", mod.Path)
		}
		if mod.Main && mod.GoVersion != minimumGo {
			return nil, fmt.Errorf("dependency graph needs Go %s, module declares Go %s", mod.GoVersion, minimumGo)
		}
		if want := versions[mod.Path]; want != "" && mod.Version != want {
			return nil, fmt.Errorf("resolved %s at %s, want candidate %s", mod.Path, mod.Version, want)
		}
		if candidateProject(mod.Path, project) {
			selected[mod.Path] = mod.Version
		}
	}
}
