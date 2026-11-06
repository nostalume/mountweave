// Package fileview projects a protocol's lexical tree view onto Core addresses.
// It does not resolve mounts or provide a physical filesystem sandbox.
package fileview

import (
	"strings"

	core "github.com/nostalume/mountweave"
)

type View struct {
	Root core.Address
	Home string
}

func New(root core.Address, home string) (View, error) {
	if home == "" {
		home = "/"
	}
	v := View{root, home}
	if root.Namespace() != core.TreeNamespace || root.Validate() != nil || core.ValidatePath(home) != nil {
		return View{}, core.Fail(core.InvalidRequest, nil)
	}
	if _, _, err := v.Resolve("/", home); err != nil {
		return View{}, core.Fail(core.InvalidRequest, err)
	}
	return v, nil
}

// Resolve accepts protocol dot segments without decoding or changing name bytes.
// Traversal above the export root is rejected rather than clamped.
func (v View) Resolve(cwd, name string) (core.Address, string, error) {
	if !strings.HasPrefix(name, "/") {
		name = strings.TrimSuffix(cwd, "/") + "/" + name
	}
	parts := make([]string, 0, 8)
	for _, p := range strings.Split(name, "/") {
		switch p {
		case "", ".":
		case "..":
			if len(parts) == 0 {
				return core.Address{}, "", core.Fail(core.InvalidPath, nil)
			}
			parts = parts[:len(parts)-1]
		default:
			parts = append(parts, p)
		}
	}
	visible := "/" + strings.Join(parts, "/")
	joined := strings.TrimSuffix(v.Root.Value(), "/") + visible
	if visible == "/" {
		joined = v.Root.Value()
	}
	a := core.Path(joined)
	return a, visible, a.Validate()
}

// ResolveChange protects the export anchor even when it is a storage child.
func (v View) ResolveChange(cwd, name string) (core.Address, error) {
	address, visible, err := v.Resolve(cwd, name)
	if err == nil && visible == "/" {
		return core.Address{}, core.Fail(core.PermissionDenied, nil)
	}
	return address, err
}

func PageEntries(n int) (int, error) {
	if n == 0 {
		n = core.DefaultPage
	}
	if n < 1 || n > core.MaxPage {
		return 0, core.Fail(core.InvalidRequest, nil)
	}
	return n, nil
}

func NameBytes(e core.Entry) int {
	return len(e.Name) + len(e.Address.Value()) + len(e.Address.Space()) +
		len(e.Evidence.Validator.Domain) + len(e.Evidence.Validator.Tag) + len(e.Evidence.Version)
}
