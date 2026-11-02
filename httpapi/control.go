package httpapi

import (
	"context"
	"encoding/json"
	"net/http"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/control"
)

type aliasValue struct {
	Prefix string `json:"prefix"`
	Base   string `json:"base"`
}
type definitionValue struct {
	ID       string       `json:"id,omitempty"`
	Revision uint64       `json:"revision,omitempty"`
	Prefix   string       `json:"prefix"`
	Driver   string       `json:"driver"`
	Enabled  bool         `json:"enabled"`
	Aliases  []aliasValue `json:"aliases"`
	Config   any          `json:"config,omitempty"`
}
type saveDefinition struct {
	ExpectedRevision *uint64           `json:"expected_revision"`
	Prefix           string            `json:"prefix"`
	Driver           string            `json:"driver"`
	Config           json.RawMessage   `json:"config,omitempty"`
	Secrets          map[string]string `json:"secrets,omitempty"`
	Enabled          bool              `json:"enabled"`
	Aliases          []aliasValue      `json:"aliases,omitempty"`
}

func summary(d control.Definition) definitionValue {
	v := definitionValue{ID: d.ID, Revision: d.Revision, Prefix: d.Prefix, Driver: d.Driver, Enabled: d.Enabled, Aliases: make([]aliasValue, len(d.Aliases))}
	for i, a := range d.Aliases {
		v.Aliases[i] = aliasValue{a.Prefix, a.Base}
	}
	return v
}

type activationValue struct {
	ID                string   `json:"id"`
	Prefix            string   `json:"prefix"`
	RequestedRevision uint64   `json:"requested_revision"`
	Revision          uint64   `json:"revision"`
	Active            bool     `json:"active"`
	Retiring          bool     `json:"retiring"`
	Done              bool     `json:"done"`
	Aliases           []string `json:"aliases"`
	PendingCleanup    bool     `json:"pending_cleanup"`
	Error             string   `json:"error,omitempty"`
	RuntimeError      string   `json:"runtime_error,omitempty"`
}

func activation(a control.Activation, err error) activationValue {
	aliases := append([]string{}, a.Aliases...)
	return activationValue{ID: a.ID, Prefix: a.Prefix, RequestedRevision: a.RequestedRevision, Revision: a.Revision, Active: a.Runtime.Active, Retiring: a.Runtime.Retiring, Done: a.Runtime.Done, Aliases: aliases, PendingCleanup: a.PendingCleanup, Error: errorCode(err), RuntimeError: errorCode(a.Runtime.Err)}
}
func (h *Handler) definition(ctx context.Context, p core.Principal, d control.Definition) (definitionValue, error) {
	v := summary(d)
	if h.options.ProjectConfig != nil {
		config, err := h.options.ProjectConfig(ctx, p, d)
		v.Config = config
		return v, err
	}
	return v, nil
}
func (h *Handler) controlRoute(w http.ResponseWriter, r *http.Request, id string, p core.Principal) {
	c := h.options.Control
	if c == nil {
		writeProblem(w, r, id, core.Fail(core.UnsupportedOperation, nil), nil)
		return
	}
	query, err := singleQuery(r, "id")
	if err != nil {
		writeProblem(w, r, id, err, nil)
		return
	}
	if r.URL.Path == "/api/v1/mounts" || r.URL.Path == "/api/v1/mounts/restore" {
		if len(query) != 0 {
			writeProblem(w, r, id, core.Fail(core.InvalidRequest, nil), nil)
			return
		}
		if r.URL.Path == "/api/v1/mounts/restore" {
			if err = noBody(r); err != nil {
				writeProblem(w, r, id, err, nil)
				return
			}
			results, restoreErr := c.Restore(r.Context(), p)
			values := make([]activationValue, len(results))
			for i, result := range results {
				values[i] = activation(result.Activation, result.Err)
			}
			if restoreErr != nil {
				writeDomainProblem(w, r, id, restoreErr, values)
				return
			}
			respond(w, r, id, 200, values)
			return
		}
		definitions, loadErr := c.List(r.Context(), p)
		if loadErr != nil {
			writeDomainProblem(w, r, id, loadErr, nil)
			return
		}
		values := make([]definitionValue, len(definitions))
		for i, d := range definitions {
			values[i] = summary(d)
		}
		respond(w, r, id, 200, values)
		return
	}
	configID := query.Get("id")
	if configID == "" {
		writeProblem(w, r, id, core.Fail(core.InvalidRequest, nil), nil)
		return
	}
	switch r.URL.Path {
	case "/api/v1/mounts/definition":
		if r.Method == http.MethodGet {
			d, err := c.Load(r.Context(), p, configID)
			if err != nil {
				writeDomainProblem(w, r, id, err, nil)
				return
			}
			value, err := h.definition(r.Context(), p, d)
			if err != nil {
				writeDomainProblem(w, r, id, err, nil)
				return
			}
			respond(w, r, id, 200, value)
			return
		}
		var input saveDefinition
		if err = h.readJSON(w, r, &input); err != nil {
			writeProblem(w, r, id, err, nil)
			return
		}
		if input.ExpectedRevision == nil {
			writeProblem(w, r, id, core.Fail(core.InvalidRequest, nil), nil)
			return
		}
		d := control.Definition{ID: configID, Prefix: input.Prefix, Driver: input.Driver, Config: input.Config, Secrets: input.Secrets, Enabled: input.Enabled, Aliases: make([]control.Alias, len(input.Aliases))}
		for i, a := range input.Aliases {
			d.Aliases[i] = control.Alias{Prefix: a.Prefix, Base: a.Base}
		}
		saved, err := c.Save(r.Context(), p, d, *input.ExpectedRevision)
		if err != nil {
			writeDomainProblem(w, r, id, err, nil)
			return
		}
		// Saving returns only safe confirmation fields, independently of output
		// projectors. A projection failure cannot obscure a successful commit.
		respond(w, r, id, 200, summary(saved))
	case "/api/v1/mounts/activation":
		var a control.Activation
		if r.Method == http.MethodGet {
			a, err = c.Observe(r.Context(), p, configID)
		} else {
			var input struct {
				Revision uint64 `json:"revision"`
			}
			if err = h.readJSON(w, r, &input); err != nil {
				writeProblem(w, r, id, err, nil)
				return
			}
			if input.Revision == 0 {
				writeProblem(w, r, id, core.Fail(core.InvalidRequest, nil), nil)
				return
			}
			a, err = c.Apply(r.Context(), p, configID, input.Revision)
		}
		value := activation(a, err)
		if err != nil {
			writeDomainProblem(w, r, id, err, value)
			return
		}
		respond(w, r, id, 200, value)
	case "/api/v1/mounts/stop":
		if err = noBody(r); err != nil {
			writeProblem(w, r, id, err, nil)
			return
		}
		a, err := c.Stop(r.Context(), p, configID)
		value := activation(a, err)
		if err != nil {
			writeDomainProblem(w, r, id, err, value)
			return
		}
		respond(w, r, id, 200, value)
	}
}
