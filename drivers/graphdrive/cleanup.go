package graphdrive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) closeBody(body io.Closer) error {
	if body == nil {
		return nil
	}
	if d.readOnly {
		return d.privateResponses.CloseBody(body)
	}
	err := body.Close()
	if err != nil {
		d.mu.Lock()
		d.failedBodies = append(d.failedBodies, body)
		d.mu.Unlock()
	}
	return err
}

type pendingUpload struct {
	Target, Session string
	cause           error
}

func (d *Driver) keepUpload(id uint64, resource pendingUpload, cause error) {
	d.mu.Lock()
	resource.cause = errors.Join(d.pending[id].cause, cause)
	d.pending[id] = resource
	d.mu.Unlock()
}

// Cleanup retries owned session cleanup after active writes have ended. It never
// deletes a file by name or interprets a missing session as a publication receipt.
func (d *Driver) Cleanup(ctx context.Context) error {
	d.mu.Lock()
	pending := make(map[uint64]pendingUpload, len(d.pending))
	for id, resource := range d.pending {
		pending[id] = resource
	}
	d.mu.Unlock()
	var err error
	for id, resource := range pending {
		if resource.Session == "" {
			err = errors.Join(err, core.Fail(core.UpstreamFailure, errors.Join(fmt.Errorf("upload %d for %s requires native reconciliation", id, resource.Target), resource.cause)))
			continue
		}
		status, failure := d.abortSession(ctx, resource.Session)
		err = errors.Join(err, failure)
		if status != http.StatusNoContent && status != http.StatusNotFound {
			continue
		}
		d.mu.Lock()
		delete(d.pending, id)
		d.mu.Unlock()
	}
	return err
}

func (d *Driver) abortSession(ctx context.Context, target string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, target, nil)
	if err != nil {
		return 0, core.Fail(core.InvalidUpstreamResponse, err)
	}
	response, err := d.content.Do(req)
	if err != nil {
		return 0, networkError(err)
	}
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return response.StatusCode, errors.Join(d.responseFailure(response))
	}
	return response.StatusCode, d.closeBody(response.Body)
}

// Close releases only owned responses. Clients and their authentication remain
// borrowed; unconfirmed native sessions remain owned until Cleanup succeeds.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	bodies := d.failedBodies
	d.failedBodies = nil
	d.mu.Unlock()
	err := d.privateResponses.Retry()
	for _, body := range bodies {
		err = errors.Join(err, d.closeBody(body))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, resource := range d.pending {
		err = errors.Join(err, core.Fail(core.UpstreamFailure, errors.Join(fmt.Errorf("upload %d for %s has unconfirmed cleanup", id, resource.Target), resource.cause)))
	}
	return err
}
