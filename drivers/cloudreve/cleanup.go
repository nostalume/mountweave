package cloudreve

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/onedrive"
)

type pendingUpload struct {
	ID, URI, BodyURL string
	reconcile        bool
	cause            error
}

func (d *Driver) closeBody(body io.Closer) error {
	if body == nil {
		return nil
	}
	err := body.Close()
	if err != nil {
		d.mu.Lock()
		d.failedBodies = append(d.failedBodies, body)
		d.mu.Unlock()
	}
	return err
}

func (d *Driver) keepUpload(id uint64, session pendingUpload, cause error) {
	d.mu.Lock()
	session.cause = errors.Join(d.pending[id].cause, cause)
	d.pending[id] = session
	d.mu.Unlock()
}

func (d *Driver) retireBody(ctx context.Context, session pendingUpload) (bool, error) {
	if session.BodyURL == "" {
		return false, placeholderRemains()
	}
	// Cloudreve's id+URI cancellation can mutate a replacement when its KV
	// session expires. Only the admitted OneDrive capability is private cleanup;
	// its result never proves removal of the logical file or Cloudreve metadata.
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, session.BodyURL, nil)
	if err != nil {
		return false, err
	}
	response, err := d.content.Do(req)
	if err != nil {
		return false, networkError(err)
	}
	if response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusNotFound {
		return true, d.closeBody(response.Body)
	}
	options := onedrive.Options{MaxResponseBytes: d.opts.MaxResponseBytes, CloseBody: d.closeBody}
	failure, cleanup := options.Failure(response)
	return false, errors.Join(failure, cleanup)
}

func placeholderRemains() error {
	return core.Fail(core.UnsupportedOperation, errors.New("Cloudreve has no conditional cancellation for the logical placeholder; its file effect remains"))
}

// Cleanup retries owned cancellation after active writes have ended. An unknown
// initiation or attempted callback needs native reconciliation, never a path
// delete or a guessed cancellation of a possibly published file.
func (d *Driver) Cleanup(ctx context.Context) error {
	d.mu.Lock()
	pending := make(map[uint64]pendingUpload, len(d.pending))
	for id, session := range d.pending {
		pending[id] = session
	}
	d.mu.Unlock()
	var err error
	for id, session := range pending {
		if session.ID == "" || session.reconcile {
			err = errors.Join(err, core.Fail(core.UpstreamFailure, errors.Join(fmt.Errorf("upload %d for %s requires native reconciliation", id, session.URI), session.cause)))
			continue
		}
		confirmed, failure := d.retireBody(ctx, session)
		err = errors.Join(err, failure)
		if !confirmed {
			continue
		}
		// The ordinary caller already received its Incomplete file effect.
		// A retired body no longer occupies private admission or runtime state.
		err = errors.Join(err, placeholderRemains())
		d.mu.Lock()
		delete(d.pending, id)
		d.mu.Unlock()
	}
	return err
}

// Close releases owned response bodies; prepared clients remain borrowed.
// Unconfirmed sessions stay owned until Cleanup or native reconciliation.
func (d *Driver) Close() error {
	d.closed.Store(true)
	d.mu.Lock()
	bodies := d.failedBodies
	d.failedBodies = nil
	d.mu.Unlock()
	var err error
	for _, body := range bodies {
		err = errors.Join(err, d.closeBody(body))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, session := range d.pending {
		err = errors.Join(err, core.Fail(core.UpstreamFailure, errors.Join(fmt.Errorf("upload %d for %s has unconfirmed cleanup", id, session.URI), session.cause)))
	}
	return err
}
