package feishudrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	core "github.com/nostalume/mountweave"
)

const (
	exportPollInterval = 5 * time.Second
	exportWait         = 50 * time.Second
)

func supportsExport(source, format string) bool {
	switch source {
	case "doc":
		return format == "docx" || format == "pdf"
	case "docx":
		return format == "docx" || format == "pdf" || format == "markdown"
	case "sheet":
		return format == "xlsx"
	case "bitable":
		return format == "xlsx" || format == "base"
	case "slides":
		return format == "pptx" || format == "pdf"
	}
	return false
}

type exportResult struct {
	Source string `json:"token"`
	Type   string `json:"type"`
	Format string `json:"file_extension"`
	Token  string `json:"file_token"`
	Size   *int64 `json:"file_size"`
	Status *int   `json:"job_status"`
	Reason string `json:"job_error_msg"`
}

func (d *Driver) export(ctx context.Context, file item) (exportResult, error) {
	// The control deadline ends before the caller opens the result body.
	ctx, cancel := context.WithTimeout(ctx, exportWait)
	defer cancel()

	format := d.exports[file.exportType]
	payload, err := json.Marshal(map[string]string{"token": file.Token, "type": file.exportType, "file_extension": format})
	if err != nil {
		return exportResult{}, err
	}
	resp, _, err := d.request(ctx, http.MethodPost, "/export_tasks", "", bytes.NewReader(payload))
	if err != nil {
		return exportResult{}, err
	}
	var started struct {
		Ticket string `json:"ticket"`
	}
	if err := d.envelope(resp, &started); err != nil {
		return exportResult{}, err
	}
	if !validToken(started.Ticket) {
		return exportResult{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Feishu export omitted a valid ticket"))
	}
	query := url.Values{"token": {file.Token}}
	route := "/export_tasks/" + url.PathEscape(started.Ticket) + "?" + query.Encode()
	for {
		resp, _, err := d.request(ctx, http.MethodGet, route, "", nil)
		if err != nil {
			return exportResult{}, err
		}
		var poll struct {
			Result exportResult `json:"result"`
		}
		if err := d.envelope(resp, &poll); err != nil {
			return exportResult{}, err
		}
		result := poll.Result
		if result.Status == nil || (result.Source != "" && result.Source != file.Token) {
			return exportResult{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Feishu export status or source"))
		}
		switch *result.Status {
		case 0:
			// A success state without a file token is not downloadable yet.
			if result.Token != "" {
				if !validToken(result.Token) || (result.Type != "" && result.Type != file.exportType) ||
					(result.Format != "" && result.Format != format) || (result.Size != nil && *result.Size < 0) {
					return exportResult{}, core.Fail(core.InvalidUpstreamResponse, errors.New("Feishu export result does not match the request"))
				}
				return result, nil
			}
		case 1, 2:
		default:
			return exportResult{}, core.Fail(core.UpstreamFailure, fmt.Errorf("Feishu export failed with status %d: %s", *result.Status, result.Reason))
		}
		timer := time.NewTimer(exportPollInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return exportResult{}, ctx.Err()
		}
	}
}
