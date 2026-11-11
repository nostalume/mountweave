// Package mcptool encodes stateless MCP tool calls and validates their JSON replies.
package mcptool

import (
	"encoding/json"
	"errors"

	core "github.com/nostalume/mountweave"
)

func MarshalCall(id uint64, name string, arguments any) ([]byte, error) {
	type params struct {
		Name      string `json:"name"`
		Arguments any    `json:"arguments"`
	}
	return json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Method  string `json:"method"`
		Params  params `json:"params"`
	}{"2.0", id, "tools/call", params{name, arguments}})
}

// Reply separates a JSON-RPC error code from the first text tool result.
type Reply struct {
	ErrorCode *int
	IsError   bool
	Text      string
}

func DecodeReply(data []byte, id uint64) (Reply, error) {
	var envelope struct {
		JSONRPC string
		ID      uint64
		Error   *struct{ Code *int }
		Result  *struct {
			IsError bool
			Content []struct{ Type, Text string }
		}
	}
	if json.Unmarshal(data, &envelope) != nil || envelope.JSONRPC != "2.0" ||
		envelope.ID != id || (envelope.Result == nil) == (envelope.Error == nil) ||
		envelope.Error != nil && envelope.Error.Code == nil {
		return Reply{}, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid MCP reply"))
	}
	if envelope.Error != nil {
		return Reply{ErrorCode: envelope.Error.Code}, nil
	}
	reply := Reply{IsError: envelope.Result.IsError}
	for _, part := range envelope.Result.Content {
		if part.Type == "text" {
			reply.Text = part.Text
			break
		}
	}
	return reply, nil
}
