package mobilepan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	core "github.com/nostalume/mountweave"
)

func TestWebsiteCreateReceipt(t *testing.T) {
	const digest = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	for _, scenario := range []struct {
		name, occupant, initial, completion string
		length                              int64
		puts, commits                       int
		effect                              core.Effect
		code                                core.Code
		cause                               error
	}{
		{"ordinary", "file", "", `{"accepted":true,"fileId":"file"}`, 5, 1, 1, core.Applied, "", nil},
		{"existing", "other", `{"exist":true,"fileId":"other"}`, "", 5, 0, 0, core.Unchanged, core.AlreadyExists, nil},
		{"competing completion", "other", "", `{"accepted":false,"code":"04010330"}`, 5, 1, 1, core.Unchanged, core.AlreadyExists, nil},
		{"lost acknowledgment", "", "", "", 5, 1, 1, core.Unknown, "", context.Canceled},
		{"source EOF", "", "", "", 6, 0, 0, core.Unchanged, "", io.ErrUnexpectedEOF},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			staging := t.TempDir()
			account := strings.Repeat("f", 64)
			var grants, puts, commits int
			website := Website{Release: func() error { return nil }, Upload: &WebsiteUploadOptions{StagingDirectory: staging, MaxStagingBytes: 7}}
			website.Call = func(_ context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
				var request struct {
					Route, Account string
					Data           struct {
						ParentFileID, Name, Type, FileRenameMode, ContentHash, ContentHashAlgorithm string
						FileID, UploadID                                                            string
						Size                                                                        int64
						PartInfos                                                                   []websiteUploadPart
					}
				}
				if json.Unmarshal(args, &request) != nil || request.Account != account {
					t.Fatal("invalid account or upload request")
				}
				var data string
				switch request.Route {
				case "list":
					data = `{"items":[]}`
					if grants > 0 && scenario.occupant != "" {
						data = fmt.Sprintf(`{"items":[{"fileId":%q,"parentFileId":"/","name":"sample","type":"file","size":5}]}`, scenario.occupant)
					}
				case "upload-grant":
					grants++
					r := request.Data
					if scenario.length != 5 || r.ParentFileID != "/" || r.Name != "sample" || r.Type != "file" || r.FileRenameMode != "refuse" ||
						r.Size != 5 || r.ContentHash != digest || r.ContentHashAlgorithm != "SHA256" || len(r.PartInfos) != 1 || r.PartInfos[0].Size != 5 {
						t.Fatal("grant request did not bind staged input and exact name")
					}
					data = scenario.initial
					if data == "" {
						data = `{"fileId":"file","parentFileId":"/","name":"sample","type":"file","uploadId":"upload","partCount":1,"part":{"partNumber":1,"partSize":null,"uploadUrl":"https://body.fixture/put?signature=fixture"}}`
					}
				case "upload-complete":
					commits++
					if puts != 1 || request.Data.FileID != "file" || request.Data.UploadID != "upload" || request.Data.ContentHash != digest {
						t.Fatal("commit was not bound to the transferred input")
					}
					data = scenario.completion
					if data == "" {
						cancel()
						return nil, context.Canceled
					}
				default:
					t.Fatalf("unexpected route %q", request.Route)
				}
				return json.RawMessage(`{"success":true,"data":` + data + `}`), nil
			}
			content := &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
				puts++
				body, err := io.ReadAll(req.Body)
				if err != nil || req.Method != http.MethodPut || req.URL.Host != "body.fixture" || string(body) != "hello" ||
					req.ContentLength != 5 || req.Header.Get("Cookie") != "" || req.Header.Get("Authorization") != "" {
					t.Fatal("signed PUT did not receive only the staged bytes")
				}
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			})}
			d, err := website.Open(ctx, account, "/", content)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Install("/phone", d); err != nil {
				t.Fatal(err)
			}
			got, err := c.Write(ctx, "fixture", core.Path("/phone/sample"), strings.NewReader("hello"), &scenario.length)
			if got.Effect != scenario.effect || puts != scenario.puts || commits != scenario.commits ||
				(scenario.cause != nil && !errors.Is(err, scenario.cause)) || (scenario.code != "" && core.CodeOf(err) != scenario.code) ||
				(scenario.cause == nil && scenario.code == "" && err != nil) {
				t.Fatalf("Write=%+v, %v, PUT=%d, complete=%d", got, err, puts, commits)
			}
			entries, err := os.ReadDir(staging)
			if err != nil || len(entries) != 0 {
				t.Fatal("upload staging file not cleaned")
			}
		})
	}
}
