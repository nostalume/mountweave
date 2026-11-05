package s3

import (
	"bytes"
	"context"
	"encoding/json"
	core "github.com/nostalume/mountweave"
	"testing"
)

func TestCheckpointGrowthAndRecoveryEdges(t *testing.T) {
	ctx := context.Background()
	for _, count := range []int{10, 1000, 10000} {
		binding := resumeBinding{Scope: "account", Bucket: "bucket", Key: "file", Source: "version", Length: int64(count) * 5 << 20, PartSize: 5 << 20}
		binding.Replace = count == 10
		var log bytes.Buffer
		appendRecord := func(_ context.Context, data []byte) error { _, err := log.Write(data); return err }
		emit := func(record uploadRecord) {
			t.Helper()
			if err := appendUpload(ctx, appendRecord, record); err != nil {
				t.Fatal(err)
			}
		}
		emit(uploadRecord{Version: 2, Kind: "start", Binding: &binding, Token: "attempt", Phase: "initializing"})
		if binding.Replace {
			binding.Replace = false
		} else {
			binding.When = core.IfAbsent()
		}
		emit(uploadRecord{Version: 2, Kind: "phase", Phase: "uploading", UploadID: "upload"})
		for n := 1; n <= count; n++ {
			emit(uploadRecord{Version: 2, Kind: "part", Part: n, ETag: "0123456789abcdef0123456789abcdef"})
		}
		emit(uploadRecord{Version: 3, Kind: "phase", Phase: "committing", UploadID: "upload"})
		state, err := restoreUpload(log.Bytes(), binding, count)
		if err != nil || state.Phase != "committing" || len(state.Parts) != count {
			t.Fatalf("recovery: %+v %v", state, err)
		}
		if log.Len() > 512+100*count {
			t.Fatalf("metadata grew beyond the linear envelope: %d", log.Len())
		}
		t.Logf("parts=%d recovery_bytes=%d", count, log.Len())
		if _, err = restoreUpload(log.Bytes()[:log.Len()-2], binding, count); err == nil {
			t.Fatal("torn committed record accepted")
		}
		wrong := binding
		wrong.Scope = "another-account"
		if _, err = restoreUpload(log.Bytes(), wrong, count); err == nil {
			t.Fatal("scope change accepted")
		}
		wrong = binding
		wrong.When = core.IfPresent()
		if _, err = restoreUpload(log.Bytes(), wrong, count); err == nil {
			t.Fatal("predicate change accepted")
		}
		var policy core.WritePolicy
		for _, guard := range []string{`{"macth":"\"old\""}`, `{"match":""}`, `{"match":null}`, `null`} {
			if err := json.Unmarshal([]byte(`{"mode":"publish","when":`+guard+`}`), &policy); err == nil {
				t.Fatal("malformed durable guard became unconditional", guard)
			}
		}
		emit(uploadRecord{Version: 2, Kind: "part", Part: 1, ETag: "different"})
		if _, err = restoreUpload(log.Bytes(), binding, count); err == nil {
			t.Fatal("receipt after commit barrier accepted")
		}
	}
}
