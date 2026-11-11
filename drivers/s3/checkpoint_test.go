package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	core "github.com/nostalume/mountweave"
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
		policy := core.Publish()
		if !binding.Replace {
			policy = core.Publish(core.IfAbsent())
		}
		binding.Replace, binding.Policy = false, &policy
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
		changed := core.Publish(core.IfPresent())
		wrong.Policy = &changed
		if _, err = restoreUpload(log.Bytes(), wrong, count); err == nil {
			t.Fatal("predicate change accepted")
		}
		var decoded core.WritePolicy
		for _, guard := range []string{`{"macth":"\"old\""}`, `{"match":""}`, `{"match":null}`, `null`} {
			if err := json.Unmarshal([]byte(`{"mode":"publish","when":`+guard+`}`), &decoded); err == nil {
				t.Fatal("malformed durable guard became unconditional", guard)
			}
		}
		proof := core.Evidence{Validator: core.Validator{Domain: "s3-etag", Tag: `"native"`}}
		emit(uploadRecord{Version: 5, Kind: "phase", Phase: "done", UploadID: "upload", Evidence: &proof})
		if state, err := restoreUpload(log.Bytes(), binding, count); err != nil || state.Phase != "done" || state.Evidence != proof {
			t.Fatal("new phase lost the original publication binding", state, err)
		}
		emit(uploadRecord{Version: 2, Kind: "part", Part: 1, ETag: "different"})
		if _, err = restoreUpload(log.Bytes(), binding, count); err == nil {
			t.Fatal("receipt after commit barrier accepted")
		}
		if count == 10 {
			log.Reset()
			direct := binding
			creation := core.WritePolicy{}
			direct.Policy = &creation
			emit(uploadRecord{Version: 5, Kind: "start", Binding: &direct, Token: "direct", Phase: "initializing"})
			if _, err := restoreUpload(log.Bytes(), direct, count); err != nil {
				t.Fatal("direct policy did not survive its checkpoint", err)
			}
			if _, err := restoreUpload(log.Bytes(), binding, count); err == nil {
				t.Fatal("direct checkpoint accepted complete publication")
			}
			emit(uploadRecord{Version: 5, Kind: "phase", Phase: "uploading", UploadID: "upload"})
			for n := 1; n <= count; n++ {
				emit(uploadRecord{Version: 5, Kind: "part", Part: n, ETag: "native"})
			}
			emit(uploadRecord{Version: 5, Kind: "phase", Phase: "committing", UploadID: "upload"})
			emit(uploadRecord{Version: 2, Kind: "phase", Phase: "done", UploadID: "upload"})
			if _, err := restoreUpload(log.Bytes(), direct, count); err == nil {
				t.Fatal("old record version bypassed a new publication receipt")
			}
			log.Reset()
			direct.Policy = nil
			emit(uploadRecord{Version: 5, Kind: "start", Binding: &direct, Token: "missing", Phase: "initializing"})
			if _, err := restoreUpload(log.Bytes(), binding, count); err == nil {
				t.Fatal("new checkpoint omitted its explicit policy")
			}
		}
	}
}
