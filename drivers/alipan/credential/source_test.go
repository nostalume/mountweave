package credential_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nostalume/mountweave/drivers/alipan/credential"
)

func fresh() credential.Token {
	return credential.Token{Access: "new-access", Refresh: "new-refresh", ExpiresAt: time.Now().Add(time.Hour)}
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not acknowledge")
	}
}
func TestSharedRefreshSurvivesWaiterCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var refreshes, saves atomic.Int32
	source, err := credential.New(credential.Record{Token: credential.Token{Refresh: "old-refresh"}}, func(ctx context.Context, token string) (credential.Token, error) {
		refreshes.Add(1)
		if token != "old-refresh" {
			t.Error("wrong refresh input")
		}
		close(started)
		select {
		case <-release:
			return fresh(), nil
		case <-ctx.Done():
			return credential.Token{}, ctx.Err()
		}
	}, func(_ context.Context, record credential.Record) error {
		if record.Pending {
			return nil
		}
		saves.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := source.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := source.Access(ctx); first <- err }()
	await(t, started)
	second := make(chan string, 1)
	go func() {
		value, err := source.Access(context.Background())
		if err != nil {
			second <- "failure"
			return
		}
		second <- value
	}()
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	select {
	case value := <-second:
		if value != "new-access" {
			t.Fatal("shared refresh did not survive cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shared refresh stalled")
	}
	// An older request rejected the already-replaced token: it must reuse the new one.
	if _, err := source.Reject(context.Background(), "old-access"); err != nil {
		t.Fatal(err)
	}
	if refreshes.Load() != 1 || saves.Load() != 1 {
		t.Fatal("duplicated rotation or persistence")
	}
}
func TestPersistenceFailureRetainsNewToken(t *testing.T) {
	var refreshes, saves int
	var persisted credential.Record
	source, err := credential.New(credential.Record{Token: credential.Token{Refresh: "old-refresh"}}, func(context.Context, string) (credential.Token, error) { refreshes++; return fresh(), nil }, func(_ context.Context, record credential.Record) error {
		if record.Pending {
			persisted = record
			return nil
		}
		token := record.Token

		saves++
		if token.Refresh != "new-refresh" {
			t.Error("old refresh token was saved")
		}
		if saves == 1 {
			return errors.New("durable store unavailable")
		}
		persisted = record
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Access(context.Background())
	if !errors.Is(err, credential.ErrPersistence) {
		t.Fatal(err)
	}
	_, err = source.Access(context.Background())
	if !errors.Is(err, credential.ErrPersistence) || refreshes != 1 || saves != 1 {
		t.Fatal("failed persistence triggered another effect")
	}
	if err = source.RetrySave(context.Background()); err != nil {
		t.Fatal(err)
	}
	value, err := source.Access(context.Background())
	if err != nil || value != "new-access" || refreshes != 1 || saves != 2 {
		t.Fatal("save-only recovery failed")
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := credential.New(persisted, func(context.Context, string) (credential.Token, error) {
		t.Error("confirmed recovery refreshed")
		return credential.Token{}, nil
	}, func(context.Context, credential.Record) error { t.Error("confirmed recovery wrote"); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if value, err = restored.Access(context.Background()); err != nil || value != "new-access" {
		t.Fatal("confirmed checkpoint did not restore", err)
	}
	if err = restored.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestUncertainRefreshIsNeverAutomaticallyRetried(t *testing.T) {
	calls := 0
	var persisted credential.Record
	source, err := credential.New(credential.Record{Token: credential.Token{Refresh: "single-use"}}, func(context.Context, string) (credential.Token, error) {
		if !persisted.Pending {
			t.Error("refresh preceded pending checkpoint")
		}
		calls++
		return credential.Token{}, errors.New("response lost after submission")
	}, func(_ context.Context, record credential.Record) error {
		if record.Pending {
			persisted = record
			return nil
		}
		t.Error("unexpected persistence")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		_, err = source.Access(context.Background())
		if !errors.Is(err, credential.ErrRefreshUncertain) {
			t.Fatal(err)
		}
	}
	if err = source.RetrySave(context.Background()); !errors.Is(err, credential.ErrRefreshUncertain) {
		t.Fatal(err)
	}
	if _, restoreErr := credential.New(persisted, nil, nil); !errors.Is(restoreErr, credential.ErrRefreshUncertain) {
		t.Fatal("pending checkpoint accepted", restoreErr)
	}
	if calls != 1 {
		t.Fatal("uncertain refresh was retried")
	}
	if err = source.Close(); !errors.Is(err, credential.ErrRefreshUncertain) {
		t.Fatal(err)
	}
}
func TestCloseWaitsForRefreshAcknowledgement(t *testing.T) {
	for _, phase := range []string{"checkpoint", "refresh"} {
		t.Run(phase, func(t *testing.T) {
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			block := func(ctx context.Context) { close(started); <-ctx.Done(); close(canceled); <-release }
			refreshes := 0
			source, err := credential.New(credential.Record{Token: credential.Token{Refresh: "old"}}, func(ctx context.Context, _ string) (credential.Token, error) {
				refreshes++
				if phase == "refresh" {
					block(ctx)
				}
				return credential.Token{}, ctx.Err()
			}, func(ctx context.Context, record credential.Record) error {
				if phase == "checkpoint" && record.Pending {
					block(ctx)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			accessDone := make(chan struct{})
			go func() { defer close(accessDone); _, _ = source.Access(context.Background()) }()
			await(t, started)
			closed := make(chan error, 1)
			go func() { closed <- source.Close() }()
			await(t, canceled)
			select {
			case <-closed:
				t.Fatal("close acknowledged unfinished writer")
			default:
			}
			close(release)
			select {
			case err := <-closed:
				if phase == "refresh" && !errors.Is(err, credential.ErrRefreshUncertain) {
					t.Fatal(err)
				}
				if phase == "checkpoint" && (err != nil || refreshes != 0) {
					t.Fatal("shutdown invoked refresh after checkpoint", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("close stalled after acknowledgement")
			}
			await(t, accessDone)
		})
	}
}

func TestExpiredRefreshResultIsSavedBeforeFailingAccess(t *testing.T) {
	var saved credential.Token
	source, err := credential.New(credential.Record{Token: credential.Token{Refresh: "old"}}, func(context.Context, string) (credential.Token, error) {
		token := fresh()
		token.ExpiresAt = time.Now().Add(-time.Minute)
		return token, nil
	}, func(_ context.Context, record credential.Record) error {
		if record.Pending {
			return nil
		}
		token := record.Token
		saved = token
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Access(context.Background())
	if !errors.Is(err, credential.ErrUnusableAccess) || saved.Refresh != "new-refresh" {
		t.Fatal("known rotated token was discarded")
	}
	if err = source.Close(); err != nil {
		t.Fatal("completed durable rotation was confused with unfinished cleanup", err)
	}
}
func TestKnownRotationSurvivesAssociatedFailure(t *testing.T) {
	issue := errors.New("response cleanup failed")
	var saved credential.Token
	source, err := credential.New(credential.Record{Token: credential.Token{Refresh: "old"}}, func(context.Context, string) (credential.Token, error) {
		token := fresh()
		token.ExpiresAt = time.Now().Add(-time.Minute)
		return token, issue
	}, func(_ context.Context, record credential.Record) error {
		if record.Pending {
			return nil
		}
		token := record.Token
		saved = token
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Access(context.Background())
	if !errors.Is(err, issue) || saved.Refresh != "new-refresh" {
		t.Fatal("known rotation was discarded on associated failure")
	}
	if err = source.Close(); !errors.Is(err, issue) {
		t.Fatal("unfinished cleanup was discarded", err)
	}
}
func TestSaveRecoveryDoesNotClearAssociatedRefreshFailure(t *testing.T) {
	issue := errors.New("response cleanup unfinished")
	saves := 0
	source, err := credential.New(credential.Record{Token: credential.Token{Refresh: "old"}}, func(context.Context, string) (credential.Token, error) { return fresh(), issue }, func(_ context.Context, record credential.Record) error {
		if record.Pending {
			return nil
		}

		saves++
		if saves == 1 {
			return errors.New("save unavailable")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Access(context.Background())
	if !errors.Is(err, credential.ErrPersistence) || !errors.Is(err, issue) {
		t.Fatal(err)
	}
	err = source.RetrySave(context.Background())
	if !errors.Is(err, issue) || errors.Is(err, credential.ErrPersistence) || saves != 2 {
		t.Fatal("save recovery erased unfinished refresh effect", err)
	}
	if err = source.Close(); !errors.Is(err, issue) {
		t.Fatal(err)
	}
}
func TestPendingCheckpointFailurePreventsRefresh(t *testing.T) {
	calls, writes := 0, 0
	source, err := credential.New(credential.Record{Token: credential.Token{Refresh: "old"}}, func(context.Context, string) (credential.Token, error) {
		calls++
		return fresh(), nil
	}, func(_ context.Context, record credential.Record) error {
		writes++
		if !record.Pending {
			t.Error("unexpected confirmation")
		}
		return errors.New("pending write unacknowledged")
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Access(context.Background())
	if !errors.Is(err, credential.ErrPersistence) || errors.Is(err, credential.ErrRefreshUncertain) || calls != 0 || writes != 1 {
		t.Fatal("external effect preceded durable intent", err)
	}
	if err = source.RetrySave(context.Background()); !errors.Is(err, credential.ErrPersistence) || calls != 0 || writes != 1 {
		t.Fatal("unknown write retried through save-only recovery", err)
	}
	if err = source.Close(); err != nil {
		t.Fatal("acknowledged worker was confused with pending external effect", err)
	}
}

// This subprocess fixture uses synthetic credentials and disposable files.
// It models process death at commit boundaries, not disk/power failure.
func TestCheckpointProcessHelper(t *testing.T) {
	args := flag.Args()
	if len(args) != 3 || args[0] != "credential-checkpoint" {
		return
	}
	phase, dir := args[1], args[2]
	recordPath := filepath.Join(dir, "record.json")
	input, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	var initial credential.Record
	if err = json.Unmarshal(input, &initial); err != nil {
		t.Fatal(err)
	}
	pause := func() { fmt.Println(phase); _, _ = io.Copy(io.Discard, os.Stdin) }
	source, err := credential.New(initial, func(context.Context, string) (credential.Token, error) {
		if err := os.WriteFile(filepath.Join(dir, "provider-consumed"), []byte("old consumed"), 0600); err != nil {
			return credential.Token{}, err
		}
		if phase == "rotated" {
			pause()
		}
		return fresh(), nil
	}, func(_ context.Context, record credential.Record) error {
		output, err := os.OpenFile(recordPath, os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			return err
		}
		writeErr := json.NewEncoder(output).Encode(record)
		syncErr := output.Sync()
		return errors.Join(writeErr, syncErr, output.Close())
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = source.Access(context.Background()); err != nil {
		t.Fatal(err)
	}
	if phase == "confirmed" {
		pause()
	}
}

func TestCheckpointRecoveryAfterProcessDeath(t *testing.T) {
	for _, phase := range []string{"rotated", "confirmed"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			initial, _ := json.Marshal(credential.Record{Token: credential.Token{Refresh: "old"}})
			recordPath := filepath.Join(dir, "record.json")
			if err := os.WriteFile(recordPath, initial, 0600); err != nil {
				t.Fatal(err)
			}
			// The watchdog only bounds a broken fixture.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCheckpointProcessHelper$", "--", "credential-checkpoint", phase, dir)
			input, err := child.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			output, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = child.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = child.Process.Kill() }()
			line := bufio.NewScanner(output)
			reached := line.Scan() && line.Text() == phase
			killErr := child.Process.Kill()
			waitErr := child.Wait()
			if !reached || killErr != nil || waitErr == nil {
				t.Fatalf("fixture did not crash at %s: %v %v", phase, killErr, waitErr)
			}
			if _, err = os.Stat(filepath.Join(dir, "provider-consumed")); err != nil {
				t.Fatal("external effect was not reached", err)
			}
			data, err := os.ReadFile(recordPath)
			if err != nil {
				t.Fatal(err)
			}
			var restored credential.Record
			if err = json.Unmarshal(data, &restored); err != nil {
				t.Fatal(err)
			}
			calls := 0
			source, err := credential.New(restored, func(context.Context, string) (credential.Token, error) { calls++; return fresh(), nil }, func(context.Context, credential.Record) error { calls++; return nil })
			if phase == "rotated" {
				if !restored.Pending || !errors.Is(err, credential.ErrRefreshUncertain) || calls != 0 {
					t.Fatal("uncertain checkpoint resumed", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				value, accessErr := source.Access(context.Background())
				if accessErr != nil || value != "new-access" || restored.Pending || calls != 0 {
					t.Fatal("confirmed checkpoint did not resume without refresh", accessErr)
				}
				if err = source.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
