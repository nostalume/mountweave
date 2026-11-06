package s3

import (
	"context"
	"encoding/binary"
	"errors"

	bolt "go.etcd.io/bbolt"
)

var nativeLog = []byte("native-checkpoints")

func checkpointBytes(upload *bolt.Bucket) uint64 {
	if log := upload.Bucket(nativeLog); log != nil {
		return log.Sequence()
	}
	return 0
}

func loadCheckpoint(upload *bolt.Bucket) ([]byte, error) {
	size := checkpointBytes(upload)
	if size == 0 {
		return nil, nil
	}
	if size > uint64(^uint(0)>>1) {
		return nil, errors.New("multipart checkpoint is too large")
	}
	out := make([]byte, 0, int(size))
	err := upload.Bucket(nativeLog).ForEach(func(key, value []byte) error {
		if len(key) != 8 || binary.BigEndian.Uint64(key) != uint64(len(out)) ||
			len(value) == 0 || len(value) > cap(out)-len(out) {
			return errors.New("invalid multipart checkpoint offset")
		}
		out = append(out, value...)
		return nil
	})
	if err == nil && len(out) != cap(out) {
		err = errors.New("incomplete multipart checkpoint")
	}
	return out, err
}

func (m *multipart) appendCheckpoint(ctx context.Context, id string, delta []byte) error {
	if len(delta) == 0 {
		return errors.New("empty multipart checkpoint")
	}
	return m.DB.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		upload := tx.Bucket(manifestRoot).Bucket(uploadTable).Bucket([]byte(id))
		if upload == nil || upload.Bucket(nativeLog) == nil {
			return uploadConflict()
		}
		// The frozen request is not read or rewritten per append. The only
		// caller is the exclusive Core.Write attempt, which joins callbacks.
		log := upload.Bucket(nativeLog)
		offset := log.Sequence()
		limit := uint64(m.MaxCheckpointBytes)
		if offset > limit || uint64(len(delta)) > limit-offset {
			return multipartCapacity()
		}
		if err := log.Put(binary.BigEndian.AppendUint64(nil, offset), delta); err != nil {
			return err
		}
		return log.SetSequence(offset + uint64(len(delta)))
	})
}
