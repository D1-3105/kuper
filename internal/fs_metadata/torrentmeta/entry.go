package torrentmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/klog/v2"
)

// TorrentMeta - metadata for libtorrent
type TorrentMeta struct {
	entry   Entry
	Content string `json:"content"`
}

// Entry - materialized file meta
type Entry struct {
	dmeta   *DirMeta
	Name    string       `json:"name,omitempty"`
	Size    int64        `json:"size,omitempty"`
	Hash    *string      `json:"hash,omitempty"`
	Version *string      `json:"version,omitempty"`
	Type    DirEntryType `json:"type,omitempty"`
}

// GetTorrentMeta - if it's a file - return associated torrent metadata for libtorrent (.torrent content)
func (e *Entry) GetTorrentMeta(ctx context.Context, etcdClient *clientv3.Client) (*TorrentMeta, error) {
	if e.Type == Keep {
		return nil, errors.New("keep file cannot have torrent meta")
	}
	if e.Type == Directory {
		return nil, errors.New("directory cannot have torrent meta")
	}
	if etcdClient == nil {
		return nil, errors.New("etcd client is nil")
	}
	if e.dmeta == nil {
		return nil, errors.New("dir meta is nil")
	}
	torrentMetas, err := etcdClient.Get(ctx, fmt.Sprintf(TorrentMetaEndpoint, e.dmeta.VolumeName, e.dmeta.Name))
	if err != nil {
		return nil, err
	}
	torrentMeta := &TorrentMeta{}
	if torrentMetas.Count == 0 {
		return nil, errors.New("no torrent meta found")
	}
	err = json.Unmarshal(torrentMetas.Kvs[torrentMetas.Count].Value, torrentMeta)
	if err != nil {
		return nil, err
	}
	torrentMeta.entry = *e
	return torrentMeta, nil
}

// EtcdFullPath - full etcd key
func (e *Entry) EtcdFullPath() (string, error) {
	if e.dmeta == nil {
		return "", errors.New("etcd meta is nil")
	}
	return fmt.Sprintf(EntriesEndpoint, e.dmeta.VolumeName, e.dmeta.Name+"/"+e.Name), nil
}

// LockPath - lock for this specific object
func (e *Entry) LockPath() (string, error) {
	return fmt.Sprintf(EntriesLockEndpoint, e.dmeta.VolumeName, e.dmeta.Name+"/"+e.Name), nil
}

// AtomicOp callMe with locked entry
func (e *Entry) AtomicOp(
	ctx context.Context,
	callMe func(context.Context, *concurrency.Session) error,
	etcdClient *clientv3.Client,
	ttl uint16,
) error {
	lockPath, err := e.LockPath()
	if err != nil {
		return err
	}

	sessionCtx, cancelSession := context.WithTimeout(ctx, time.Duration(ttl)*time.Second)
	defer cancelSession()

	session, err := concurrency.NewSession(etcdClient,
		concurrency.WithTTL(int(ttl)),
		concurrency.WithContext(sessionCtx),
	)
	if err != nil {
		return fmt.Errorf("failed to create etcd session: %w", err)
	}
	defer func() {
		if err := session.Close(); err != nil {
			klog.Errorf("session for >>%s<< close error: %v", lockPath, err)
		}
	}()

	mu := concurrency.NewMutex(session, lockPath)

	lockCtx, cancelLock := context.WithTimeout(ctx, 5*time.Second)
	defer cancelLock()

	if err := mu.Lock(lockCtx); err != nil {
		return fmt.Errorf("failed to acquire lock for %s: %w", lockPath, err)
	}
	defer func() {
		if err := mu.Unlock(context.Background()); err != nil {
			klog.Errorf("failed to unlock mutex for %s: %v", lockPath, err)
		}
	}()

	return callMe(ctx, session)
}
