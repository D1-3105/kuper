package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"kuper/internal/fs_metadata/torrentmeta"
	"testing"
)

func TestGetEntries(t *testing.T) {
	mockKV := new(MockKV)
	cli := &clientv3.Client{KV: mockKV}

	// example DirMeta
	dmeta := &torrentmeta.DirMeta{VolumeName: "vol1", Name: "dir1"}

	// fake Entry
	entry := torrentmeta.Entry{
		Name: "file1", Size: 123, Type: torrentmeta.File,
	}
	value, err := json.Marshal(entry)
	assert.NoError(t, err)

	expectedKey := fmt.Sprintf(torrentmeta.EntriesEndpoint, dmeta.VolumeName, dmeta.Name+"/file1")

	resp := &clientv3.GetResponse{
		Kvs: []*mvccpb.KeyValue{
			{Key: []byte(expectedKey), Value: value},
		},
		Count: 1,
	}

	// Setup expectation for Get call with correct key prefix
	mockKV.On(
		"Get", mock.Anything, mock.MatchedBy(
			func(key string) bool {
				return len(key) >= len(fmt.Sprintf(torrentmeta.EntriesEndpoint, dmeta.VolumeName, dmeta.Name)) &&
					key[:len(
						fmt.Sprintf(
							torrentmeta.EntriesEndpoint, dmeta.VolumeName, dmeta.Name,
						),
					)] == fmt.Sprintf(torrentmeta.EntriesEndpoint, dmeta.VolumeName, dmeta.Name)
			},
		),
	).Return(resp, nil)

	entries, err := dmeta.GetEntries(context.Background(), cli, nil)

	assert.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, "file1", entries[0].Name)
	mockKV.AssertExpectations(t)
}

func etcdClientFixture(t *testing.T, kv *MockKV) (*clientv3.Client, error) {
	cfg := clientv3.Config{
		Endpoints: []string{"localhost:2379"},
		Logger:    zap.NewNop(),
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	cli.KV = kv
	return cli, nil
}

func etcdSessionFixture(t *testing.T) {
	oldSesF := torrentmeta.ETCDSessionFactory
	oldMuF := torrentmeta.ETCDMutexFactory
	torrentmeta.ETCDSessionFactory = MockNewSession
	torrentmeta.ETCDMutexFactory = MockNewMutex
	t.Cleanup(
		func() {
			torrentmeta.ETCDSessionFactory = oldSesF
			torrentmeta.ETCDMutexFactory = oldMuF
		},
	)
}

func TestDeleteEntry_File(t *testing.T) {
	etcdSessionFixture(t)
	mockKV := new(MockKV)
	cli, err := etcdClientFixture(t, mockKV)
	require.NoError(t, err)

	dmeta := &torrentmeta.DirMeta{VolumeName: "vol", Name: "dir"}
	entry := &torrentmeta.Entry{
		Name: "file1", Type: torrentmeta.File,
	}
	entry.SetDirMeta(dmeta)

	metaKey := fmt.Sprintf(torrentmeta.TorrentMetaEndpoint, dmeta.VolumeName, dmeta.Name)
	entryKey, _ := entry.EtcdFullPath()

	mockKV.On("Delete", mock.Anything, metaKey).Return(&clientv3.DeleteResponse{}, nil)
	mockKV.On("Delete", mock.Anything, entryKey).Return(&clientv3.DeleteResponse{}, nil)

	err = entry.Delete(context.Background(), cli)
	assert.NoError(t, err)
	mockKV.AssertExpectations(t)
}

func TestDeleteEntry_Keep(t *testing.T) {
	etcdSessionFixture(t)
	mockKV := new(MockKV)
	cli, err := etcdClientFixture(t, mockKV)
	require.NoError(t, err)

	dmeta := &torrentmeta.DirMeta{VolumeName: "vol", Name: "dir"}
	entry := &torrentmeta.Entry{
		Name: "keep", Type: torrentmeta.Keep,
	}
	entry.SetDirMeta(dmeta)

	entryKey, _ := entry.EtcdFullPath()
	mockKV.On("Delete", mock.Anything, entryKey).Return(&clientv3.DeleteResponse{}, nil)

	err = entry.Delete(context.Background(), cli)
	assert.NoError(t, err)
	mockKV.AssertExpectations(t)
}

func mustMarshalEntry(e *torrentmeta.Entry) []byte {
	data, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return data
}

func TestDeleteDirMeta_Delete(t *testing.T) {
	etcdSessionFixture(t)
	mockKV := new(MockKV)
	cli, err := etcdClientFixture(t, mockKV)
	require.NoError(t, err)

	// Prepare DirMeta and its entries
	dmeta := &torrentmeta.DirMeta{
		VolumeName: "vol",
		Name:       "dir",
	}

	entry1 := &torrentmeta.Entry{Name: "file1", Type: torrentmeta.File}
	entry2 := &torrentmeta.Entry{Name: "file2", Type: torrentmeta.File}
	entry1.SetDirMeta(dmeta)
	entry2.SetDirMeta(dmeta)

	// Prepare Etcd keys
	metaKey := fmt.Sprintf(torrentmeta.TorrentMetaEndpoint, dmeta.VolumeName, dmeta.Name)
	entryKey1, _ := entry1.EtcdFullPath()
	entryKey2, _ := entry2.EtcdFullPath()

	// Mock Get response
	mockKV.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(
		&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(entryKey1),
					Value: mustMarshalEntry(entry1),
				},
				{
					Key:   []byte(entryKey2),
					Value: mustMarshalEntry(entry2),
				},
			},
		}, nil,
	)

	// Mock Delete for entries
	mockKV.On("Delete", mock.Anything, metaKey).Return(&clientv3.DeleteResponse{}, nil)
	mockKV.On("Delete", mock.Anything, entryKey1).Return(&clientv3.DeleteResponse{}, nil)
	mockKV.On("Delete", mock.Anything, entryKey2).Return(&clientv3.DeleteResponse{}, nil)

	// Act
	err = dmeta.Delete(context.Background(), cli)

	// Assert
	assert.NoError(t, err)
	mockKV.AssertExpectations(t)
}
