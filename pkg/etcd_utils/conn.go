package etcd_utils

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"kuper/conf"
	"time"
)

func NewETCDClient(cfg *conf.ETCDConfig) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		Username:    cfg.EtcdUsername,
		Password:    cfg.EtcdPassword,
		DialTimeout: time.Duration(cfg.EtcdDialTimeout) * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}
