package conf

import (
	"fmt"
	"github.com/caarlos0/env/v7"
)

type ETCDConfig struct {
	EtcdEndpoints   []string `env:"ETCD_ENDPOINTS" envSeparator:","`
	EtcdUsername    string   `env:"ETCD_USERNAME"`
	EtcdPassword    string   `env:"ETCD_PASSWORD"`
	EtcdDialTimeout int      `env:"ETCD_DIAL_TIMEOUT" envDefault:"5"` // seconds
}

// NewETCDConfig read conf
func NewETCDConfig() (*ETCDConfig, error) {
	cfg := &ETCDConfig{}

	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse env vars: %w", err)
	}

	if len(cfg.EtcdEndpoints) == 0 {
		cfg.EtcdEndpoints = []string{"localhost:2379"}
	}

	return cfg, nil
}
