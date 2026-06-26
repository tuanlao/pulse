package redis

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/redis/rueidis"
)

// clientOption translates Config into a rueidis.ClientOption. It is pure (no
// dialing / side effects beyond reading TLS files) so the mapping — especially
// the client-side caching / broadcast tracking — is unit-testable without a live
// redis server.
func (c Config) clientOption() (rueidis.ClientOption, error) {
	opt := rueidis.ClientOption{
		InitAddress:      c.Addresses,
		Username:         c.Username,
		Password:         c.Password,
		SelectDB:         c.DB,
		ClientName:       c.ClientName,
		ConnWriteTimeout: c.ConnWriteTimeout,
	}
	opt.Dialer.Timeout = c.DialTimeout

	// Tuning: only override when set, otherwise rueidis defaults apply.
	if c.BlockingPoolSize > 0 {
		opt.BlockingPoolSize = c.BlockingPoolSize
	}
	if c.PipelineMultiplex != 0 {
		opt.PipelineMultiplex = c.PipelineMultiplex
	}
	if c.RingScaleEachConn > 0 {
		opt.RingScaleEachConn = c.RingScaleEachConn
	}
	if c.MaxFlushDelay > 0 {
		opt.MaxFlushDelay = c.MaxFlushDelay
	}

	if c.SendToReplicas {
		opt.SendToReplicas = func(cmd rueidis.Completed) bool { return cmd.IsReadOnly() }
	}

	// Client-side caching (the headline feature).
	if !c.Cache.Enabled {
		opt.DisableCache = true
	} else {
		if c.Cache.SizePerConn > 0 {
			opt.CacheSizeEachConn = c.Cache.SizePerConn
		}
		if c.Cache.Broadcast.Enabled {
			if len(c.Cache.Broadcast.Prefixes) == 0 {
				return rueidis.ClientOption{}, fmt.Errorf("redis: cache.broadcast.enabled requires at least one prefix")
			}
			// CLIENT TRACKING ... PREFIX <p> [PREFIX <p> ...] BCAST
			track := make([]string, 0, len(c.Cache.Broadcast.Prefixes)*2+1)
			for _, p := range c.Cache.Broadcast.Prefixes {
				track = append(track, "PREFIX", p)
			}
			track = append(track, "BCAST")
			opt.ClientTrackingOptions = track
		}
	}

	if c.TLS.Enabled {
		tc, err := buildTLSConfig(c.TLS)
		if err != nil {
			return rueidis.ClientOption{}, err
		}
		opt.TLSConfig = tc
	}

	if c.Sentinel.Enabled {
		opt.Sentinel = rueidis.SentinelOption{
			MasterSet: c.Sentinel.MasterSet,
			Username:  c.Sentinel.Username,
			Password:  c.Sentinel.Password,
		}
	}

	return opt, nil
}

// buildTLSConfig assembles a *tls.Config from TLSConfig, loading the optional CA
// bundle and client key pair from disk.
func buildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	tc := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // opt-in dev escape hatch
		ServerName:         cfg.ServerName,
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("redis: read tls ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("redis: tls ca_file %q contains no valid certificates", cfg.CAFile)
		}
		tc.RootCAs = pool
	}
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("redis: load tls client key pair: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}
