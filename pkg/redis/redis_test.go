package redis

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// resolve mimics New's defaults/opts/defaults resolution and returns the mapped
// rueidis option, so the mapping can be asserted without dialing a server.
func resolve(t *testing.T, mutate func(*Config)) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.applyDefaults()
	if mutate != nil {
		mutate(&cfg)
	}
	cfg.applyDefaults()
	return cfg
}

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	if !c.Enabled {
		t.Fatalf("default should be enabled")
	}
	if len(c.Addresses) != 1 || c.Addresses[0] != "localhost:6379" {
		t.Fatalf("default address wrong: %v", c.Addresses)
	}
	if !c.Cache.Enabled {
		t.Fatalf("client-side caching should be on by default")
	}
	if c.Cache.Broadcast.Enabled {
		t.Fatalf("broadcast should be off by default (prefixes are app-specific)")
	}
	if !c.PingOnStart || !c.Tracing.Enabled || !c.Metrics.Enabled {
		t.Fatalf("ping/tracing/metrics should default on")
	}
}

func TestApplyDefaults(t *testing.T) {
	var c Config // zero value
	c.applyDefaults()
	if len(c.Addresses) == 0 {
		t.Fatalf("addresses not defaulted")
	}
	if c.DialTimeout != 5*time.Second || c.ConnWriteTimeout != 10*time.Second {
		t.Fatalf("timeouts not defaulted: %v %v", c.DialTimeout, c.ConnWriteTimeout)
	}
	if c.Metrics.Namespace != "pulse" || c.Metrics.Subsystem != "redis" {
		t.Fatalf("metrics ns/subsystem not defaulted")
	}
	if len(c.Metrics.Buckets) == 0 {
		t.Fatalf("metrics buckets not defaulted")
	}
}

func TestApplyDefaults_SentinelMasterSet(t *testing.T) {
	c := DefaultConfig()
	c.Sentinel.Enabled = true // no master set given
	c.applyDefaults()
	if c.Sentinel.MasterSet != "mymaster" {
		t.Fatalf("sentinel master set not defaulted: %q", c.Sentinel.MasterSet)
	}
}

func TestOptions(t *testing.T) {
	cfg := resolve(t, func(c *Config) {
		WithAddresses("a:1", "b:2")(c)
		WithCredentials("u", "p")(c)
		WithDB(3)(c)
		WithClientName("svc")(c)
		WithSendToReplicas(true)(c)
		WithBroadcast("user:", "product:")(c)
		WithSentinel("themaster")(c)
	})

	if !reflect.DeepEqual(cfg.Addresses, []string{"a:1", "b:2"}) {
		t.Fatalf("WithAddresses: %v", cfg.Addresses)
	}
	if cfg.Username != "u" || cfg.Password != "p" || cfg.DB != 3 || cfg.ClientName != "svc" {
		t.Fatalf("credentials/db/name options not applied: %+v", cfg)
	}
	if !cfg.SendToReplicas {
		t.Fatalf("WithSendToReplicas not applied")
	}
	if !cfg.Cache.Enabled || !cfg.Cache.Broadcast.Enabled {
		t.Fatalf("WithBroadcast should enable cache + broadcast")
	}
	if !reflect.DeepEqual(cfg.Cache.Broadcast.Prefixes, []string{"user:", "product:"}) {
		t.Fatalf("WithBroadcast prefixes: %v", cfg.Cache.Broadcast.Prefixes)
	}
	if !cfg.Sentinel.Enabled || cfg.Sentinel.MasterSet != "themaster" {
		t.Fatalf("WithSentinel not applied: %+v", cfg.Sentinel)
	}
}

func TestClientOption_Defaults(t *testing.T) {
	cfg := resolve(t, nil)
	opt, err := cfg.clientOption()
	if err != nil {
		t.Fatalf("clientOption: %v", err)
	}
	if !reflect.DeepEqual(opt.InitAddress, []string{"localhost:6379"}) {
		t.Fatalf("InitAddress: %v", opt.InitAddress)
	}
	if opt.DisableCache {
		t.Fatalf("cache enabled by default -> DisableCache should be false")
	}
	if opt.ClientTrackingOptions != nil {
		t.Fatalf("default (non-broadcast) should leave ClientTrackingOptions nil, got %v", opt.ClientTrackingOptions)
	}
	if opt.SendToReplicas != nil {
		t.Fatalf("SendToReplicas should be nil by default")
	}
	if opt.Dialer.Timeout != 5*time.Second || opt.ConnWriteTimeout != 10*time.Second {
		t.Fatalf("timeouts not mapped: %v %v", opt.Dialer.Timeout, opt.ConnWriteTimeout)
	}
	if opt.TLSConfig != nil {
		t.Fatalf("TLS should be off by default")
	}
}

func TestClientOption_Broadcast(t *testing.T) {
	cfg := resolve(t, func(c *Config) { WithBroadcast("user:", "product:")(c) })
	opt, err := cfg.clientOption()
	if err != nil {
		t.Fatalf("clientOption: %v", err)
	}
	want := []string{"PREFIX", "user:", "PREFIX", "product:", "BCAST"}
	if !reflect.DeepEqual(opt.ClientTrackingOptions, want) {
		t.Fatalf("ClientTrackingOptions = %v, want %v", opt.ClientTrackingOptions, want)
	}
	if opt.DisableCache {
		t.Fatalf("broadcast implies caching enabled")
	}
}

func TestClientOption_BroadcastRequiresPrefixes(t *testing.T) {
	cfg := resolve(t, func(c *Config) {
		c.Cache.Enabled = true
		c.Cache.Broadcast.Enabled = true
		c.Cache.Broadcast.Prefixes = nil
	})
	if _, err := cfg.clientOption(); err == nil {
		t.Fatalf("expected error when broadcast enabled without prefixes")
	}
}

func TestClientOption_CacheDisabled(t *testing.T) {
	cfg := resolve(t, func(c *Config) { WithClientCache(false)(c) })
	opt, err := cfg.clientOption()
	if err != nil {
		t.Fatalf("clientOption: %v", err)
	}
	if !opt.DisableCache {
		t.Fatalf("cache disabled -> DisableCache should be true")
	}
}

func TestClientOption_SendToReplicasAndTuning(t *testing.T) {
	cfg := resolve(t, func(c *Config) {
		c.SendToReplicas = true
		c.BlockingPoolSize = 10
		c.PipelineMultiplex = -1
		c.RingScaleEachConn = 12
		c.MaxFlushDelay = 25 * time.Microsecond
		c.Cache.SizePerConn = 1 << 20
	})
	opt, err := cfg.clientOption()
	if err != nil {
		t.Fatalf("clientOption: %v", err)
	}
	if opt.SendToReplicas == nil {
		t.Fatalf("SendToReplicas should be set")
	}
	if opt.BlockingPoolSize != 10 || opt.PipelineMultiplex != -1 || opt.RingScaleEachConn != 12 {
		t.Fatalf("tuning knobs not mapped: %+v", opt)
	}
	if opt.MaxFlushDelay != 25*time.Microsecond {
		t.Fatalf("MaxFlushDelay not mapped: %v", opt.MaxFlushDelay)
	}
	if opt.CacheSizeEachConn != 1<<20 {
		t.Fatalf("CacheSizeEachConn not mapped: %d", opt.CacheSizeEachConn)
	}
}

func TestClientOption_TLSAndSentinel(t *testing.T) {
	cfg := resolve(t, func(c *Config) {
		c.TLS = TLSConfig{Enabled: true, InsecureSkipVerify: true, ServerName: "redis.example"}
		WithSentinel("themaster")(c)
		c.Sentinel.Password = "spw"
	})
	opt, err := cfg.clientOption()
	if err != nil {
		t.Fatalf("clientOption: %v", err)
	}
	if opt.TLSConfig == nil || opt.TLSConfig.ServerName != "redis.example" || !opt.TLSConfig.InsecureSkipVerify {
		t.Fatalf("TLS not mapped: %+v", opt.TLSConfig)
	}
	if opt.Sentinel.MasterSet != "themaster" || opt.Sentinel.Password != "spw" {
		t.Fatalf("Sentinel not mapped: %+v", opt.Sentinel)
	}
}

func TestCmdVerb(t *testing.T) {
	cases := map[string][]string{
		"GET":     {"get", "k"},
		"SET":     {"SET", "k", "v"},
		"unknown": {},
	}
	for want, in := range cases {
		if got := cmdVerb(in); got != want {
			t.Errorf("cmdVerb(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestNew_Disabled(t *testing.T) {
	c, err := New(Config{Enabled: false}, Deps{})
	if err != nil {
		t.Fatalf("New disabled: %v", err)
	}
	if !c.disabled || c.Client != nil {
		t.Fatalf("disabled client should not dial")
	}
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("disabled Start should be no-op: %v", err)
	}
	if err := c.Stop(ctx); err != nil {
		t.Fatalf("disabled Stop should be no-op: %v", err)
	}
	if err := c.CheckReady(ctx); err != nil {
		t.Fatalf("disabled CheckReady should be no-op: %v", err)
	}
	if c.Name() != "redis" {
		t.Fatalf("Name should be redis")
	}
}
