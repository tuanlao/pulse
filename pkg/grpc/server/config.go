// Package server provides a configurable gRPC server that plugs into the pulse
// lifecycle: it implements lifecycle.Component, builds a *grpc.Server with safe
// message-size and keepalive settings, chains the standard interceptors
// (recovery, request logger, RED metrics) plus otelgrpc tracing via a
// StatsHandler, registers the gRPC health service and (optionally) reflection,
// and gracefully drains in-flight RPCs on shutdown.
//
// Like every pulse package it exposes Config + DefaultConfig() + functional
// Options. A service registers its own service implementations with Register
// (before Start) and composes the server into its own lifecycle manager.
package server

import (
	"strconv"
	"time"

	"github.com/tuanlao/pulse/pkg/grpc/server/interceptor"
)

// TLSConfig configures server-side TLS. When Enabled is false the server listens
// without TLS (suitable for local dev or in-cluster traffic behind an mTLS mesh).
// When ClientCAFile is set the server requires and verifies a client certificate
// (mTLS).
type TLSConfig struct {
	Enabled      bool   `mapstructure:"enabled"`
	CertFile     string `mapstructure:"cert_file"`
	KeyFile      string `mapstructure:"key_file"`
	ClientCAFile string `mapstructure:"client_ca_file"`
}

// KeepaliveConfig maps onto google.golang.org/grpc/keepalive ServerParameters and
// EnforcementPolicy. The defaults match grpc-go's own; they are exposed so a
// service can cap connection age and tune client-ping enforcement.
type KeepaliveConfig struct {
	// Time is how long an idle connection waits before the server pings it.
	Time time.Duration `mapstructure:"time"`
	// Timeout is how long the server waits for a ping ack before closing. Default 20s.
	Timeout time.Duration `mapstructure:"timeout"`
	// MaxConnectionIdle closes a connection after this much idle time. Default 0 (infinite).
	MaxConnectionIdle time.Duration `mapstructure:"max_connection_idle"`
	// MaxConnectionAge caps a connection's total lifetime. Default 0 (infinite).
	MaxConnectionAge time.Duration `mapstructure:"max_connection_age"`
	// MaxConnectionAgeGrace is the grace period after MaxConnectionAge. Default 0.
	MaxConnectionAgeGrace time.Duration `mapstructure:"max_connection_age_grace"`
	// EnforcementMinTime is the minimum client ping interval the server tolerates. Default 5m.
	EnforcementMinTime time.Duration `mapstructure:"enforcement_min_time"`
	// EnforcementPermitWithoutStream allows client pings with no active streams. Default true.
	EnforcementPermitWithoutStream bool `mapstructure:"enforcement_permit_without_stream"`
}

// Config configures the gRPC server. Every field has a default.
type Config struct {
	// Port is the TCP port to listen on (binds all interfaces, ":<port>"). Default 9090.
	Port int `mapstructure:"port"`
	// Network is the listen network. Default "tcp".
	Network string `mapstructure:"network"`

	// MaxRecvMsgSize caps the size of a received message in bytes. Default 4 MiB.
	MaxRecvMsgSize int `mapstructure:"max_recv_msg_size"`
	// MaxSendMsgSize caps the size of a sent message in bytes. Default 4 MiB.
	MaxSendMsgSize int `mapstructure:"max_send_msg_size"`

	// ConnectionTimeout bounds connection setup (handshake). Default 120s.
	ConnectionTimeout time.Duration `mapstructure:"connection_timeout"`
	// ShutdownDrain bounds GracefulStop before a hard Stop. Should be <= the
	// lifecycle ShutdownTimeout. Default 25s.
	ShutdownDrain time.Duration `mapstructure:"shutdown_drain"`

	// EnableReflection registers the gRPC server reflection service. Default false
	// (opt-in; keep off in production unless intended, it exposes the schema).
	EnableReflection bool `mapstructure:"enable_reflection"`
	// EnableHealth registers the standard gRPC health service. Default true.
	EnableHealth bool `mapstructure:"enable_health"`

	Keepalive KeepaliveConfig           `mapstructure:"keepalive"`
	Metrics   interceptor.MetricsConfig `mapstructure:"metrics"`
	TLS       TLSConfig                 `mapstructure:"tls"`
}

// DefaultConfig returns Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Port:              9090,
		Network:           "tcp",
		MaxRecvMsgSize:    4 << 20,
		MaxSendMsgSize:    4 << 20,
		ConnectionTimeout: 120 * time.Second,
		ShutdownDrain:     25 * time.Second,
		EnableReflection:  false,
		EnableHealth:      true,
		Keepalive: KeepaliveConfig{
			Time:                           2 * time.Hour,
			Timeout:                        20 * time.Second,
			EnforcementMinTime:             5 * time.Minute,
			EnforcementPermitWithoutStream: true,
		},
		Metrics: interceptor.DefaultMetricsConfig(),
		TLS:     TLSConfig{},
	}
}

// applyDefaults backfills unset numeric/string/duration fields from DefaultConfig.
// Bools that default true (EnableHealth, EnforcementPermitWithoutStream) are NOT
// re-applied here: a literal false from a partial config is only honored when the
// struct originates from DefaultConfig() (which the service's defaultAppConfig
// does). This matches the HTTP server's TLS.Enabled caveat.
func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.Port == 0 {
		c.Port = d.Port
	}
	if c.Network == "" {
		c.Network = d.Network
	}
	if c.MaxRecvMsgSize == 0 {
		c.MaxRecvMsgSize = d.MaxRecvMsgSize
	}
	if c.MaxSendMsgSize == 0 {
		c.MaxSendMsgSize = d.MaxSendMsgSize
	}
	if c.ConnectionTimeout <= 0 {
		c.ConnectionTimeout = d.ConnectionTimeout
	}
	if c.ShutdownDrain <= 0 {
		c.ShutdownDrain = d.ShutdownDrain
	}
	c.Keepalive.applyDefaults(d.Keepalive)
	c.Metrics.ApplyDefaults(interceptor.DefaultMetricsConfig())
}

func (k *KeepaliveConfig) applyDefaults(d KeepaliveConfig) {
	if k.Time <= 0 {
		k.Time = d.Time
	}
	if k.Timeout <= 0 {
		k.Timeout = d.Timeout
	}
	if k.EnforcementMinTime <= 0 {
		k.EnforcementMinTime = d.EnforcementMinTime
	}
	// MaxConnectionIdle/Age/AgeGrace default to 0 (infinite) and are not backfilled.
}

// listenAddr returns the ":<port>" address the server binds to.
func (c *Config) listenAddr() string { return ":" + strconv.Itoa(c.Port) }

// Option overrides Config fields.
type Option func(*Config)

// WithPort sets the listen port.
func WithPort(port int) Option { return func(c *Config) { c.Port = port } }

// WithReflection toggles the gRPC reflection service.
func WithReflection(v bool) Option { return func(c *Config) { c.EnableReflection = v } }

// WithHealth toggles the gRPC health service.
func WithHealth(v bool) Option { return func(c *Config) { c.EnableHealth = v } }

// WithMaxRecvMsgSize sets the max received message size in bytes.
func WithMaxRecvMsgSize(n int) Option { return func(c *Config) { c.MaxRecvMsgSize = n } }

// WithMaxSendMsgSize sets the max sent message size in bytes.
func WithMaxSendMsgSize(n int) Option { return func(c *Config) { c.MaxSendMsgSize = n } }

// WithTLS overrides the TLS config.
func WithTLS(t TLSConfig) Option { return func(c *Config) { c.TLS = t } }

// WithKeepalive overrides the keepalive config.
func WithKeepalive(k KeepaliveConfig) Option { return func(c *Config) { c.Keepalive = k } }
