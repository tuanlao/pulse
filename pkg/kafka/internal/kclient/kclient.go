// Package kclient holds the connection-level configuration (brokers, client id,
// SASL, TLS) shared by the producer and consumer, plus the helpers that turn it
// into franz-go client options. It is internal: only pkg/kafka and its
// sub-packages compose it (the facade exposes it as ClientConfig).
package kclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config is the Kafka connection configuration.
type Config struct {
	// Brokers are the seed broker addresses (host:port). Default ["localhost:9092"].
	Brokers []string `mapstructure:"brokers"`
	// ClientID is sent to the broker for identification. Default "pulse".
	ClientID string `mapstructure:"client_id"`
	// SASL configures authentication. Opt-in.
	SASL SASLConfig `mapstructure:"sasl"`
	// TLS configures transport encryption. Opt-in.
	TLS TLSConfig `mapstructure:"tls"`
}

// SASLConfig configures SASL authentication.
type SASLConfig struct {
	// Enabled toggles SASL. Default false.
	Enabled bool `mapstructure:"enabled"`
	// Mechanism is "plain", "scram-sha-256", or "scram-sha-512". Default "plain".
	Mechanism string `mapstructure:"mechanism"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
}

// TLSConfig configures TLS for the broker connection.
type TLSConfig struct {
	// Enabled toggles TLS. Default false.
	Enabled bool `mapstructure:"enabled"`
	// InsecureSkipVerify disables certificate verification (dev only).
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`
	// CAFile is a PEM bundle to verify the server certificate against.
	CAFile string `mapstructure:"ca_file"`
	// CertFile / KeyFile are the client certificate for mutual TLS.
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
	// ServerName overrides the SNI / verification hostname.
	ServerName string `mapstructure:"server_name"`
}

// DefaultConfig returns the connection defaults.
func DefaultConfig() Config {
	return Config{
		Brokers:  []string{"localhost:9092"},
		ClientID: "pulse",
		SASL:     SASLConfig{Enabled: false, Mechanism: "plain"},
		TLS:      TLSConfig{Enabled: false},
	}
}

// ApplyDefaults fills empty fields from DefaultConfig.
func (c *Config) ApplyDefaults() {
	d := DefaultConfig()
	if len(c.Brokers) == 0 {
		c.Brokers = d.Brokers
	}
	if c.ClientID == "" {
		c.ClientID = d.ClientID
	}
	if c.SASL.Mechanism == "" {
		c.SASL.Mechanism = d.SASL.Mechanism
	}
}

// Opts builds the base franz-go client options (brokers, client id, SASL, TLS)
// shared by producer and consumer. Callers append role-specific options.
func Opts(c Config) ([]kgo.Opt, error) {
	c.ApplyDefaults()
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
	}

	if c.SASL.Enabled {
		m, err := saslMechanism(c.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(m))
	}

	if c.TLS.Enabled {
		tc, err := tlsConfig(c.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	return opts, nil
}

// Build constructs a *kgo.Client from the connection config plus any extra
// role-specific options (producer tuning, consumer-group options, ...).
func Build(c Config, extra ...kgo.Opt) (*kgo.Client, error) {
	base, err := Opts(c)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(append(base, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return cl, nil
}

func saslMechanism(c SASLConfig) (sasl.Mechanism, error) {
	switch strings.ToLower(strings.TrimSpace(c.Mechanism)) {
	case "", "plain":
		return plain.Auth{User: c.Username, Pass: c.Password}.AsMechanism(), nil
	case "scram-sha-256":
		return scram.Auth{User: c.Username, Pass: c.Password}.AsSha256Mechanism(), nil
	case "scram-sha-512":
		return scram.Auth{User: c.Username, Pass: c.Password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafka: unknown sasl mechanism %q", c.Mechanism)
	}
}

func tlsConfig(c TLSConfig) (*tls.Config, error) {
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.InsecureSkipVerify,
		ServerName:         c.ServerName,
	}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: read tls ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka: tls ca_file %q has no valid certificates", c.CAFile)
		}
		tc.RootCAs = pool
	}
	if c.CertFile != "" || c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: load tls client cert: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}
