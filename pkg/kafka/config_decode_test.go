package kafka

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tuanlao/pulse/pkg/config"
)

// TestConsumersDecodeFromYAML verifies the `consumers:` map decodes through the
// real viper loader: the squashed consumer.Config fields (group_id/topics/mode)
// land alongside the nested retry/dedup, and durations parse. This is the path the
// example service relies on, so a squash regression would surface here.
func TestConsumersDecodeFromYAML(t *testing.T) {
	dir := t.TempDir()
	yaml := `
kafka:
  consumers:
    orders:
      group_id: example-orders
      topics: ["orders"]
      mode: unordered
      concurrency: 128
      retry:
        enabled: true
        backoffs: ["5s", "10s"]
        dlq:
          enabled: true
      dedup:
        enabled: true
        ttl: 2h
    audit:
      group_id: example-audit
      topics: ["orders", "users"]
      mode: ordered
      retry:
        enabled: false
`
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	type appConfig struct {
		Kafka Config `mapstructure:"kafka"`
	}
	app := appConfig{Kafka: DefaultConfig()}
	if err := config.Load(&app, config.Options{SearchPaths: []string{dir}}); err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	app.Kafka.applyDefaults()

	if n := len(app.Kafka.Consumers); n != 2 {
		t.Fatalf("decoded %d consumers, want 2", n)
	}

	o := app.Kafka.Consumers["orders"]
	if o.GroupID != "example-orders" { // squashed consumer.Config field
		t.Errorf("orders.group_id = %q, want example-orders (squash failed?)", o.GroupID)
	}
	if len(o.Topics) != 1 || o.Topics[0] != "orders" {
		t.Errorf("orders.topics = %v, want [orders]", o.Topics)
	}
	if o.Concurrency != 128 {
		t.Errorf("orders.concurrency = %d, want 128", o.Concurrency)
	}
	if !o.Retry.Enabled || len(o.Retry.Backoffs) != 2 || !o.Retry.DLQ.Enabled {
		t.Errorf("orders.retry decoded wrong: %+v", o.Retry)
	}
	if !o.Dedup.Enabled || o.Dedup.TTL != 2*time.Hour {
		t.Errorf("orders.dedup decoded wrong: enabled=%v ttl=%v", o.Dedup.Enabled, o.Dedup.TTL)
	}

	a := app.Kafka.Consumers["audit"]
	if a.GroupID != "example-audit" || a.Mode != "ordered" || len(a.Topics) != 2 {
		t.Errorf("audit decoded wrong: group=%q mode=%q topics=%v", a.GroupID, a.Mode, a.Topics)
	}
	if a.Retry.Enabled {
		t.Error("audit.retry.enabled should be false")
	}
}
