//go:build integration

// End-to-end integration test for the canonical flow this library exists to
// support: an HTTP API publishes a Kafka event and a worker (consumer) receives
// and processes it. It boots a focused subset of the example service (HTTP server
// + Kafka producer + Kafka consumer) wired through the lifecycle manager exactly
// like main(), then drives it through the outbound HTTP client.
//
// It needs a live Kafka broker. Run with the docker-compose stack:
//
//	make infra-up
//	go test -race -tags=integration ./examples/service/... -run TestE2E -v
//
// The broker address comes from KAFKA_BROKERS (default localhost:9092).
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/http/client"
	"github.com/tuanlao/pulse/pkg/http/server"
	"github.com/tuanlao/pulse/pkg/kafka"
	"github.com/tuanlao/pulse/pkg/lifecycle"
	"github.com/tuanlao/pulse/pkg/log"
)

// kafkaBrokers returns the seed brokers for integration tests (KAFKA_BROKERS,
// comma-separated; default localhost:9092).
func kafkaBrokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9092"}
}

// freePort asks the OS for an unused TCP port so two parallel runs don't collide
// on the server's listen port (the server binds a fixed port from its config).
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestE2E_PublishViaAPI_WorkerConsumes(t *testing.T) {
	const topic = "orders" // registerPublishRoute publishes here
	logger := log.Nop()
	port := freePort(t)
	group := "e2e-" + uuid.NewString()

	// HTTP server (gin) on a private port, in gin "test" mode.
	srv, err := server.New(server.DefaultConfig(), server.Deps{Logger: logger},
		server.WithPort(port), server.WithMode("test"))
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	// Kafka config: the producer provisions the "orders" topic; the consumer runs
	// in its default at-least-once mode and auto-provisions its retry tiers + DLQ.
	kcfg := kafka.DefaultConfig()
	kcfg.ServiceName = "e2e-test"
	kcfg.Producer.Topics = []string{topic}

	producer, err := kafka.NewProducer(kcfg, kafka.Deps{Logger: logger},
		kafka.WithBrokers(kafkaBrokers()...))
	if err != nil {
		t.Fatalf("kafka.NewProducer: %v", err)
	}
	consumer, err := kafka.NewConsumer(kcfg, kafka.Deps{Logger: logger},
		kafka.WithBrokers(kafkaBrokers()...), kafka.WithGroupID(group), kafka.WithTopics(topic))
	if err != nil {
		t.Fatalf("kafka.NewConsumer: %v", err)
	}

	// The worker: decode the payload into orderEvent and hand it to the test. The
	// channel is buffered so republishes (below) never block the handler.
	received := make(chan orderEvent, 16)
	kafka.On(consumer, topic, func(_ context.Context, e orderEvent, _ *kafka.Message) error {
		received <- e
		return nil
	})

	// The real HTTP route: POST /publish/:id -> kafka.Send(topic="orders").
	registerPublishRoute(srv, producer, logger)

	// Outbound client pointed at our own server (this is the "test call API" leg).
	ccfg := client.DefaultConfig()
	ccfg.BaseURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	cli, err := client.New(ccfg, client.Deps{Logger: logger})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	// Lifecycle: producer (provisions topic) -> consumer (subscribes) -> server last.
	mgr := lifecycle.New(lifecycle.DefaultConfig(), logger.LifecycleAdapter())
	mgr.Register(producer, consumer, srv)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(30 * time.Second):
			t.Error("lifecycle did not shut down in time")
		}
	})

	waitServerReady(t, cli, port)

	// Publish via the API, retrying until the worker reports the event. The retry
	// makes the test robust to the consumer-group join race (a brand-new group may
	// not be assigned its partitions the instant the server reports ready).
	const id = "order-123"
	publish := func() {
		var resp map[string]any
		if err := cli.PostJSON(ctx, "/publish/"+id, nil, &resp); err != nil {
			t.Errorf("POST /publish/%s: %v", id, err)
		}
	}
	publish()

	republish := time.NewTicker(2 * time.Second)
	defer republish.Stop()
	deadline := time.After(30 * time.Second)
	for {
		select {
		case got := <-received:
			if got.ID != id {
				t.Fatalf("worker received wrong event: got id %q, want %q", got.ID, id)
			}
			return // success: API -> publish -> worker consumed
		case <-republish.C:
			publish()
		case <-deadline:
			t.Fatal("timed out waiting for the worker to consume the published event")
		}
	}
}

// waitServerReady polls /healthz until the HTTP server is serving (or fails).
func waitServerReady(t *testing.T, cli *client.Client, port int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		var out map[string]any
		err := cli.GetJSON(ctx, "/healthz", &out)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("http server on port %d never became ready", port)
}
