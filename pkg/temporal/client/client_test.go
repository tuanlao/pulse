package client

import (
	"context"
	"errors"
	"testing"

	sdkclient "go.temporal.io/sdk/client"
)

func TestConfigApplyDefaults(t *testing.T) {
	var c Config
	c.ApplyDefaults()
	if c.DefaultWorkflowTaskTimeout != DefaultConfig().DefaultWorkflowTaskTimeout {
		t.Fatalf("DefaultWorkflowTaskTimeout = %v, want %v",
			c.DefaultWorkflowTaskTimeout, DefaultConfig().DefaultWorkflowTaskTimeout)
	}
}

func TestDisabledClient(t *testing.T) {
	c := Disabled(nil)

	if c.Enabled() {
		t.Error("Enabled() = true, want false")
	}
	if c.SDK() != nil {
		t.Error("SDK() should be nil for a disabled client")
	}
	if c.Name() != "temporal-client" {
		t.Errorf("Name() = %q, want %q", c.Name(), "temporal-client")
	}
	// Lifecycle methods must be safe no-ops (never touch the nil embedded client).
	if err := c.Start(context.Background()); err != nil {
		t.Errorf("Start() = %v, want nil", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Errorf("Stop() = %v, want nil", err)
	}
	if err := c.CheckReady(context.Background()); err != nil {
		t.Errorf("CheckReady() = %v, want nil", err)
	}
	// StartWorkflow returns a sentinel rather than panicking.
	if _, err := c.StartWorkflow(context.Background(), sdkclient.StartWorkflowOptions{}, "WF"); !errors.Is(err, ErrDisabled) {
		t.Errorf("StartWorkflow() err = %v, want ErrDisabled", err)
	}
}

func TestNewOwnedLazyClient(t *testing.T) {
	// New builds a LAZY client, so this succeeds with no Temporal server running
	// (no connection is attempted until Start's health check, which we don't call).
	c, err := New(DefaultConfig(), Deps{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !c.Enabled() {
		t.Error("Enabled() = false, want true")
	}
	if c.SDK() == nil {
		t.Error("SDK() should be non-nil for an enabled client")
	}
	// Stop closes the owned (lazy, never-connected) client without error.
	if err := c.Stop(context.Background()); err != nil {
		t.Errorf("Stop() = %v, want nil", err)
	}
}

func TestNewSharedClientNotOwned(t *testing.T) {
	// Build a lazy client to inject as a shared connection.
	shared, err := New(DefaultConfig(), Deps{})
	if err != nil {
		t.Fatalf("New(shared) error = %v", err)
	}
	c, err := New(DefaultConfig(), Deps{SDKClient: shared.SDK()})
	if err != nil {
		t.Fatalf("New(wrapping shared) error = %v", err)
	}
	if c.owned {
		t.Error("a client wrapping an injected SDKClient must not be owned")
	}
	// Stopping the wrapper must NOT close the shared client (owned == false); the
	// owner (shared) closes it.
	if err := c.Stop(context.Background()); err != nil {
		t.Errorf("Stop(wrapper) = %v, want nil", err)
	}
	if err := shared.Stop(context.Background()); err != nil {
		t.Errorf("Stop(shared) = %v, want nil", err)
	}
}
