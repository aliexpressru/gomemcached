// nolint
package memcached

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/aliexpressru/gomemcached/consistenthash"
	"github.com/aliexpressru/gomemcached/logger"
)

func TestWithOptionsBase(t *testing.T) {
	os.Setenv("AER_MEMCACHED_SERVERS", "localhost:11211")
	os.Setenv("AER_MEMCACHED_PORT", "99999")

	ctx := context.TODO()

	hMcl, _ := InitFromEnv(ctx)
	assert.NotNil(t, hMcl.hr, "InitFromEnv: hash ring is nil")

	const (
		maxIdleConns = 10
		disable      = true
		enable
		authUser = "admin"
		authPass = "password"
		timeout  = 5 * time.Second
		period   = time.Second
	)

	hr := consistenthash.NewCustomHashRing(1, nil)
	mcl, _ := InitFromEnv(
		ctx,
		WithMaxIdleConns(maxIdleConns),
		WithTimeout(timeout),
		WithCustomHashRing(hr),
		WithPeriodForNodeHealthCheck(period),
		WithPeriodForRebuildingNodes(period),
		WithDisableNodeProvider(),
		WithDisableRefreshConnsInPool(),
		WithDisableMemcachedDiagnostic(),
		WithAuthentication(authUser, authPass),
		WithDisableLogger(),
	)
	t.Cleanup(func() { _ = mcl.CloseAllConns(ctx) })

	// envs
	assert.Equal(t, []string{"localhost:11211"}, mcl.cfg.Servers, "Client from env should use servers from env")
	assert.Equal(t, 99999, mcl.cfg.MemcachedPort, "Client from env should use port from env")
	// options
	assert.Equal(t, maxIdleConns, mcl.maxIdleConns, "WithMaxIdleConns should set maxIdleConns")
	assert.Equal(t, timeout, mcl.timeout, "WithTimeout should set timeout")
	assert.Equal(t, hr, mcl.hr, "WithCustomHashRing should set hr")
	assert.Equal(t, period, mcl.nodeHCPeriod, "WithPeriodForNodeHealthCheck should set period")
	assert.Equal(t, period, mcl.nodeRBPeriod, "WithPeriodForRebuildingNodes should set period")
	assert.Equal(t, disable, mcl.disableNodeProvider, "WithDisableNodeProvider should set disable")
	assert.Equal(t, disable, mcl.disableRefreshConns, "WithDisableRefreshConnsInPool should set disable")
	assert.Equal(t, disable, mcl.disableMemcachedDiagnostic, "WithDisableMemcachedDiagnostic should set disable")
	assert.Equal(t, enable, mcl.authEnable, "WithAuthentication should set enable")
	assert.Equal(t, disable, logger.IsDisable(), "WithDisableLogger should set disable")
}

func TestWithOptionsOverrides(t *testing.T) {
	// envs for override
	os.Setenv("AER_MEMCACHED_SERVERS", "localhost:11211")
	os.Setenv("AER_MEMCACHED_PORT", "99999")

	ctx := context.TODO()

	var (
		optionPort    = 11215
		optionServers = []string{"localhost:11213", "localhost:11214"}
	)
	clientWithOptions, err := InitFromEnv(ctx,
		WithServersList(optionServers),
		WithMemcachedPort(optionPort),
		WithDisableNodeProvider(),
	)
	assert.NoError(t, err)
	assert.NotNil(t, clientWithOptions)

	t.Cleanup(func() {
		if clientWithOptions != nil {
			clientWithOptions.CloseAllConns(ctx)
		}
	})

	assert.Equal(t, optionServers, clientWithOptions.cfg.Servers, "WithServersList should override env")
	assert.Equal(t, optionPort, clientWithOptions.cfg.MemcachedPort, "WithMemcachedPort should override env")

	// We check WithHeadlessServiceAddress separately, since when installing it, the client
	// prioritizes headless over servers and tries to make a lookup
	opt := &options{
		Client: Client{
			cfg: &config{},
		},
	}
	WithHeadlessServiceAddress("option-headless.local")(opt)
	assert.Equal(t, "option-headless.local", opt.cfg.HeadlessServiceAddress, "WithHeadlessServiceAddress should set headless address")
}

func TestWithNamespace(t *testing.T) {
	ctx := context.TODO()

	t.Run("default_namespace_is_aer", func(t *testing.T) {
		// Set environment variables with default AER prefix
		t.Setenv("AER_MEMCACHED_SERVERS", "localhost:11211")
		t.Setenv("AER_MEMCACHED_PORT", "11212")

		client, err := InitFromEnv(ctx, WithDisableNodeProvider())
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.CloseAllConns(ctx)

		assert.Equal(t, []string{"localhost:11211"}, client.cfg.Servers, "Should read from AER_MEMCACHED_SERVERS by default")
		assert.Equal(t, 11212, client.cfg.MemcachedPort, "Should read from AER_MEMCACHED_PORT by default")
	})

	t.Run("override_namespace_to_empty", func(t *testing.T) {
		// Set environment variables without prefix
		t.Setenv("MEMCACHED_SERVERS", "localhost:11213")
		t.Setenv("MEMCACHED_PORT", "11214")

		client, err := InitFromEnv(
			ctx,
			WithNamespace(""),  // Override to use no prefix
			WithDisableNodeProvider(),
		)
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.CloseAllConns(ctx)

		assert.Equal(t, []string{"localhost:11213"}, client.cfg.Servers, "Should read from MEMCACHED_SERVERS with empty namespace")
		assert.Equal(t, 11214, client.cfg.MemcachedPort, "Should read from MEMCACHED_PORT with empty namespace")
	})

	t.Run("override_namespace_to_custom", func(t *testing.T) {
		// Set environment variables with custom prefix
		t.Setenv("CUSTOM_MEMCACHED_SERVERS", "localhost:11215")
		t.Setenv("CUSTOM_MEMCACHED_PORT", "11216")

		client, err := InitFromEnv(
			ctx,
			WithNamespace("custom"),
			WithDisableNodeProvider(),
		)
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.CloseAllConns(ctx)

		assert.Equal(t, []string{"localhost:11215"}, client.cfg.Servers, "Should read from CUSTOM_MEMCACHED_SERVERS")
		assert.Equal(t, 11216, client.cfg.MemcachedPort, "Should read from CUSTOM_MEMCACHED_PORT")
	})
}
