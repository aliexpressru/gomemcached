package memcached

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/aliexpressru/gomemcached/consistenthash"
	"github.com/aliexpressru/gomemcached/logger"
)

func TestWithOptions(t *testing.T) {
	os.Setenv("MEMCACHED_SERVERS", "localhost:11211")

	hMcl, _ := InitFromEnv()
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
	t.Cleanup(func() {
		mcl.CloseAllConns()
	})

	assert.Equal(t, maxIdleConns, mcl.maxIdleConns, "WithMaxIdleConns should set maxIdleConns")
	assert.Equal(t, timeout, mcl.timeout, "WithTimeout should set timeout")
	assert.Equal(t, hr, mcl.hr, "WithCustomHashRing should set hr")
	assert.Equal(t, period, mcl.nodeHCPeriod, "WithPeriodForNodeHealthCheck should set period")
	assert.Equal(t, period, mcl.nodeRBPeriod, "WithPeriodForRebuildingNodes should set period")
	assert.Equal(t, disable, mcl.disableNodeProvider, "WithDisableNodeProvider should set disable")
	assert.Equal(t, disable, mcl.disableRefreshConns, "WithDisableRefreshConnsInPool should set disable")
	assert.Equal(t, disable, mcl.disableMemcachedDiagnostic, "WithDisableMemcachedDiagnostic should set disable")
	assert.Equal(t, enable, mcl.authEnable, "WithAuthentication should set enable")
	assert.Equal(t, disable, logger.LoggerIsDisable(), "WithDisableLogger should set disable")
}
