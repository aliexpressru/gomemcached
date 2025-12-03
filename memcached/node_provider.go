package memcached

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"github.com/aliexpressru/gomemcached/logger"
	"github.com/aliexpressru/gomemcached/utils"
)

func (c *Client) initNodesProvider(ctx context.Context) {
	var (
		periodHC = c.getHCPeriod()
		tHC      = time.NewTimer(periodHC)

		periodRB = c.getRBPeriod()
		tRB      = time.NewTimer(periodRB)
	)

	if c.deadNodes == nil {
		c.deadNodes = make(map[string]struct{})
	}

	go func() {
		for {
			select {
			case <-tHC.C:
				c.checkNodesHealth(ctx)
				tHC.Reset(periodHC)
			case <-ctx.Done():
				tHC.Stop()
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case <-tRB.C:
				c.rebuildNodes(ctx)
				tRB.Reset(periodRB)
			case <-ctx.Done():
				tRB.Stop()
				return
			}
		}
	}()
}

func (c *Client) checkNodesHealth(ctx context.Context) {
	currentNodes, err := getNodes(c.nw.lookupHost, c.cfg)
	if err != nil {
		logger.Warnf(ctx, "%s: Error occurred while checking nodes health, getNodes error - %s", libPrefix, err.Error())
		return
	}

	recheckDeadNodes := func(node any) {
		sNode := utils.Repr(node)
		if !slices.Contains(currentNodes, sNode) {
			c.safeRemoveFromDeadNodes(sNode)
			return
		}

		if c.nodeIsDead(ctx, node) {
			c.safeAddToDeadNodes(sNode)
		} else {
			c.safeRemoveFromDeadNodes(sNode)
			logger.Warnf(ctx, "%s: Recovered node - %s", libPrefix, sNode)
		}
	}

	wg := sync.WaitGroup{}
	for node := range c.safeGetDeadNodes() {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			recheckDeadNodes(n)
		}(node)
	}
	wg.Wait()

	ringNodes := c.hr.GetAllNodes()
	for node := range c.safeGetDeadNodes() {
		ringNodes = slices.DeleteFunc(ringNodes, func(a any) bool { return utils.Repr(a) == node })
	}

	for _, node := range ringNodes {
		wg.Add(1)
		go func(n any) {
			defer wg.Done()
			if c.nodeIsDead(ctx, n) {
				sNode := utils.Repr(n)
				c.safeAddToDeadNodes(sNode)
			}
		}(node)
	}

	wg.Wait()

	deadNodes := c.safeGetDeadNodes()
	if len(deadNodes) != 0 {
		nodes := maps.Keys(deadNodes)

		logger.Warnf(ctx, "%s: Dead nodes - %s", libPrefix, nodes)

		for _, node := range nodes {
			addr, cErr := utils.AddrRepr(node)
			if cErr != nil {
				continue
			}
			c.hr.Remove(addr)
			c.removeFromFreeConns(addr)
		}
	}
}

func (c *Client) rebuildNodes(ctx context.Context) {
	currentNodes, err := getNodes(c.nw.lookupHost, c.cfg)
	if err != nil {
		logger.Warnf(ctx, "%s: Error occurred while rebuild nodes health, getNodes error - %s", libPrefix, err.Error())
		return
	}
	slices.Sort(currentNodes)

	for node := range c.safeGetDeadNodes() {
		currentNodes = slices.DeleteFunc(currentNodes, func(a string) bool { return a == node })
	}

	var (
		allNodes    = c.hr.GetAllNodes()
		nodesInRing = make([]string, len(allNodes))
	)
	for i := range allNodes {
		nodesInRing[i] = utils.Repr(allNodes[i])
	}
	slices.Sort(nodesInRing)

	nodesToAdd := make([]string, 0, len(currentNodes))
	for _, node := range currentNodes {
		if _, ok := slices.BinarySearch(nodesInRing, node); !ok {
			nodesToAdd = append(nodesToAdd, node)
		}
	}

	nodesToRemove := make([]string, 0, len(nodesInRing))
	for _, node := range nodesInRing {
		if _, ok := slices.BinarySearch(currentNodes, node); !ok {
			nodesToRemove = append(nodesToRemove, node)
		}
	}

	if len(nodesToAdd) != 0 {
		for _, node := range nodesToAdd {
			addr, cErr := utils.AddrRepr(node)
			if cErr != nil {
				continue
			}
			c.hr.Add(addr)
		}
	}

	if len(nodesToRemove) != 0 {
		for _, node := range nodesToRemove {
			addr, cErr := utils.AddrRepr(node)
			if cErr != nil {
				continue
			}
			c.hr.Remove(addr)
		}
	}

	if !c.disableRefreshConns {
		_, err = c.CloseAvailableConnsInAllShardPools(ctx, DefaultOfNumberConnsToDestroyPerRBPeriod)
		if err != nil {
			logger.Warnf(ctx, "%s: Error occurred while draining connections, CloseAvailableConnsInAllShardPools error - %s",
				libPrefix, err.Error(),
			)
		}
	}
}

func (c *Client) nodeIsDead(ctx context.Context, node any) bool {
	addr, err := utils.AddrRepr(utils.Repr(node))
	if err != nil {
		return true
	}

	var (
		countRetry uint8
		cn         net.Conn
	)

	for {
		cn, err = c.dial(addr)
		if err != nil {
			var tErr *ConnectTimeoutError
			if errors.As(err, &tErr) {
				if countRetry < DefaultRetryCountForConn {
					countRetry++
					continue
				}
				logger.Errorf(ctx, "%s. Node health check failed. error - %s, with timeout - %s",
					ErrServerError.Error(), err.Error(), c.netTimeout(),
				)
				return true
			}
			logger.Errorf(ctx, "%s. %s", ErrServerError.Error(), err.Error())
			return true
		}
		_ = cn.Close()
		break
	}

	return false
}

func (c *Client) safeGetDeadNodes() map[string]struct{} {
	c.dmu.RLock()
	defer c.dmu.RUnlock()
	return maps.Clone(c.deadNodes)
}

func (c *Client) safeAddToDeadNodes(node string) {
	c.dmu.Lock()
	defer c.dmu.Unlock()
	c.deadNodes[node] = struct{}{}
}

func (c *Client) safeRemoveFromDeadNodes(node string) {
	c.dmu.Lock()
	defer c.dmu.Unlock()
	delete(c.deadNodes, node)
}

func getNodes(lookup func(host string) (addrs []string, err error), cfg *config) ([]string, error) {
	if cfg != nil {
		if cfg.HeadlessServiceAddress != "" {
			nodes, err := lookup(cfg.HeadlessServiceAddress)
			if err != nil {
				return nil, err
			}

			nodesWithHost := make([]string, len(nodes))
			for i := range nodes {
				nodesWithHost[i] = net.JoinHostPort(nodes[i], strconv.Itoa(cfg.MemcachedPort))
			}

			return nodesWithHost, nil
		} else if len(cfg.Servers) != 0 {
			for _, s := range cfg.Servers {
				_, _, err := net.SplitHostPort(s)
				if err != nil {
					return nil, err
				}
			}
			return cfg.Servers, nil
		}
	}

	return []string{}, nil
}
