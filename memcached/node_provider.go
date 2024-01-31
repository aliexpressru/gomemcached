package memcached

import (
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

func (c *Client) initNodesProvider() {
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
				c.checkNodesHealth()
				tHC.Reset(periodHC)
			case <-c.ctx.Done():
				tHC.Stop()
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case <-tRB.C:
				c.rebuildNodes()
				tRB.Reset(periodRB)
			case <-c.ctx.Done():
				tRB.Stop()
				return
			}
		}
	}()
}

func (c *Client) checkNodesHealth() {
	currentNodes, err := getNodes(c.nw.lookupHost, c.cfg)
	if err != nil {
		logger.Warnf("%s: Error occurred while checking nodes health, getNodes error - %s", libPrefix, err.Error())
		return
	}

	recheckDeadNodes := func(node any) {
		sNode := utils.Repr(node)
		if !slices.Contains(currentNodes, sNode) {
			c.safeRemoveFromDeadNodes(sNode)
			return
		}

		if c.nodeIsDead(node) {
			c.safeAddToDeadNodes(sNode)
		} else {
			c.safeRemoveFromDeadNodes(sNode)
		}
	}

	var (
		wg        = sync.WaitGroup{}
		deadNodes = c.safeGetDeadNodes()
	)
	for node := range deadNodes {
		wg.Add(1)
		go func(n string) {
			recheckDeadNodes(n)
			wg.Done()
		}(node)
	}
	wg.Wait()

	ringNodes := c.hr.GetAllNodes()
	for node := range deadNodes {
		ringNodes = slices.DeleteFunc(ringNodes, func(a any) bool { return utils.Repr(a) == node })
	}

	for _, node := range ringNodes {
		wg.Add(1)
		go func(n any) {
			if c.nodeIsDead(n) {
				sNode := utils.Repr(n)
				c.safeAddToDeadNodes(sNode)
			}
			wg.Done()
		}(node)
	}

	wg.Wait()

	if len(deadNodes) != 0 {
		nodes := maps.Keys(deadNodes)

		logger.Warnf("%s: Dead nodes - %s", libPrefix, nodes)

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

func (c *Client) rebuildNodes() {
	currentNodes, err := getNodes(c.nw.lookupHost, c.cfg)
	if err != nil {
		logger.Warnf("%s: Error occurred while rebuild nodes health, getNodes error - %s", libPrefix, err.Error())
		return
	}
	slices.Sort(currentNodes)

	deadNodes := c.safeGetDeadNodes()
	for node := range deadNodes {
		currentNodes = slices.DeleteFunc(currentNodes, func(a string) bool { return a == node })
	}

	var nodesInRing []string
	for _, node := range c.hr.GetAllNodes() {
		nodesInRing = append(nodesInRing, utils.Repr(node))
	}
	slices.Sort(nodesInRing)

	var nodesToAdd []string
	for _, node := range currentNodes {
		if _, ok := slices.BinarySearch(nodesInRing, node); !ok {
			nodesToAdd = append(nodesToAdd, node)
		}
	}

	var nodesToRemove []string
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
		_ = c.CloseAvailableConnsInAllShardPools(DefaultOfNumberConnsToDestroyPerRBPeriod)
	}
}

func (c *Client) nodeIsDead(node any) bool {
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
				logger.Errorf("%s. Node health check failed. error - %s, with timeout - %d",
					ErrServerError.Error(), err.Error(), c.netTimeout(),
				)
				return true
			} else {
				logger.Errorf("%s. %s", ErrServerError.Error(), err.Error())
				return true
			}
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
			return cfg.Servers, nil
		}
	}

	return []string{}, nil
}
