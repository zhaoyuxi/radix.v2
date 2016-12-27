// Package sentinel provides a convenient interface with a redis sentinel which
// will automatically handle pooling connections and failover.
//
// Here's an example of creating a sentinel client and then using it to perform
// some commands
//
//	func example() error {
//		// If there exists sentinel masters "bucket0" and "bucket1", and we want
//		// out client to create pools for both:
//		client, err := sentinel.NewClient("tcp", "localhost:6379", 100, "bucket0", "bucket1")
//		if err != nil {
//			return err
//		}
//
//		if err := exampleCmd(client); err != nil {
//			return err
//		}
//
//		return nil
//	}
//
//	func exampleCmd(client *sentinel.Client) error {
//		conn, err := client.GetMaster("bucket0")
//		if err != nil {
//			return redisErr
//		}
//		defer client.PutMaster("bucket0", conn)
//
//		i, err := conn.Cmd("GET", "foo").Int()
//		if err != nil {
//			return err
//		}
//
//		if err := conn.Cmd("SET", "foo", i+1); err != nil {
//			return err
//		}
//
//		return nil
//	}
//
// This package only guarantees that when Do is called the used connection will
// be a connection to the master as of the moment that method is called. It is
// still possible that there is a failover in the middle of an Action.
package sentinel

import (
	"errors"
	"sync"

	radix "github.com/mediocregopher/radix.v2"
)

type sentinelClient struct {
	// we read lock when calling methods on p, and normal lock when swapping the
	// value of p, pAddr, or modifying addrs
	sync.RWMutex
	p     radix.Pool
	pAddr string
	addrs []string // the known sentinel addresses

	name string
	dfn  radix.DialFunc // the function used to dial sentinel instances
	pfn  radix.PoolFunc
}

func (sc *sentinelClient) Do(a radix.Action) error {
	sc.RLock()
	defer sc.RUnlock()
	return sc.p.Do(a)
}

func (sc *sentinelClient) Close() error {
	sc.RLock()
	defer sc.RUnlock()
	// TODO probably need to stop the sentinel conn
	return sc.p.Close()
}

func (sc *sentinelClient) Get() (radix.PoolConn, error) {
	sc.RLock()
	defer sc.RUnlock()
	return sc.p.Get()
}

// given a connection to a sentinel, ensures that the pool currently being held
// agrees with what the sentinel thinks it should be
func (sc *sentinelClient) ensureMaster(conn radix.Conn) error {
	sc.Lock()
	lastAddr := sc.pAddr
	sc.Unlock()

	var m map[string]string
	err := radix.CmdNoKey("SENTINEL", "MASTER", sc.name).Into(&m).Run(conn)
	if err != nil {
		return err
	} else if m["ip"] == "" || m["port"] == "" {
		return errors.New("malformed SENTINEL MASTER response")
	}
	newAddr := m["ip"] + ":" + m["port"]
	if newAddr == lastAddr {
		return nil
	}

	newPool, err := sc.pfn("tcp", newAddr)
	if err != nil {
		return err
	}

	sc.Lock()
	if sc.p != nil {
		sc.p.Close()
	}
	sc.p = newPool
	sc.Unlock()

	return nil
}

// annoyingly the SENTINEL SENTINELS <name> command doesn't return _this_
// sentinel instance, only the others it knows about for that master
func (sc *sentinelClient) sentinelAddrs() ([]string, error) {
	/*
		127.0.0.1:28001> sentinel sentinels test
		1)  1) "name"
		    2) "09f9f2c99556e12b69295e6b320b5dff64a90fb9"
		    3) "ip"
		    4) "127.0.0.1"
		    5) "port"
		    6) "28000"
		    7) "runid"
		    8) "09f9f2c99556e12b69295e6b320b5dff64a90fb9"
		    9) "flags"
		   10) "sentinel"
		   11) "link-pending-commands"
		   12) "0"
		   13) "link-refcount"
		   14) "1"
		   15) "last-ping-sent"
		   16) "0"
		   17) "last-ok-ping-reply"
		   18) "756"
		   19) "last-ping-reply"
		   20) "756"
		   21) "down-after-milliseconds"
		   22) "60000"
		   23) "last-hello-message"
		   24) "264"
		   25) "voted-leader"
		   26) "?"
		   27) "voted-leader-epoch"
		   28) "0"
	*/
}
