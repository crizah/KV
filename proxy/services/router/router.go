package router

import (
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"reflect"
	"sync"

	zmq "github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

type RouteTable struct {
	mx             sync.RWMutex
	routeMapCPU    map[int64]map[string]struct{} // maps hash to an array of nodes in CPU
	routeMapGPU    map[int64]map[string]struct{} // in GPU
	endpoint       string                        // endpoint to listen to for kv cache
	replayEndpoint string                        // endpoint to listen to for replay of dropped caches
	nodes          []string                      // array of nodes that exist, registered at startup
}

func NewRouteTableService(endpoint string, replayEndpoint string, nodes []string) *RouteTable {
	c := make(map[int64]map[string]struct{})
	g := make(map[int64]map[string]struct{})
	return &RouteTable{routeMapCPU: c, routeMapGPU: g,
		endpoint: endpoint, replayEndpoint: replayEndpoint, nodes: nodes}
}

func (table *RouteTable) writeCache(events *KVEventBatch) {
	table.mx.Lock()
	defer table.mx.Unlock()
	// idk what to do with the other params in this batch res
	// TODO: keep track of seq to find out if we dropped an event, if we did, listen for those through the replayEndpoint
	for _, event := range events.events {
		switch event.Type {
		case "BlockStored":
			// 	{
			// 		"type": "BlockStored",
			// 		"block_hashes": [int, ...],       # list of int64 hashes
			// 		"parent_block_hash": int | None,  # hash of the preceding block
			// 		"token_ids": [int, ...],          # actual token IDs that fill this block
			// 		"block_size": int,                # tokens per block
			// 		"lora_id": int | None,
			// 		"medium": "GPU" | "CPU_PINNED" | "DISK" | "EXTERNAL" | None,
			// 		"cache_salt": str | None,
			// 		"session_id": str | None,
			// 		"lora_name": str | None,          # only in "dynamo" format
			// 	}

			medium, ok := event.Fields["medium"].(string)
			if !ok {
				// idk just return ig lol
				return
			}
			h, ok := event.Fields["block_hashes"].([]any)
			if !ok {
				return
			}
			hashes := make([]int64, len(h))
			for _, hh := range h {
				num, ok := toInt64(hh)
				if !ok {
					return
				}
				hashes = append(hashes, num)
			}

			switch medium {
			case "GPU":
				for _, hash := range hashes {
					table.routeMapGPU[hash][events.nodeIP] = struct{}{}
				}
			case "CPU_PINNED":
				for _, hash := range hashes {
					table.routeMapCPU[hash][events.nodeIP] = struct{}{}
				}
			default:
				// nuh huh, we dont need to do anything i think
				continue

			}
		case "BlockRemoved":
			// 	{
			// 	  "type": "BlockRemoved",
			// 	  "block_hashes": [int, ...],
			// 	  "medium" : "GPU" | "CPU_PINNED" | ... | None,
			// 	}
			medium, ok := event.Fields["medium"].(string)
			if !ok {
				// idk
			}
			hashes, ok := event.Fields["block_hashes"].([]int64)
			if !ok {
				// idk
			}
			switch medium {
			case "GPU":
				for _, hash := range hashes {
					delete(table.routeMapGPU[hash], events.nodeIP)
				}
			case "CPU_PINNED":
				for _, hash := range hashes {
					delete(table.routeMapCPU[hash], events.nodeIP)
				}
			default:
				// nuh huh
				continue

			}
		default:
			// 	{"type": "AllBlocksCleared"}
			// remove from both tables
			for h, _ := range table.routeMapCPU {
				delete(table.routeMapCPU[h], events.nodeIP)
			}
			for h, _ := range table.routeMapGPU {
				delete(table.routeMapCPU[h], events.nodeIP)
			}
		}
	}

}

func (table *RouteTable) ReadCache(ctx context.Context, hash int64) map[string]struct{} {
	// reads the cache for this hash and returns the []string of a nodes if it exists in the table
	// check both GPU and CPU but i thinkwe are adding events in batches so only cpu should be fine?
	// if an event does have both gpu and cpu, are we not addint it in one lock?
	table.mx.RLock()
	defer table.mx.RUnlock()
	node, ok := table.routeMapCPU[hash]
	if !ok {
		return nil
	}
	// return a copy here since we dont need to modify it anymore
	n := maps.Clone(node)
	return n
}

type rawKVEvent struct {
	Type   string // "BlockStored", "BlockRemoved", "AllBlocksCleared"
	Fields map[string]any
}

type KVEventBatch struct {
	// the batched event
	seq        uint64
	ts         float64
	attnDPRank *int64
	events     []rawKVEvent
	error      error
	nodeIP     string
}

func (table *RouteTable) Listen(ctx context.Context) {
	// keep one channel that all the listeners write to
	// and one routine that reads from the channel
	ch := make(chan *KVEventBatch, 1024)

	for _, n := range table.nodes {
		// have all of them write to ch
		nodeIP := n + table.endpoint
		go listenNode(ctx, nodeIP, n, ch)
	}

	// idk guys should the listener be before we start spinning up the routines? probaby right?
	select {
	case event := <-ch:
		if event.error != nil {
			// retry this guy here

		} else {
			// write sequentially because we have a lock anyways
			table.writeCache(event)
		}
	case <-ctx.Done():
		return

	}
}

func listenNode(ctx context.Context, nodeIP string, node string, ch chan<- *KVEventBatch) {
	// listens to a the endpoint of one node
	// create a new socket
	// add cancel to the context

	sock := zmq.NewSub(ctx)
	defer sock.Close()
	event := &KVEventBatch{nodeIP: node}

	if err := sock.SetOption(zmq.OptionSubscribe, ""); err != nil {
		event.error = err
		ch <- event
		return
	}

	if err := sock.Dial(nodeIP); err != nil {
		err := fmt.Errorf("zmq dial %s: %w", nodeIP, err)
		event.error = err
		ch <- event
		return
	}

	for {
		msg, err := sock.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				err := fmt.Errorf("zmq recv: %w", err)
				event.error = err
				ch <- event
				return
			}
		}
		if len(msg.Frames) != 3 {
			continue // malformed, skip
		}

		seq := binary.BigEndian.Uint64(msg.Frames[1])
		batch := decodeBatch(seq, msg.Frames[2])
		batch.nodeIP = node
		if batch.error != nil {
			continue // bad msgpack frame, skip and keep going
		}

		select {
		case ch <- batch:
		case <-ctx.Done():
			return
		}
	}

}

func decodeBatch(seq uint64, payload []byte) *KVEventBatch {
	// NOTE: keep array_like=True on the python side
	// array [ts:float64, events:[]any, attn_dp_rank:int|nil]
	var raw []any
	if err := msgpack.Unmarshal(payload, &raw); err != nil {
		return &KVEventBatch{error: err}
	}
	if len(raw) < 2 {
		err := fmt.Errorf("batch too short: %d fields", len(raw))
		return &KVEventBatch{error: err}
	}

	ts, _ := raw[0].(float64)

	var dpRank *int64
	if len(raw) >= 3 && raw[2] != nil {
		v, ok := toInt64(raw[2])
		if !ok {
			err := fmt.Errorf("couldnt type convert")
			return &KVEventBatch{error: err}
		}
		dpRank = &v
	}

	rawEvents, _ := raw[1].([]any)
	events := make([]rawKVEvent, 0, len(rawEvents))
	for _, re := range rawEvents {
		m, ok := re.(map[string]any)
		if !ok {
			continue
		}
		typ, _ := m["type"].(string)
		delete(m, "type")
		events = append(events, rawKVEvent{Type: typ, Fields: m})
	}

	return &KVEventBatch{seq: seq, ts: ts, attnDPRank: dpRank, events: events, error: nil}
}

func toInt64(v any) (int64, bool) {
	// to change diff int type to 64
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), true
	}
	return 0, false
}
