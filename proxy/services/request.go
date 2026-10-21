package services

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/daulet/tokenizers"
)

type RequestService struct {
	model    Model
	input    string
	pageSize int
}

type Model string

const (
	QWEN306B Model = "Qwen3-0.6B"
)

func NewRequestService(model Model, input string) (*RequestService, error) {
	// TODO: 16 is a default. MAKE SURE THE SERVER AND PROXY USE THE SAME NUMBER FOR THIS
	return &RequestService{model: model, input: input, pageSize: 16}, nil
}

func (req *RequestService) HandleReq() error {
	// tokenise her
	encoded, err := req.tokenise()
	if err != nil {
		return fmt.Errorf("error encoding %s", err.Error())
	}

	// hash her ain a chain until we hit a cache miss
	N, err := req.hasher(encoded, nil)
	if err != nil {
		return fmt.Errorf("handleReq error %s", err.Error)
	}
	// at this point N should never be null right?
	node, b := req.router(N)
	if !b {
		// we send with headers
	} else {
		// we send
	}

}

type routerRes struct {
	nodeD    string   // the node to send the rteq to
	nodeP    string   // the node to pull the req from. if set, we send with the special headers
	metaData []string // the metadat needed to send with a pull req
}

func (req *RequestService) router(R *hasherRes) *routerRes {
	// TODO: this function waits for the goroutine thats going to deliver us the list of healthy nodes

	if R.exists {
		// we iterate over R.Nodes and see if they are healthy from the list we got
		// select the best one based on least recently used/load
		// if the ones that have the block are conjested, find the best node to send it to and set with the node to pull form and metdata
	} else {
		// itarate over the available nodes and find the best one to choose from
	}

}

func (req *RequestService) tokenise() ([]uint32, error) {
	// i dont think the tokeniser depends on order
	// so should e use gorutines here for a very large prompt?
	// because goroutines can be overkill just because of the setup
	path := fmt.Sprintf("../configs/%s/tokenizer_config.json", req.model)

	tk, err := tokenizers.FromFile(path)
	if err != nil {
		return nil, err
	}
	defer tk.Close()
	encoded, _ := tk.Encode(req.input, true) // SGLang sets this as true, we want to replicate
	return encoded, nil

}

type hasherRes struct {
	exists bool
	nodes  []string
}

func (req *RequestService) checkCache(hash int64) ([]string, error) {
	// checks the route tables for this hash and returns the ips of the nodes if it exists
	// this is where we will need to check for both CPU and GPU tables
	// if it doesnt exist in CPU, girl its gone (since we also write in transactions)

	// oh i think we only need tp check if it exists in CPU or not. so yes, do that
	// // also move this to a router service along with other router related shit. this guy needs to be built before handler registrations
}

func (req *RequestService) hasher(tokens []uint32, priorDigest []byte) (*hasherRes, error) {
	// int(SHA256(parent_digest + tokens)[:16], 16) reinterpreted as int64
	// NOTE: pageSize defaults to 16, set is as that and make sure its the same on both server and proxy. maybe we can have an event
	// at startup that gives us this
	//
	// return false if we didnt find a cache hit

	if req.pageSize <= 0 {
		panic("pageSize must be > 0")
	}

	numPages := len(tokens) / req.pageSize
	hashes := make([]int64, 0, numPages)

	digest := priorDigest
	flag := false
	var N []string

	for p := 0; p < numPages; p++ {
		page := tokens[p*req.pageSize : (p+1)*req.pageSize]
		digest = hashOnePage(page, digest)

		// takes the first 8 bytes of a SHA-256 digest, interprets them as a big-endian uint64, and reinterprets as int64.
		bigInd := int64(binary.BigEndian.Uint64(digest[:8]))

		//search if this is a miss right here, if it is, we wont find anything further so stop
		// only continue if we get a cache hit

		nodes, err := req.checkCache(bigInd)
		if err != nil {
			return nil, fmt.Errorf("error checking for cache in table %s", err.Error)
		}
		if len(nodes) == 0 {
			// we didnt find any servers that have this guy for this digest
			if !flag {
				// we never even found one
				return &hasherRes{exists: false}, nil
			}
			// otherwise, we had found one, so we return the previosuly set nodes
			return &hasherRes{exists: true, nodes: N}, nil
		}

		// otherwise, cache hit, update vars
		flag = true
		N = nodes
		hashes = append(hashes, bigInd)
	}

	return &hasherRes{exists: true, nodes: N}, nil
}

func hashOnePage(tokens []uint32, priorDigest []byte) []byte {
	h := sha256.New()
	if len(priorDigest) == 32 {
		h.Write(priorDigest)
	}
	// Tokens as little-endian uint32 — matches the C++ raw-bytes path.
	buf := make([]byte, len(tokens)*4)
	for i, t := range tokens {
		binary.LittleEndian.PutUint32(buf[i*4:], t)
	}
	h.Write(buf)
	return h.Sum(nil)
}
