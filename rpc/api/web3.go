// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package api

import (
	"GO_Demo/go-ethereum/common"
	"GO_Demo/go-ethereum/crypto"
	"GO_Demo/go-ethereum/rpc/codec"
	"GO_Demo/go-ethereum/rpc/shared"
	"GO_Demo/go-ethereum/xeth"
)

const (
	Web3ApiVersion = "1.0"
)

var (
	// mapping between methods and handlers
	Web3Mapping = map[string]web3handler{
		"web3_sha3":          (*web3Api).Sha3,
		"web3_clientVersion": (*web3Api).ClientVersion,
	}
)

// web3 callback handler
type web3handler func(*web3Api, *shared.Request) (interface{}, error)

// web3 api provider
type web3Api struct {
	xeth    *xeth.XEth
	methods map[string]web3handler
	codec   codec.ApiCoder
}

// create a new web3 api instance
func NewWeb3Api(xeth *xeth.XEth, coder codec.Codec) *web3Api {
	return &web3Api{
		xeth:    xeth,
		methods: Web3Mapping,
		codec:   coder.New(nil),
	}
}

// collection with supported methods
func (self *web3Api) Methods() []string {
	methods := make([]string, len(self.methods))
	i := 0
	for k := range self.methods {
		methods[i] = k
		i++
	}
	return methods
}

// Execute given request
func (self *web3Api) Execute(req *shared.Request) (interface{}, error) {
	if callback, ok := self.methods[req.Method]; ok {
		return callback(self, req)
	}

	return nil, &shared.NotImplementedError{req.Method}
}

func (self *web3Api) Name() string {
	return shared.Web3ApiName
}

func (self *web3Api) ApiVersion() string {
	return Web3ApiVersion
}

// Calculates the sha3 over req.Params.Data
func (self *web3Api) Sha3(req *shared.Request) (interface{}, error) {
	args := new(Sha3Args)
	if err := self.codec.Decode(req.Params, &args); err != nil {
		return nil, err
	}

	return common.ToHex(crypto.Sha3(common.FromHex(args.Data))), nil
}

// returns the xeth client vrsion
func (self *web3Api) ClientVersion(req *shared.Request) (interface{}, error) {
	return self.xeth.ClientVersion(), nil
}
