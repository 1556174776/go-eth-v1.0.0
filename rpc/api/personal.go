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
	"time"

	"GO_Demo/go-ethereum/common"
	"GO_Demo/go-ethereum/eth"
	"GO_Demo/go-ethereum/rpc/codec"
	"GO_Demo/go-ethereum/rpc/shared"
	"GO_Demo/go-ethereum/xeth"
)

const (
	PersonalApiVersion = "1.0"
)

var (
	// mapping between methods and handlers
	personalMapping = map[string]personalhandler{
		"personal_listAccounts":  (*personalApi).ListAccounts,
		"personal_newAccount":    (*personalApi).NewAccount,
		"personal_deleteAccount": (*personalApi).DeleteAccount,
		"personal_unlockAccount": (*personalApi).UnlockAccount,
	}
)

// net callback handler
type personalhandler func(*personalApi, *shared.Request) (interface{}, error)

// net api provider
type personalApi struct {
	xeth     *xeth.XEth
	ethereum *eth.Ethereum
	methods  map[string]personalhandler
	codec    codec.ApiCoder
}

// create a new net api instance
func NewPersonalApi(xeth *xeth.XEth, eth *eth.Ethereum, coder codec.Codec) *personalApi {
	return &personalApi{
		xeth:     xeth,
		ethereum: eth,
		methods:  personalMapping,
		codec:    coder.New(nil),
	}
}

// collection with supported methods
func (self *personalApi) Methods() []string {
	methods := make([]string, len(self.methods))
	i := 0
	for k := range self.methods {
		methods[i] = k
		i++
	}
	return methods
}

// Execute given request
func (self *personalApi) Execute(req *shared.Request) (interface{}, error) {
	if callback, ok := self.methods[req.Method]; ok {
		return callback(self, req)
	}

	return nil, shared.NewNotImplementedError(req.Method)
}

func (self *personalApi) Name() string {
	return shared.PersonalApiName
}

func (self *personalApi) ApiVersion() string {
	return PersonalApiVersion
}

func (self *personalApi) ListAccounts(req *shared.Request) (interface{}, error) {
	return self.xeth.Accounts(), nil
}

func (self *personalApi) NewAccount(req *shared.Request) (interface{}, error) {
	args := new(NewAccountArgs)
	if err := self.codec.Decode(req.Params, &args); err != nil {
		return nil, shared.NewDecodeParamError(err.Error())
	}

	am := self.ethereum.AccountManager()
	acc, err := am.NewAccount(args.Passphrase)
	return acc.Address.Hex(), err
}

func (self *personalApi) DeleteAccount(req *shared.Request) (interface{}, error) {
	args := new(DeleteAccountArgs)
	if err := self.codec.Decode(req.Params, &args); err != nil {
		return nil, shared.NewDecodeParamError(err.Error())
	}

	addr := common.HexToAddress(args.Address)
	am := self.ethereum.AccountManager()
	if err := am.DeleteAccount(addr, args.Passphrase); err == nil {
		return true, nil
	} else {
		return false, err
	}
}

func (self *personalApi) UnlockAccount(req *shared.Request) (interface{}, error) {
	args := new(UnlockAccountArgs)
	if err := self.codec.Decode(req.Params, &args); err != nil {
		return nil, shared.NewDecodeParamError(err.Error())
	}

	var err error
	am := self.ethereum.AccountManager()
	addr := common.HexToAddress(args.Address)

	if args.Duration == -1 {
		err = am.Unlock(addr, args.Passphrase)
	} else {
		err = am.TimedUnlock(addr, args.Passphrase, time.Duration(args.Duration)*time.Second)
	}

	if err == nil {
		return true, nil
	}
	return false, err
}
