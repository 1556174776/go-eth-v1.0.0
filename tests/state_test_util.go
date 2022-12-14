// Copyright 2014 The go-ethereum Authors
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

package tests

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"strconv"

	"GO_Demo/go-ethereum/common"
	"GO_Demo/go-ethereum/core"
	"GO_Demo/go-ethereum/core/state"
	"GO_Demo/go-ethereum/core/vm"
	"GO_Demo/go-ethereum/crypto"
	"GO_Demo/go-ethereum/ethdb"
	"GO_Demo/go-ethereum/logger/glog"
)

func RunStateTestWithReader(r io.Reader, skipTests []string) error {
	tests := make(map[string]VmTest)
	if err := readJson(r, &tests); err != nil {
		return err
	}

	if err := runStateTests(tests, skipTests); err != nil {
		return err
	}

	return nil
}

func RunStateTest(p string, skipTests []string) error {
	tests := make(map[string]VmTest)
	if err := readJsonFile(p, &tests); err != nil {
		return err
	}

	if err := runStateTests(tests, skipTests); err != nil {
		return err
	}

	return nil

}

func runStateTests(tests map[string]VmTest, skipTests []string) error {
	skipTest := make(map[string]bool, len(skipTests))
	for _, name := range skipTests {
		skipTest[name] = true
	}

	for name, test := range tests {
		if skipTest[name] {
			glog.Infoln("Skipping state test", name)
			return nil
		}

		if err := runStateTest(test); err != nil {
			return fmt.Errorf("%s: %s\n", name, err.Error())
		}

		glog.Infoln("State test passed: ", name)
		//fmt.Println(string(statedb.Dump()))
	}
	return nil

}

func runStateTest(test VmTest) error {
	db, _ := ethdb.NewMemDatabase()
	statedb := state.New(common.Hash{}, db)
	for addr, account := range test.Pre {
		obj := StateObjectFromAccount(db, addr, account)
		statedb.SetStateObject(obj)
		for a, v := range account.Storage {
			obj.SetState(common.HexToHash(a), common.HexToHash(v))
		}
	}

	// XXX Yeah, yeah...
	env := make(map[string]string)
	env["currentCoinbase"] = test.Env.CurrentCoinbase
	env["currentDifficulty"] = test.Env.CurrentDifficulty
	env["currentGasLimit"] = test.Env.CurrentGasLimit
	env["currentNumber"] = test.Env.CurrentNumber
	env["previousHash"] = test.Env.PreviousHash
	if n, ok := test.Env.CurrentTimestamp.(float64); ok {
		env["currentTimestamp"] = strconv.Itoa(int(n))
	} else {
		env["currentTimestamp"] = test.Env.CurrentTimestamp.(string)
	}

	var (
		ret []byte
		// gas  *big.Int
		// err  error
		logs state.Logs
	)

	ret, logs, _, _ = RunState(statedb, env, test.Transaction)

	// // Compare expected  and actual return
	rexp := common.FromHex(test.Out)
	if bytes.Compare(rexp, ret) != 0 {
		return fmt.Errorf("return failed. Expected %x, got %x\n", rexp, ret)
	}

	// check post state
	for addr, account := range test.Post {
		obj := statedb.GetStateObject(common.HexToAddress(addr))
		if obj == nil {
			continue
		}

		if obj.Balance().Cmp(common.Big(account.Balance)) != 0 {
			return fmt.Errorf("(%x) balance failed. Expected %v, got %v => %v\n", obj.Address().Bytes()[:4], account.Balance, obj.Balance(), new(big.Int).Sub(common.Big(account.Balance), obj.Balance()))
		}

		if obj.Nonce() != common.String2Big(account.Nonce).Uint64() {
			return fmt.Errorf("(%x) nonce failed. Expected %v, got %v\n", obj.Address().Bytes()[:4], account.Nonce, obj.Nonce())
		}

		for addr, value := range account.Storage {
			v := obj.GetState(common.HexToHash(addr))
			vexp := common.HexToHash(value)

			if v != vexp {
				return fmt.Errorf("(%x: %s) storage failed. Expected %x, got %x (%v %v)\n", obj.Address().Bytes()[0:4], addr, vexp, v, vexp.Big(), v.Big())
			}
		}
	}

	statedb.Sync()
	if common.HexToHash(test.PostStateRoot) != statedb.Root() {
		return fmt.Errorf("Post state root error. Expected %s, got %x", test.PostStateRoot, statedb.Root())
	}

	// check logs
	if len(test.Logs) > 0 {
		if err := checkLogs(test.Logs, logs); err != nil {
			return err
		}
	}

	return nil
}

func RunState(statedb *state.StateDB, env, tx map[string]string) ([]byte, state.Logs, *big.Int, error) {
	var (
		data  = common.FromHex(tx["data"])
		gas   = common.Big(tx["gasLimit"])
		price = common.Big(tx["gasPrice"])
		value = common.Big(tx["value"])
		nonce = common.Big(tx["nonce"]).Uint64()
		caddr = common.HexToAddress(env["currentCoinbase"])
	)

	var to *common.Address
	if len(tx["to"]) > 2 {
		t := common.HexToAddress(tx["to"])
		to = &t
	}
	// Set pre compiled contracts
	vm.Precompiled = vm.PrecompiledContracts()

	snapshot := statedb.Copy()
	coinbase := statedb.GetOrNewStateObject(caddr)
	coinbase.SetGasLimit(common.Big(env["currentGasLimit"]))

	key, _ := hex.DecodeString(tx["secretKey"])
	addr := crypto.PubkeyToAddress(crypto.ToECDSA(key).PublicKey)
	message := NewMessage(addr, to, data, value, gas, price, nonce)
	vmenv := NewEnvFromMap(statedb, env, tx)
	vmenv.origin = addr
	ret, _, err := core.ApplyMessage(vmenv, message, coinbase)
	if core.IsNonceErr(err) || core.IsInvalidTxErr(err) || state.IsGasLimitErr(err) {
		statedb.Set(snapshot)
	}
	statedb.SyncObjects()

	return ret, vmenv.state.Logs(), vmenv.Gas, err
}
