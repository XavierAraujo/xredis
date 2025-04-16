package main

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPingCommand(t *testing.T) {
	xredis := NewXRedis()

	pingCommand := "*1\r\n$4\r\nPING\r\n"
	rsp := xredis.handleRequest([]byte(pingCommand))
	rspData, ok := rsp.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"PONG"}, rspData)
}

func TestEchoCommand(t *testing.T) {
	xredis := NewXRedis()

	echoCommand := "*2\r\n$4\r\nECHO\r\n$16\r\necho-hello-world"
	rsp := xredis.handleRequest([]byte(echoCommand))
	rspData, ok := rsp.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"echo-hello-world"}, rspData)
}

func TestSetAndGetKeyCommand(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*3\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n"
	setRsp := xredis.handleRequest([]byte(setCommand))
	setRspData, ok := setRsp.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"OK"}, setRspData)

	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp := xredis.handleRequest([]byte(getCommand))
	getRspData, ok := getRsp.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"bli"}, getRspData)
}

func TestGetEmptyKeyCommand(t *testing.T) {
	xredis := NewXRedis()

	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp := xredis.handleRequest([]byte(getCommand))
	_, ok := getRsp.(RespNil)
	assert.True(t, ok)
}

func TestDeleteKeyCommand(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*3\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n"
	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	deleteCommand := "*2\r\n$3\r\nDEL\r\n$3\r\nbla\r\n"
	xredis.handleRequest([]byte(setCommand))
	getRsp1 := xredis.handleRequest([]byte(getCommand))
	getRspData1, _ := getRsp1.(RespString)
	assert.Equal(t, RespString{"bli"}, getRspData1)

	deleteRsp1 := xredis.handleRequest([]byte(deleteCommand))
	deleteRspData1, ok := deleteRsp1.(RespInt)
	assert.True(t, ok)
	assert.Equal(t, RespInt{1}, deleteRspData1)
	getRsp1 = xredis.handleRequest([]byte(getCommand))
	_, ok = getRsp1.(RespNil)
	assert.True(t, ok)

	deleteRsp2 := xredis.handleRequest([]byte(deleteCommand))
	deleteRspData2, ok := deleteRsp2.(RespInt)
	assert.True(t, ok)
	assert.Equal(t, RespInt{0}, deleteRspData2)
}

func TestExistsKeyCommand(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*3\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n"
	existsCommand := "*2\r\n$6\r\nEXISTS\r\n$3\r\nbla\r\n"
	existsRsp1 := xredis.handleRequest([]byte(existsCommand))
	existsRspData1, _ := existsRsp1.(RespInt)
	assert.Equal(t, RespInt{0}, existsRspData1)

	xredis.handleRequest([]byte(setCommand))
	existsRsp2 := xredis.handleRequest([]byte(existsCommand))
	existsRspData2, _ := existsRsp2.(RespInt)
	assert.Equal(t, RespInt{1}, existsRspData2)
}

func TestSetAndGetKeyCommandWithExTimeout(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*5\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n$2\r\nEX\r\n$1\r\n1\r\n"
	setRsp := xredis.handleRequest([]byte(setCommand))
	setRspData, ok := setRsp.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"OK"}, setRspData)

	time.Sleep(995 * time.Millisecond)
	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp1 := xredis.handleRequest([]byte(getCommand))
	getRspData1, ok := getRsp1.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"bli"}, getRspData1)

	time.Sleep(10 * time.Millisecond)
	getRsp2 := xredis.handleRequest([]byte(getCommand))
	_, ok = getRsp2.(RespNil)
	assert.True(t, ok)
}

func TestSetAndGetKeyCommandWithPxTimeout(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*5\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n$2\r\nPX\r\n$3\r\n100\r\n"
	setRsp := xredis.handleRequest([]byte(setCommand))
	setRspData, ok := setRsp.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"OK"}, setRspData)

	time.Sleep(95 * time.Millisecond)
	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp1 := xredis.handleRequest([]byte(getCommand))
	getRspData1, ok := getRsp1.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"bli"}, getRspData1)

	time.Sleep(10 * time.Millisecond)
	getRsp2 := xredis.handleRequest([]byte(getCommand))
	_, ok = getRsp2.(RespNil)
	assert.True(t, ok)
}

func TestSetAndGetKeyCommandWithExatTimeout(t *testing.T) {
	xredis := NewXRedis()

	now := time.Now().UnixMilli()
	expireTimestamp := (now / 1000) + int64(1)
	expireTimestampStr := strconv.FormatInt(expireTimestamp, 10)
	expireTimestampStrLen := strconv.Itoa(len(expireTimestampStr))
	millisToExpireTimestamp := expireTimestamp*1000 - now
	setCommand := "*5\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n$4\r\nEXAT\r\n$" + expireTimestampStrLen + "\r\n" + expireTimestampStr + "\r\n"
	setRsp := xredis.handleRequest([]byte(setCommand))
	setRspData, ok := setRsp.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"OK"}, setRspData)

	time.Sleep(time.Duration((millisToExpireTimestamp - 5)) * time.Millisecond)
	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp1 := xredis.handleRequest([]byte(getCommand))
	getRspData1, ok := getRsp1.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"bli"}, getRspData1)

	time.Sleep(10 * time.Millisecond)
	getRsp2 := xredis.handleRequest([]byte(getCommand))
	_, ok = getRsp2.(RespNil)
	assert.True(t, ok)
}

func TestSetAndGetKeyCommandWithPxatTimeout(t *testing.T) {
	xredis := NewXRedis()

	expireTimestampStr := strconv.FormatInt(time.Now().UnixMilli()+int64(1000), 10)
	expireTimestampStrLen := strconv.Itoa(len(expireTimestampStr))
	setCommand := "*5\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n$4\r\nPXAT\r\n$" + expireTimestampStrLen + "\r\n" + expireTimestampStr + "\r\n"
	setRsp := xredis.handleRequest([]byte(setCommand))
	setRspData, ok := setRsp.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"OK"}, setRspData)

	time.Sleep(995 * time.Millisecond)
	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp1 := xredis.handleRequest([]byte(getCommand))
	getRspData1, ok := getRsp1.(RespString)
	assert.True(t, ok)
	assert.Equal(t, RespString{"bli"}, getRspData1)

	time.Sleep(10 * time.Millisecond)
	getRsp2 := xredis.handleRequest([]byte(getCommand))
	_, ok = getRsp2.(RespNil)
	assert.True(t, ok)
}

func TestBasicIncrement(t *testing.T) {
	xredis := NewXRedis()

	incrCommand := "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"
	incrRsp1 := xredis.handleRequest([]byte(incrCommand))
	incrRspData1, ok := incrRsp1.(RespInt)
	assert.True(t, ok)
	assert.Equal(t, RespInt{1}, incrRspData1)

	incrRsp2 := xredis.handleRequest([]byte(incrCommand))
	incrRspData2, ok := incrRsp2.(RespInt)
	assert.True(t, ok)
	assert.Equal(t, RespInt{2}, incrRspData2)
}

func TestBasicDecrement(t *testing.T) {
	xredis := NewXRedis()

	decrCommand := "*2\r\n$4\r\nDECR\r\n$7\r\ncounter\r\n"
	decrRsp1 := xredis.handleRequest([]byte(decrCommand))
	decrRspData1, ok := decrRsp1.(RespInt)
	assert.True(t, ok)
	assert.Equal(t, RespInt{-1}, decrRspData1)

	decrRsp2 := xredis.handleRequest([]byte(decrCommand))
	decrRspData2, ok := decrRsp2.(RespInt)
	assert.True(t, ok)
	assert.Equal(t, RespInt{-2}, decrRspData2)
}

func TestIncrementNonNumericKey(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$4\r\ntext\r\n"
	_ = xredis.handleRequest([]byte(setCommand))
	incrCommand := "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"
	incrRsp := xredis.handleRequest([]byte(incrCommand))
	_, ok := incrRsp.(RespError)
	assert.True(t, ok)
}

func TestDecrementNonNumericKey(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$4\r\ntext\r\n"
	_ = xredis.handleRequest([]byte(setCommand))
	decrCommand := "*2\r\n$4\r\nDECR\r\n$7\r\ncounter\r\n"
	decrRsp := xredis.handleRequest([]byte(decrCommand))
	_, ok := decrRsp.(RespError)
	assert.True(t, ok)
}

func TestBasicLPush(t *testing.T) {
	xredis := NewXRedis()

	lpushCommand1 := "*3\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$4\r\nxxxx\r\n"
	lpushCommand2 := "*3\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$4\r\nyyyy\r\n"
	lpushCommand3 := "*3\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$4\r\nzzzz\r\n"
	_ = xredis.handleRequest([]byte(lpushCommand1))
	_ = xredis.handleRequest([]byte(lpushCommand2))
	_ = xredis.handleRequest([]byte(lpushCommand3))

	getCommand := "*2\r\n$3\r\nGET\r\n$4\r\nlist\r\n"
	getRsp := xredis.handleRequest([]byte(getCommand))
	list := getRsp.(RespArray)
	elem1 := list.elements[0].(RespString)
	elem2 := list.elements[1].(RespString)
	elem3 := list.elements[2].(RespString)
	assert.Equal(t, "zzzz", elem1.str)
	assert.Equal(t, "yyyy", elem2.str)
	assert.Equal(t, "xxxx", elem3.str)
}

func TestBasicRPush(t *testing.T) {
	xredis := NewXRedis()

	rpushCommand1 := "*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$4\r\nxxxx\r\n"
	rpushCommand2 := "*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$4\r\nyyyy\r\n"
	rpushCommand3 := "*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$4\r\nzzzz\r\n"
	_ = xredis.handleRequest([]byte(rpushCommand1))
	_ = xredis.handleRequest([]byte(rpushCommand2))
	_ = xredis.handleRequest([]byte(rpushCommand3))

	getCommand := "*2\r\n$3\r\nGET\r\n$4\r\nlist\r\n"
	getRsp := xredis.handleRequest([]byte(getCommand))
	list := getRsp.(RespArray)
	elem1 := list.elements[0].(RespString)
	elem2 := list.elements[1].(RespString)
	elem3 := list.elements[2].(RespString)
	assert.Equal(t, "xxxx", elem1.str)
	assert.Equal(t, "yyyy", elem2.str)
	assert.Equal(t, "zzzz", elem3.str)
}
