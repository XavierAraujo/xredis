package main

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInvalidCommand(t *testing.T) {
	xredis := NewXRedis()

	invalidCommand := "*1\r\n$18\r\nNONEXISTENTCOMMAND\r\n"
	rsp := handleRequest(xredis, []byte(invalidCommand))
	assert.Equal(t, "-ERR INVALID-COMMAND\r\n", string(rsp))
}

func TestInvalidSerializedRequest(t *testing.T) {
	xredis := NewXRedis()

	invalidCommand := "xxxx"
	rsp := handleRequest(xredis, []byte(invalidCommand))
	assert.Equal(t, "-ERR FAILED-DESERIALIZING\r\n", string(rsp))
}
func TestNonArrayRequest(t *testing.T) {
	xredis := NewXRedis()

	invalidCommand := ":1\r\n"
	rsp := handleRequest(xredis, []byte(invalidCommand))
	assert.Equal(t, "-ERR UNEXPECTED-ARGUMENT-TYPE\r\n", string(rsp))
}

func TestPingRequest(t *testing.T) {
	xredis := NewXRedis()

	pingCommand := "*1\r\n$4\r\nPING\r\n"
	rsp := handleRequest(xredis, []byte(pingCommand))
	assert.Equal(t, "$4\r\nPONG\r\n", string(rsp))
}

func TestEchoRequest(t *testing.T) {
	xredis := NewXRedis()

	echoCommand := "*2\r\n$4\r\nECHO\r\n$16\r\necho-hello-world\r\n"
	rsp := handleRequest(xredis, []byte(echoCommand))
	assert.Equal(t, "$16\r\necho-hello-world\r\n", string(rsp))
}

func TestSetAndGetRequests(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*3\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n"
	setRsp := handleRequest(xredis, []byte(setCommand))
	assert.Equal(t, "$2\r\nOK\r\n", string(setRsp))

	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp := handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "$3\r\nbli\r\n", string(getRsp))
}

func TestDeleteRequest(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*3\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n"
	_ = handleRequest(xredis, []byte(setCommand))
	deleteCommand := "*2\r\n$3\r\nDEL\r\n$3\r\nbla\r\n"
	deleteRsp := handleRequest(xredis, []byte(deleteCommand))
	assert.Equal(t, ":1\r\n", string(deleteRsp))
}

func TestExistsRequest(t *testing.T) {
	xredis := NewXRedis()

	existsCommand := "*2\r\n$6\r\nEXISTS\r\n$3\r\nbla\r\n"
	existsRsp := handleRequest(xredis, []byte(existsCommand))
	assert.Equal(t, ":0\r\n", string(existsRsp))

	setCommand := "*3\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n"
	_ = handleRequest(xredis, []byte(setCommand))
	existsCommand = "*2\r\n$6\r\nEXISTS\r\n$3\r\nbla\r\n"
	existsRsp = handleRequest(xredis, []byte(existsCommand))
	assert.Equal(t, ":1\r\n", string(existsRsp))
}

func TestSetAndGetRequestWithExpirationModeEx(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*5\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n$2\r\nEX\r\n$1\r\n1\r\n"
	_ = handleRequest(xredis, []byte(setCommand))

	time.Sleep(995 * time.Millisecond)
	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp := handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "$3\r\nbli\r\n", string(getRsp))

	time.Sleep(10 * time.Millisecond)
	getCommand = "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp = handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "$-1\r\n", string(getRsp))
}

func TestSetAndGetRequestWithExpirationModePx(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*5\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n$2\r\nPX\r\n$3\r\n100\r\n"
	_ = handleRequest(xredis, []byte(setCommand))

	time.Sleep(95 * time.Millisecond)
	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp := handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "$3\r\nbli\r\n", string(getRsp))

	time.Sleep(10 * time.Millisecond)
	getCommand = "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp = handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "$-1\r\n", string(getRsp))
}

func TestSetAndGetRequestWithExpirationModeExat(t *testing.T) {
	xredis := NewXRedis()

	now := time.Now().UnixMilli()
	expireTimestamp := (now / 1000) + int64(1)
	expireTimestampStr := strconv.FormatInt(expireTimestamp, 10)
	expireTimestampStrLen := strconv.Itoa(len(expireTimestampStr))
	millisToExpireTimestamp := expireTimestamp*1000 - now
	setCommand := "*5\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n$4\r\nEXAT\r\n$" + expireTimestampStrLen + "\r\n" + expireTimestampStr + "\r\n"
	_ = handleRequest(xredis, []byte(setCommand))

	time.Sleep(time.Duration((millisToExpireTimestamp - 5)) * time.Millisecond)
	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp := handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "$3\r\nbli\r\n", string(getRsp))

	time.Sleep(10 * time.Millisecond)
	getCommand = "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp = handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "$-1\r\n", string(getRsp))
}

func TestSetAndGetRequestWithExpirationModePxat(t *testing.T) {
	xredis := NewXRedis()

	expireTimestampStr := strconv.FormatInt(time.Now().UnixMilli()+int64(1000), 10)
	expireTimestampStrLen := strconv.Itoa(len(expireTimestampStr))
	setCommand := "*5\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n$4\r\nPXAT\r\n$" + expireTimestampStrLen + "\r\n" + expireTimestampStr + "\r\n"
	_ = handleRequest(xredis, []byte(setCommand))

	time.Sleep(995 * time.Millisecond)
	getCommand := "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp := handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "$3\r\nbli\r\n", string(getRsp))

	time.Sleep(10 * time.Millisecond)
	getCommand = "*2\r\n$3\r\nGET\r\n$3\r\nbla\r\n"
	getRsp = handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "$-1\r\n", string(getRsp))
}

func TestSetAndGetRequestWithInvalidExpirationMode(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*5\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n$2\r\nXX\r\n$4\r\n1000\r\n"
	rsp := handleRequest(xredis, []byte(setCommand))
	assert.Equal(t, "-ERR UNRECOGNIZED-TIMEOUT-MODE\r\n", string(rsp))
}

func TestSetAndGetRequestWithInvalidExpirationValue(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*5\r\n$3\r\nSET\r\n$3\r\nbla\r\n$3\r\nbli\r\n$2\r\nPX\r\n$4\r\nxxxx\r\n"
	rsp := handleRequest(xredis, []byte(setCommand))
	assert.Equal(t, "-ERR INVALID-TIMEOUT-VALUE\r\n", string(rsp))
}

func TestIncrementRequest(t *testing.T) {
	xredis := NewXRedis()

	incrCommand := "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"
	incrRsp := handleRequest(xredis, []byte(incrCommand))
	assert.Equal(t, "$1\r\n1\r\n", string(incrRsp))

	incrRsp = handleRequest(xredis, []byte(incrCommand))
	assert.Equal(t, "$1\r\n2\r\n", string(incrRsp))
}

func TestDecrementRequest(t *testing.T) {
	xredis := NewXRedis()

	decrCommand := "*2\r\n$4\r\nDECR\r\n$7\r\ncounter\r\n"
	decrRsp := handleRequest(xredis, []byte(decrCommand))
	assert.Equal(t, "$2\r\n-1\r\n", string(decrRsp))

	decrRsp = handleRequest(xredis, []byte(decrCommand))
	assert.Equal(t, "$2\r\n-2\r\n", string(decrRsp))
}

func TestIncrementNonNumericKeyRequest(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$4\r\ntext\r\n"
	_ = handleRequest(xredis, []byte(setCommand))
	incrCommand := "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"
	incrRsp := handleRequest(xredis, []byte(incrCommand))
	assert.Equal(t, "-ERR VALUE-NOT-NUMERIC-OR-MAX-REACHED\r\n", string(incrRsp))
}

func TestDecrementNonNumericKeyRequest(t *testing.T) {
	xredis := NewXRedis()

	setCommand := "*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$4\r\ntext\r\n"
	_ = handleRequest(xredis, []byte(setCommand))
	decrCommand := "*2\r\n$4\r\nDECR\r\n$7\r\ncounter\r\n"
	decrRsp := handleRequest(xredis, []byte(decrCommand))
	assert.Equal(t, "-ERR VALUE-NOT-NUMERIC-OR-MAX-REACHED\r\n", string(decrRsp))
}

func TestLPushRequest(t *testing.T) {
	xredis := NewXRedis()

	lpushCommand1 := "*3\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$4\r\nxxxx\r\n"
	lpushCommand2 := "*3\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$4\r\nyyyy\r\n"
	lpushCommand3 := "*3\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$4\r\nzzzz\r\n"
	_ = handleRequest(xredis, []byte(lpushCommand1))
	_ = handleRequest(xredis, []byte(lpushCommand2))
	_ = handleRequest(xredis, []byte(lpushCommand3))

	getCommand := "*2\r\n$3\r\nGET\r\n$4\r\nlist\r\n"
	getRsp := handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "*3\r\n$4\r\nzzzz\r\n$4\r\nyyyy\r\n$4\r\nxxxx\r\n", string(getRsp))
}

func TestRPushRequest(t *testing.T) {
	xredis := NewXRedis()

	rpushCommand1 := "*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$4\r\nxxxx\r\n"
	rpushCommand2 := "*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$4\r\nyyyy\r\n"
	rpushCommand3 := "*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$4\r\nzzzz\r\n"
	_ = handleRequest(xredis, []byte(rpushCommand1))
	_ = handleRequest(xredis, []byte(rpushCommand2))
	_ = handleRequest(xredis, []byte(rpushCommand3))

	getCommand := "*2\r\n$3\r\nGET\r\n$4\r\nlist\r\n"
	getRsp := handleRequest(xredis, []byte(getCommand))
	assert.Equal(t, "*3\r\n$4\r\nxxxx\r\n$4\r\nyyyy\r\n$4\r\nzzzz\r\n", string(getRsp))
}
