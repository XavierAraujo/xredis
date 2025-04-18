package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSetAndGetKeyCommand(t *testing.T) {
	xredis := NewXRedis()

	xredis.Set("bla", RespString{"bli"})
	rsp := xredis.Get("bla")
	assert.Equal(t, RespString{"bli"}, rsp)
}

func TestGetEmptyKeyCommand(t *testing.T) {
	xredis := NewXRedis()

	rsp := xredis.Get("bla")
	_, ok := rsp.(RespNil)
	assert.True(t, ok)
}

func TestDeleteKeyCommand(t *testing.T) {
	xredis := NewXRedis()

	xredis.Set("bla", RespString{"bli"})
	xredis.Delete("bla")
	rsp := xredis.Get("bla")
	_, ok := rsp.(RespNil)
	assert.True(t, ok)
}

func TestExistsKeyCommand(t *testing.T) {
	xredis := NewXRedis()

	exists := xredis.Exists("bla")
	assert.False(t, exists)

	xredis.Set("bla", RespString{"bli"})
	exists = xredis.Exists("bla")
	assert.True(t, exists)
}

func TestSetAndGetKeyCommandWithExpirationTime(t *testing.T) {
	xredis := NewXRedis()

	xredis.SetWithExpiration("bla", RespString{"bli"}, time.Now().Add(100*time.Millisecond))

	time.Sleep(95 * time.Millisecond)
	rsp := xredis.Get("bla")
	assert.Equal(t, RespString{"bli"}, rsp)

	time.Sleep(10 * time.Millisecond)
	rsp = xredis.Get("bla")
	_, ok := rsp.(RespNil)
	assert.True(t, ok)
}

func TestBasicIncrement(t *testing.T) {
	xredis := NewXRedis()

	incrRsp, _ := xredis.Increment("counter")
	assert.Equal(t, RespString{"1"}, incrRsp)
	incrRsp, _ = xredis.Increment("counter")
	assert.Equal(t, RespString{"2"}, incrRsp)
}

func TestBasicDecrement(t *testing.T) {
	xredis := NewXRedis()

	incrRsp, _ := xredis.Decrement("counter")
	assert.Equal(t, RespString{"-1"}, incrRsp)
	incrRsp, _ = xredis.Decrement("counter")
	assert.Equal(t, RespString{"-2"}, incrRsp)
}

func TestIncrementNonNumericKey(t *testing.T) {
	xredis := NewXRedis()

	xredis.Set("counter", RespString{"non-numeric-value"})
	_, err := xredis.Increment("counter")
	assert.NotNil(t, err)
}

func TestDecrementNonNumericKey(t *testing.T) {
	xredis := NewXRedis()

	xredis.Set("counter", RespString{"non-numeric-value"})
	_, err := xredis.Decrement("counter")
	assert.NotNil(t, err)
}

func TestBasicLPush(t *testing.T) {
	xredis := NewXRedis()

	err := xredis.LPush("list", RespString{"xxxx"})
	assert.Nil(t, err)
	err = xredis.LPush("list", RespString{"yyyy"})
	assert.Nil(t, err)
	err = xredis.LPush("list", RespString{"zzzz"})
	assert.Nil(t, err)

	getRsp := xredis.Get("list")
	list := getRsp.(RespArray)
	elem1 := list.Elements[0].(RespString)
	elem2 := list.Elements[1].(RespString)
	elem3 := list.Elements[2].(RespString)
	assert.Equal(t, "zzzz", elem1.Str)
	assert.Equal(t, "yyyy", elem2.Str)
	assert.Equal(t, "xxxx", elem3.Str)
}

func TestLPushOnNonListElement(t *testing.T) {
	xredis := NewXRedis()

	xredis.Set("list", RespString{"non-list-value"})

	err := xredis.LPush("list", RespString{"xxxx"})
	assert.NotNil(t, err)
}

func TestBasicRPush(t *testing.T) {
	xredis := NewXRedis()

	err := xredis.RPush("list", RespString{"xxxx"})
	assert.Nil(t, err)
	err = xredis.RPush("list", RespString{"yyyy"})
	assert.Nil(t, err)
	err = xredis.RPush("list", RespString{"zzzz"})
	assert.Nil(t, err)

	getRsp := xredis.Get("list")
	list := getRsp.(RespArray)
	elem1 := list.Elements[0].(RespString)
	elem2 := list.Elements[1].(RespString)
	elem3 := list.Elements[2].(RespString)
	assert.Equal(t, "xxxx", elem1.Str)
	assert.Equal(t, "yyyy", elem2.Str)
	assert.Equal(t, "zzzz", elem3.Str)
}

func TestRPushOnNonListElement(t *testing.T) {
	xredis := NewXRedis()

	xredis.Set("list", RespString{"non-list-value"})

	err := xredis.RPush("list", RespString{"xxxx"})
	assert.NotNil(t, err)
}

func TestSaveAndLoad(t *testing.T) {
	xredis1 := NewXRedis()

	xredis1.Set("key1", RespString{"xxxx"})
	xredis1.Set("key2", RespString{"1"})
	xredis1.Set("key3", RespArray{[]RespDataType{RespString{"xxxx"}, RespString{"2"}}})
	data := xredis1.Serialize()

	xredis2 := NewXRedis()
	xredis2.Load(data)
	assert.Equal(t, RespString{"xxxx"}, xredis2.Get("key1"))
	assert.Equal(t, RespString{"1"}, xredis2.Get("key2"))
	assert.Equal(t, RespArray{[]RespDataType{RespString{"xxxx"}, RespString{"2"}}}, xredis2.Get("key3"))
}
