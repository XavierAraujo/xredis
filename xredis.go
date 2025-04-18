package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"log"
	"math"
	"strconv"
	"time"
)

const NON_EXPIRATION_TIME = -1

type XRedisValue struct {
	Element                   RespDataType
	ExpirationTimestampMillis int64
}

type XRedis struct {
	cache    map[string]XRedisValue
	commands chan Command
}

func NewXRedis() *XRedis {
	xredis := XRedis{make(map[string]XRedisValue), make(chan Command)}
	xredis.registerRequiredTypesForSerialization()
	go func() {
		for command := range xredis.commands {
			switch cmd := command.(type) {
			case SetCommand:
				xredis.handleSetCommand(cmd)
			case GetCommand:
				xredis.handleGetCommand(cmd)
			case ExistsCommand:
				xredis.handleExistsCommand(cmd)
			case DeleteCommand:
				xredis.handleDeleteCommand(cmd)
			case IncrementCommand:
				xredis.handleIncrementCommand(cmd)
			case DecrementCommand:
				xredis.handleDecrementCommand(cmd)
			case LPushCommand:
				xredis.handleLPushCommand(cmd)
			case RPushCommand:
				xredis.handleRPushCommand(cmd)
			case SaveCommand:
				xredis.handleSaveCommand(cmd)
			case LoadCommand:
				xredis.handleLoadCommand(cmd)
			}
		}
	}()
	return &xredis
}

func (xredis *XRedis) registerRequiredTypesForSerialization() {
	gob.Register(XRedisValue{})
	gob.Register(RespString{})
	gob.Register(RespArray{})
}

func (xredis *XRedis) Set(key string, value RespDataType) {
	doneChan := make(chan struct{})
	xredis.commands <- SetCommand{key, value, NON_EXPIRATION_TIME, doneChan}
	<-doneChan // Wait for completion
}

func (xredis *XRedis) SetWithExpiration(key string, value RespDataType, expirationTime time.Time) {
	expirationTimeTimestamp := expirationTime.UnixMilli()
	doneChan := make(chan struct{})
	xredis.commands <- SetCommand{key, value, expirationTimeTimestamp, doneChan}
	<-doneChan // Wait for completion
}

func (xredis *XRedis) Get(key string) RespDataType {
	rspChan := make(chan RespDataType)
	xredis.commands <- GetCommand{key, rspChan}
	return <-rspChan
}

func (xredis *XRedis) Exists(key string) bool {
	rspChan := make(chan bool)
	xredis.commands <- ExistsCommand{key, rspChan}
	return <-rspChan
}

func (xredis *XRedis) Delete(key string) bool {
	rspChan := make(chan bool)
	xredis.commands <- DeleteCommand{key, rspChan}
	return <-rspChan
}

func (xredis *XRedis) Increment(key string) (RespString, error) {
	rspChan := make(chan RespString)
	errorChan := make(chan error)
	xredis.commands <- IncrementCommand{key, rspChan, errorChan}
	return <-rspChan, <-errorChan
}

func (xredis *XRedis) Decrement(key string) (RespString, error) {
	rspChan := make(chan RespString)
	errorChan := make(chan error)
	xredis.commands <- DecrementCommand{key, rspChan, errorChan}
	return <-rspChan, <-errorChan
}

func (xredis *XRedis) LPush(key string, value RespDataType) error {
	errorChan := make(chan error)
	xredis.commands <- LPushCommand{key, value, errorChan}
	return <-errorChan
}

func (xredis *XRedis) RPush(key string, value RespDataType) error {
	errorChan := make(chan error)
	xredis.commands <- RPushCommand{key, value, errorChan}
	return <-errorChan
}

func (xredis *XRedis) Serialize() []byte {
	rspChan := make(chan []byte)
	xredis.commands <- SaveCommand{rspChan}
	return <-rspChan
}

func (xredis *XRedis) Load(data []byte) error {
	errorChan := make(chan error)
	xredis.commands <- LoadCommand{data, errorChan}
	return <-errorChan
}

func (xredis *XRedis) handleSetCommand(cmd SetCommand) {
	xredis.cache[cmd.key] = XRedisValue{cmd.value, cmd.expirationTimestamp}
	close(cmd.done)
}

func (xredis *XRedis) handleGetCommand(cmd GetCommand) {
	var rsp RespDataType = RespNil{}
	value, exists := xredis.getAndInvalidateIfExpired(cmd.key)
	if exists {
		rsp = value.Element
	}
	cmd.rspChannel <- rsp
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleExistsCommand(cmd ExistsCommand) {
	_, exists := xredis.getAndInvalidateIfExpired(cmd.key)
	cmd.rspChannel <- exists
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleDeleteCommand(cmd DeleteCommand) {
	_, existed := xredis.getAndInvalidateIfExpired(cmd.key)
	delete(xredis.cache, cmd.key)
	cmd.rspChannel <- existed
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleIncrementCommand(cmd IncrementCommand) {
	_, exists := xredis.getAndInvalidateIfExpired(cmd.key)
	if !exists {
		xredis.cache[cmd.key] = XRedisValue{RespString{"0"}, NON_EXPIRATION_TIME}
	}
	respInt, ok := xredis.tryGetAsRespInt(cmd.key)
	if !ok || respInt.Value == math.MaxInt64 {
		cmd.rspChannel <- RespString{}
		cmd.errorChannel <- errors.New(REQUEST_ERROR_VALUE_NOT_NUMERIC_OR_MAX_REACHED)
		close(cmd.rspChannel)
		close(cmd.errorChannel)
		return
	}

	newValue := RespString{strconv.FormatInt(respInt.Value+1, 10)}
	xredis.cache[cmd.key] = XRedisValue{newValue, xredis.cache[cmd.key].ExpirationTimestampMillis}
	cmd.rspChannel <- newValue
	cmd.errorChannel <- nil
	close(cmd.rspChannel)
	close(cmd.errorChannel)
}

func (xredis *XRedis) handleDecrementCommand(cmd DecrementCommand) {
	_, exists := xredis.getAndInvalidateIfExpired(cmd.key)
	if !exists {
		xredis.cache[cmd.key] = XRedisValue{RespString{"0"}, NON_EXPIRATION_TIME}
	}
	respInt, ok := xredis.tryGetAsRespInt(cmd.key)
	if !ok || respInt.Value == math.MinInt64 {
		cmd.rspChannel <- RespString{}
		cmd.errorChannel <- errors.New(REQUEST_ERROR_VALUE_NOT_NUMERIC_OR_MAX_REACHED)
		close(cmd.rspChannel)
		close(cmd.errorChannel)
		return
	}

	newValue := RespString{strconv.FormatInt(respInt.Value-1, 10)}
	xredis.cache[cmd.key] = XRedisValue{newValue, xredis.cache[cmd.key].ExpirationTimestampMillis}
	cmd.rspChannel <- newValue
	cmd.errorChannel <- nil
	close(cmd.rspChannel)
	close(cmd.errorChannel)
}

func (xredis *XRedis) handleLPushCommand(cmd LPushCommand) {
	_, exists := xredis.getAndInvalidateIfExpired(cmd.key)
	if !exists {
		xredis.cache[cmd.key] = XRedisValue{RespArray{make([]RespDataType, 0)}, NON_EXPIRATION_TIME}
	}
	respArray, ok := xredis.cache[cmd.key].Element.(RespArray)
	if !ok {
		cmd.errorChannel <- errors.New(REQUEST_ERROR_VALUE_NOT_A_LIST)
		close(cmd.errorChannel)
		return
	}

	xredis.cache[cmd.key] = XRedisValue{RespArray{append([]RespDataType{cmd.value}, respArray.Elements...)}, xredis.cache[cmd.key].ExpirationTimestampMillis}
	cmd.errorChannel <- nil
	close(cmd.errorChannel)
}

func (xredis *XRedis) handleRPushCommand(cmd RPushCommand) {
	_, exists := xredis.getAndInvalidateIfExpired(cmd.key)
	if !exists {
		xredis.cache[cmd.key] = XRedisValue{RespArray{make([]RespDataType, 0)}, NON_EXPIRATION_TIME}
	}
	respArray, ok := xredis.cache[cmd.key].Element.(RespArray)
	if !ok {
		cmd.errorChannel <- errors.New(REQUEST_ERROR_VALUE_NOT_A_LIST)
		close(cmd.errorChannel)
		return
	}

	xredis.cache[cmd.key] = XRedisValue{RespArray{append(respArray.Elements, cmd.value)}, xredis.cache[cmd.key].ExpirationTimestampMillis}
	cmd.errorChannel <- nil
	close(cmd.errorChannel)
}

func (xredis *XRedis) handleSaveCommand(cmd SaveCommand) {
	defer close(cmd.rspChannel)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(xredis.cache)
	if err != nil {
		log.Println("Failed to serializing cache data: ", err)
		cmd.rspChannel <- nil
		return
	}

	cmd.rspChannel <- buf.Bytes()
}

func (xredis *XRedis) handleLoadCommand(cmd LoadCommand) {
	defer close(cmd.errorChannel)

	if cmd.data != nil {
		buffer := bytes.NewBuffer(cmd.data)
		dec := gob.NewDecoder(buffer)
		if err := dec.Decode(&xredis.cache); err != nil {
			log.Fatalf("failed to deserialize DB dump file: %v", err)
			cmd.errorChannel <- errors.New(REQUEST_RESULT_FAIL)
		}
	}

	cmd.errorChannel <- nil
}

func (xredis *XRedis) getAndInvalidateIfExpired(key string) (XRedisValue, bool) {
	value, exists := xredis.cache[key]
	if !exists {
		return XRedisValue{}, false
	}
	if value.ExpirationTimestampMillis != NON_EXPIRATION_TIME && time.Now().UnixMilli() > value.ExpirationTimestampMillis {
		delete(xredis.cache, key)
		return XRedisValue{}, false
	}
	return value, true
}

func (xredis *XRedis) tryGetAsRespInt(key string) (RespInt, bool) {
	value, exists := xredis.cache[key]
	if !exists {
		return RespInt{}, false
	}

	respInt, isInt := value.Element.(RespInt)
	if isInt {
		return respInt, true
	}

	respStr, isStr := value.Element.(RespString)
	if isStr {
		value, err := strconv.ParseInt(respStr.Str, 10, 64)
		if err == nil {
			return RespInt{value}, true
		}
	}

	return RespInt{}, false
}

type Command interface {
}

type SetCommand struct {
	key                 string
	value               RespDataType
	expirationTimestamp int64
	done                chan struct{}
}

type GetCommand struct {
	key        string
	rspChannel chan RespDataType
}

type ExistsCommand struct {
	key        string
	rspChannel chan bool
}

type DeleteCommand struct {
	key        string
	rspChannel chan bool
}

type IncrementCommand struct {
	key          string
	rspChannel   chan RespString
	errorChannel chan error
}

type DecrementCommand struct {
	key          string
	rspChannel   chan RespString
	errorChannel chan error
}

type LPushCommand struct {
	key          string
	value        RespDataType
	errorChannel chan error
}

type RPushCommand struct {
	key          string
	value        RespDataType
	errorChannel chan error
}

type SaveCommand struct {
	rspChannel chan []byte
}

type LoadCommand struct {
	data         []byte
	errorChannel chan error
}
