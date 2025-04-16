package main

import (
	"errors"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)

type XRedis struct {
	cache    map[string]RespDataType
	commands chan Command
}

func NewXRedis() *XRedis {
	xredis := XRedis{make(map[string]RespDataType), make(chan Command)}
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
			}
		}
	}()
	return &xredis
}

func (xredis *XRedis) handleSetCommand(cmd SetCommand) {
	xredis.cache[cmd.key] = cmd.value
	cmd.rspChannel <- RespBulkString{REQUEST_RESULT_OK}
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleGetCommand(cmd GetCommand) {
	var rsp RespDataType = RespNil{}
	if xredis.cache[cmd.key] != nil {
		rsp = xredis.cache[cmd.key]
	}
	cmd.rspChannel <- rsp
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleExistsCommand(cmd ExistsCommand) {
	exists := xredis.cache[cmd.key] != nil
	cmd.rspChannel <- RespInt{int64(bool2Int(exists))}
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleDeleteCommand(cmd DeleteCommand) {
	existed := xredis.cache[cmd.key] != nil
	xredis.cache[cmd.key] = nil
	cmd.rspChannel <- RespInt{int64(bool2Int(existed))}
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleIncrementCommand(cmd IncrementCommand) {
	if xredis.cache[cmd.key] == nil {
		xredis.cache[cmd.key] = RespInt{0}
	}
	respInt, ok := tryGetAsRespInt(xredis.cache[cmd.key])
	if !ok || respInt.value == math.MaxInt64 {
		cmd.rspChannel <- RespError{REQUEST_ERROR_VALUE_NOT_NUMERIC_OR_MAX_REACHED}
		close(cmd.rspChannel)
		return
	}

	newValue := RespInt{respInt.value + 1}
	xredis.cache[cmd.key] = newValue
	cmd.rspChannel <- newValue
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleDecrementCommand(cmd DecrementCommand) {
	if xredis.cache[cmd.key] == nil {
		xredis.cache[cmd.key] = RespInt{0}
	}
	respInt, ok := tryGetAsRespInt(xredis.cache[cmd.key])
	if !ok || respInt.value == math.MinInt64 {
		cmd.rspChannel <- RespError{REQUEST_ERROR_VALUE_NOT_NUMERIC_OR_MAX_REACHED}
		close(cmd.rspChannel)
		return
	}

	newValue := RespInt{respInt.value - 1}
	xredis.cache[cmd.key] = newValue
	cmd.rspChannel <- newValue
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleLPushCommand(cmd LPushCommand) {
	xredis.handleLPushCommand(cmd)
	if xredis.cache[cmd.key] == nil {
		xredis.cache[cmd.key] = RespArray{make([]RespDataType, 0)}
	}
	respArray, ok := xredis.cache[cmd.key].(RespArray)
	if !ok {
		cmd.rspChannel <- RespError{REQUEST_ERROR_VALUE_NOT_A_LIST}
		close(cmd.rspChannel)
		return
	}

	newList := RespArray{append([]RespDataType{cmd.value}, respArray.elements...)}
	xredis.cache[cmd.key] = newList
	cmd.rspChannel <- RespBulkString{REQUEST_RESULT_OK}
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleRPushCommand(cmd RPushCommand) {
	xredis.handleRPushCommand(cmd)
	if xredis.cache[cmd.key] == nil {
		xredis.cache[cmd.key] = RespArray{make([]RespDataType, 0)}
	}
	respArray, ok := xredis.cache[cmd.key].(RespArray)
	if !ok {
		cmd.rspChannel <- RespError{REQUEST_ERROR_VALUE_NOT_A_LIST}
		close(cmd.rspChannel)
		return
	}

	newList := RespArray{append(respArray.elements, cmd.value)}
	xredis.cache[cmd.key] = newList
	cmd.rspChannel <- RespBulkString{REQUEST_RESULT_OK}
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleClientRequest(data []byte) RespDataType {
	respData, _, err := deserialize(data)
	if err != nil {
		return RespError{REQUEST_ERROR_FAILED_DESERIALIZATION}
	}

	if !isValidRequest(respData) {
		return RespError{REQUEST_ERROR_UNEXPECTED_RESP_TYPES}
	}

	commandData, _ := respData.(RespArray)
	command := strings.ToUpper(commandData.elements[REQUEST_INDEX].(RespBulkString).str)

	switch command {
	case REQUEST_PING:
		return handleClientPingRequest(commandData)
	case REQUEST_ECHO:
		return handleClientEchoRequest(commandData)
	case REQUEST_SET:
		return handleClientSetRequest(commandData, xredis)
	case REQUEST_GET:
		return handleClientGetRequest(commandData, xredis)
	case REQUEST_DELETE:
		return handleClientDeleteRequest(commandData, xredis)
	case REQUEST_EXISTS:
		return handleClientExistsRequest(commandData, xredis)
	case REQUEST_INCREMENT:
		return handleClientIncrementRequest(commandData, xredis)
	case REQUEST_DECREMENT:
		return handleClientDecrementRequest(commandData, xredis)
	case REQUEST_LPUSH:
		return handleClientLPushRequest(commandData, xredis)
	case REQUEST_RPUSH:
		return handleClientRPushRequest(commandData, xredis)
	default:
		return RespError{REQUEST_ERROR_INVALID_COMMAND}
	}
}

func handleClientPingRequest(requestData RespArray) RespDataType {
	if len(requestData.elements) != REQUEST_PING_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	return RespBulkString{"PONG"}
}

func handleClientEchoRequest(requestData RespArray) RespDataType {
	if len(requestData.elements) != REQUEST_ECHO_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	return requestData.elements[REQUEST_ECHO_VALUE]
}

func handleClientSetRequest(requestData RespArray, xredis *XRedis) RespDataType {
	err := validateSetRequestData(requestData)
	if err != nil {
		return RespError{err.Error()}
	}

	key := requestData.elements[REQUEST_SET_KEY_INDEX].(RespBulkString).str
	value := requestData.elements[REQUEST_SET_VALUE_INDEX]
	isToSetExpiration := len(requestData.elements) == REQUEST_SET_WITH_TIMEOUT_EXPECTED_SIZE
	if isToSetExpiration {
		expirationTime := getSetRequestTimeout(requestData).UnixMilli()
		now := time.Now().UnixMilli()
		duration := time.Duration(expirationTime-now) * time.Millisecond
		time.AfterFunc(duration, func() {
			rspChan := make(chan RespDataType, 1) // Buffered channel of size of to ignore the return
			xredis.commands <- DeleteCommand{key, rspChan}
		})
	}

	rspChan := make(chan RespDataType)
	xredis.commands <- SetCommand{key, value, rspChan}
	return <-rspChan
}

func handleClientGetRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.elements) != REQUEST_GET_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.elements[REQUEST_GET_KEY_INDEX].(RespBulkString).str
	rspChan := make(chan RespDataType)
	xredis.commands <- GetCommand{key, rspChan}
	return <-rspChan
}

func handleClientExistsRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.elements) != REQUEST_EXISTS_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.elements[REQUEST_EXISTS_KEY_INDEX].(RespBulkString).str
	rspChan := make(chan RespDataType)
	xredis.commands <- ExistsCommand{key, rspChan}
	return <-rspChan
}

func handleClientDeleteRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.elements) != REQUEST_DELETE_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.elements[REQUEST_DELETE_KEY_INDEX].(RespBulkString).str
	rspChan := make(chan RespDataType)
	xredis.commands <- DeleteCommand{key, rspChan}
	return <-rspChan
}

func handleClientIncrementRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.elements) != REQUEST_INCREMENT_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.elements[REQUEST_INCREMENT_KEY_INDEX].(RespBulkString).str
	rspChan := make(chan RespDataType)
	xredis.commands <- IncrementCommand{key, rspChan}
	return <-rspChan
}

func handleClientDecrementRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.elements) != REQUEST_DECREMENT_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.elements[REQUEST_DECREMENT_KEY_INDEX].(RespBulkString).str
	rspChan := make(chan RespDataType)
	xredis.commands <- DecrementCommand{key, rspChan}
	return <-rspChan
}

func handleClientLPushRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.elements) != REQUEST_LPUSH_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.elements[REQUEST_LPUSH_KEY_INDEX].(RespBulkString).str
	value := requestData.elements[REQUEST_LPUSH_VALUE_INDEX]
	rspChan := make(chan RespDataType)
	xredis.commands <- LPushCommand{key, value, rspChan}
	return <-rspChan
}

func handleClientRPushRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.elements) != REQUEST_RPUSH_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.elements[REQUEST_RPUSH_KEY_INDEX].(RespBulkString).str
	value := requestData.elements[REQUEST_RPUSH_VALUE_INDEX]
	rspChan := make(chan RespDataType)
	xredis.commands <- RPushCommand{key, value, rspChan}
	return <-rspChan
}

func validateSetRequestData(requestData RespArray) error {
	commandSize := len(requestData.elements)
	if commandSize != REQUEST_SET_EXPECTED_SIZE && commandSize != REQUEST_SET_WITH_TIMEOUT_EXPECTED_SIZE {
		return errors.New(REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER)
	}
	if commandSize == REQUEST_SET_WITH_TIMEOUT_EXPECTED_SIZE {
		timeoutMode := requestData.elements[REQUEST_SET_TIMEOUT_MODE_INDEX].(RespBulkString).str
		switch timeoutMode {
		case REQUEST_SET_TIMEOUT_MODE_EXPIRE_SECONDS:
		case REQUEST_SET_TIMEOUT_MODE_EXPIRE_MILLISECONDS:
		case REQUEST_SET_TIMEOUT_MODE_TIMESTAMP_SECONDS:
		case REQUEST_SET_TIMEOUT_MODE_TIMESTAMP_MILLISECONDS:
			if !isInt64String(requestData.elements[REQUEST_SET_TIMEOUT_INDEX].(RespBulkString).str) {
				return errors.New(REQUEST_ERROR_INVALID_TIMEOUT_VALUE)
			}
			break
		default:
			return errors.New(REQUEST_ERROR_UNRECOGNIZED_TIMEOUT_MODE)
		}
	}
	return nil
}

func getSetRequestTimeout(requestData RespArray) time.Time {
	timeoutMode := requestData.elements[REQUEST_SET_TIMEOUT_MODE_INDEX].(RespBulkString).str
	timeoutValue, _ := strconv.Atoi(requestData.elements[REQUEST_SET_TIMEOUT_INDEX].(RespBulkString).str)
	switch timeoutMode {
	case REQUEST_SET_TIMEOUT_MODE_EXPIRE_SECONDS:
		return time.Now().Add(time.Duration(timeoutValue) * time.Second)
	case REQUEST_SET_TIMEOUT_MODE_EXPIRE_MILLISECONDS:
		return time.Now().Add(time.Duration(timeoutValue) * time.Millisecond)
	case REQUEST_SET_TIMEOUT_MODE_TIMESTAMP_SECONDS:
		return time.Unix(int64(timeoutValue), 0)
	case REQUEST_SET_TIMEOUT_MODE_TIMESTAMP_MILLISECONDS:
		return time.Unix(0, int64(timeoutValue)*int64(time.Millisecond))
	default:
		// Should never happen since it was previously validated
		log.Panic(REQUEST_ERROR_UNRECOGNIZED_TIMEOUT_MODE)
		return time.Now() // Needed for compiler return verification
	}
}

func isValidRequest(command RespDataType) bool {
	commandData, ok := command.(RespArray)
	if !ok || len(commandData.elements) <= 0 {
		return false
	}
	for _, element := range commandData.elements {
		_, ok := element.(RespBulkString)
		if !ok {
			return false
		}
	}
	return true
}

func bool2Int(boolVal bool) int {
	val := 0
	if boolVal {
		val = 1
	}
	return val
}

func isInt64String(str string) bool {
	_, err := strconv.ParseInt(str, 10, 64)
	return err == nil
}

func tryGetAsRespInt(element RespDataType) (RespInt, bool) {
	respInt, isInt := element.(RespInt)
	if isInt {
		return respInt, true
	}

	respStr, isStr := element.(RespBulkString)
	if isStr {
		value, err := strconv.ParseInt(respStr.str, 10, 64)
		if err == nil {
			return RespInt{value}, true
		}
	}

	return RespInt{}, false
}
