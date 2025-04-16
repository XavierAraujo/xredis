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
				xredis.cache[cmd.key] = cmd.value
				cmd.rspChannel <- RespBulkString{COMMAND_RESULT_OK}
				close(cmd.rspChannel)
				break
			case GetCommand:
				var rsp RespDataType = RespNil{}
				if xredis.cache[cmd.key] != nil {
					rsp = xredis.cache[cmd.key]
				}
				cmd.rspChannel <- rsp
				close(cmd.rspChannel)
				break
			case ExistsCommand:
				exists := xredis.cache[cmd.key] != nil
				cmd.rspChannel <- RespInt{int64(bool2Int(exists))}
				close(cmd.rspChannel)
				break
			case DeleteCommand:
				existed := xredis.cache[cmd.key] != nil
				xredis.cache[cmd.key] = nil
				cmd.rspChannel <- RespInt{int64(bool2Int(existed))}
				close(cmd.rspChannel)
			case IncrementCommand:
				if xredis.cache[cmd.key] == nil {
					xredis.cache[cmd.key] = RespInt{0}
				}
				respInt, ok := tryGetAsRespInt(xredis.cache[cmd.key])
				if !ok || respInt.value == math.MaxInt64 {
					cmd.rspChannel <- RespError{COMMAND_ERROR_VALUE_NOT_NUMERIC_OR_MAX_REACHED}
					close(cmd.rspChannel)
					break
				}

				newValue := RespInt{respInt.value + 1}
				xredis.cache[cmd.key] = newValue
				cmd.rspChannel <- newValue
				close(cmd.rspChannel)
				break
			case DecrementCommand:
				if xredis.cache[cmd.key] == nil {
					xredis.cache[cmd.key] = RespInt{0}
				}
				respInt, ok := tryGetAsRespInt(xredis.cache[cmd.key])
				if !ok || respInt.value == math.MinInt64 {
					cmd.rspChannel <- RespError{COMMAND_ERROR_VALUE_NOT_NUMERIC_OR_MAX_REACHED}
					close(cmd.rspChannel)
					break
				}

				newValue := RespInt{respInt.value - 1}
				xredis.cache[cmd.key] = newValue
				cmd.rspChannel <- newValue
				close(cmd.rspChannel)
				break
			case LPushCommand:
				if xredis.cache[cmd.key] == nil {
					xredis.cache[cmd.key] = RespArray{make([]RespDataType, 0)}
				}
				respArray, ok := xredis.cache[cmd.key].(RespArray)
				if !ok {
					cmd.rspChannel <- RespError{COMMAND_ERROR_VALUE_NOT_A_LIST}
					close(cmd.rspChannel)
					break
				}

				newList := RespArray{append([]RespDataType{cmd.value}, respArray.elements...)}
				xredis.cache[cmd.key] = newList
				cmd.rspChannel <- RespBulkString{COMMAND_RESULT_OK}
				close(cmd.rspChannel)
				break
			case RPushCommand:
				if xredis.cache[cmd.key] == nil {
					xredis.cache[cmd.key] = RespArray{make([]RespDataType, 0)}
				}
				respArray, ok := xredis.cache[cmd.key].(RespArray)
				if !ok {
					cmd.rspChannel <- RespError{COMMAND_ERROR_VALUE_NOT_A_LIST}
					close(cmd.rspChannel)
					break
				}

				newList := RespArray{append(respArray.elements, cmd.value)}
				xredis.cache[cmd.key] = newList
				cmd.rspChannel <- RespBulkString{COMMAND_RESULT_OK}
				close(cmd.rspChannel)
				break
			}
		}
	}()
	return &xredis
}

func (xredis *XRedis) handleRequest(data []byte) RespDataType {
	respData, _, err := deserialize(data)
	if err != nil {
		return RespError{COMMAND_ERROR_FAILED_DESERIALIZATION}
	}

	if !isValidCommand(respData) {
		return RespError{COMMAND_ERROR_UNEXPECTED_RESP_TYPES}
	}

	commandData, _ := respData.(RespArray)
	command := strings.ToUpper(commandData.elements[COMMAND_INDEX].(RespBulkString).str)

	switch command {
	case COMMAND_PING:
		return handlePing(commandData)
	case COMMAND_ECHO:
		return handleEcho(commandData)
	case COMMAND_SET:
		return handleSet(commandData, xredis)
	case COMMAND_GET:
		return handleGet(commandData, xredis)
	case COMMAND_DELETE:
		return handleDelete(commandData, xredis)
	case COMMAND_EXISTS:
		return handleExists(commandData, xredis)
	case COMMAND_INCREMENT:
		return handleIncrement(commandData, xredis)
	case COMMAND_DECREMENT:
		return handleDecrement(commandData, xredis)
	case COMMAND_LPUSH:
		return handleLPush(commandData, xredis)
	case COMMAND_RPUSH:
		return handleRPush(commandData, xredis)
	default:
		return RespError{COMMAND_ERROR_INVALID_COMMAND}
	}
}

func handlePing(commandData RespArray) RespDataType {
	if len(commandData.elements) != COMMAND_PING_EXPECTED_SIZE {
		return RespError{COMMAND_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	return RespBulkString{"PONG"}
}

func handleEcho(commandData RespArray) RespDataType {
	if len(commandData.elements) != COMMAND_ECHO_EXPECTED_SIZE {
		return RespError{COMMAND_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	return commandData.elements[COMMAND_ECHO_VALUE]
}

func handleSet(commandData RespArray, xredis *XRedis) RespDataType {
	err := validateSetCommandData(commandData)
	if err != nil {
		return RespError{err.Error()}
	}

	key := commandData.elements[COMMAND_SET_KEY_INDEX].(RespBulkString).str
	value := commandData.elements[COMMAND_SET_VALUE_INDEX]
	isToSetExpiration := len(commandData.elements) == COMMAND_SET_WITH_TIMEOUT_EXPECTED_SIZE
	if isToSetExpiration {
		expirationTime := getSetTimeout(commandData).UnixMilli()
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

func handleGet(commandData RespArray, xredis *XRedis) RespDataType {
	if len(commandData.elements) != COMMAND_GET_EXPECTED_SIZE {
		return RespError{COMMAND_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := commandData.elements[COMMAND_GET_KEY_INDEX].(RespBulkString).str
	rspChan := make(chan RespDataType)
	xredis.commands <- GetCommand{key, rspChan}
	return <-rspChan
}

func handleExists(commandData RespArray, xredis *XRedis) RespDataType {
	if len(commandData.elements) != COMMAND_EXISTS_EXPECTED_SIZE {
		return RespError{COMMAND_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := commandData.elements[COMMAND_EXISTS_KEY_INDEX].(RespBulkString).str
	rspChan := make(chan RespDataType)
	xredis.commands <- ExistsCommand{key, rspChan}
	return <-rspChan
}

func handleDelete(commandData RespArray, xredis *XRedis) RespDataType {
	if len(commandData.elements) != COMMAND_DELETE_EXPECTED_SIZE {
		return RespError{COMMAND_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := commandData.elements[COMMAND_DELETE_KEY_INDEX].(RespBulkString).str
	rspChan := make(chan RespDataType)
	xredis.commands <- DeleteCommand{key, rspChan}
	return <-rspChan
}

func handleIncrement(commandData RespArray, xredis *XRedis) RespDataType {
	if len(commandData.elements) != COMMAND_INCREMENT_EXPECTED_SIZE {
		return RespError{COMMAND_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := commandData.elements[COMMAND_INCREMENT_KEY_INDEX].(RespBulkString).str
	rspChan := make(chan RespDataType)
	xredis.commands <- IncrementCommand{key, rspChan}
	return <-rspChan
}

func handleDecrement(commandData RespArray, xredis *XRedis) RespDataType {
	if len(commandData.elements) != COMMAND_DECREMENT_EXPECTED_SIZE {
		return RespError{COMMAND_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := commandData.elements[COMMAND_DECREMENT_KEY_INDEX].(RespBulkString).str
	rspChan := make(chan RespDataType)
	xredis.commands <- DecrementCommand{key, rspChan}
	return <-rspChan
}

func handleLPush(commandData RespArray, xredis *XRedis) RespDataType {
	if len(commandData.elements) != COMMAND_LPUSH_EXPECTED_SIZE {
		return RespError{COMMAND_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := commandData.elements[COMMAND_LPUSH_KEY_INDEX].(RespBulkString).str
	value := commandData.elements[COMMAND_LPUSH_VALUE_INDEX]
	rspChan := make(chan RespDataType)
	xredis.commands <- LPushCommand{key, value, rspChan}
	return <-rspChan
}

func handleRPush(commandData RespArray, xredis *XRedis) RespDataType {
	if len(commandData.elements) != COMMAND_RPUSH_EXPECTED_SIZE {
		return RespError{COMMAND_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := commandData.elements[COMMAND_RPUSH_KEY_INDEX].(RespBulkString).str
	value := commandData.elements[COMMAND_RPUSH_VALUE_INDEX]
	rspChan := make(chan RespDataType)
	xredis.commands <- RPushCommand{key, value, rspChan}
	return <-rspChan
}

func validateSetCommandData(commandData RespArray) error {
	commandSize := len(commandData.elements)
	if commandSize != COMMAND_SET_EXPECTED_SIZE && commandSize != COMMAND_SET_WITH_TIMEOUT_EXPECTED_SIZE {
		return errors.New(COMMAND_ERROR_INVALID_ARGUMENTS_NUMBER)
	}
	if commandSize == COMMAND_SET_WITH_TIMEOUT_EXPECTED_SIZE {
		timeoutMode := commandData.elements[COMMAND_SET_TIMEOUT_MODE_INDEX].(RespBulkString).str
		switch timeoutMode {
		case COMMAND_SET_TIMEOUT_MODE_EXPIRE_SECONDS:
		case COMMAND_SET_TIMEOUT_MODE_EXPIRE_MILLISECONDS:
		case COMMAND_SET_TIMEOUT_MODE_TIMESTAMP_SECONDS:
		case COMMAND_SET_TIMEOUT_MODE_TIMESTAMP_MILLISECONDS:
			if !isInt64String(commandData.elements[COMMAND_SET_TIMEOUT_INDEX].(RespBulkString).str) {
				return errors.New(COMMAND_ERROR_INVALID_TIMEOUT_VALUE)
			}
			break
		default:
			return errors.New(COMMAND_ERROR_UNRECOGNIZED_TIMEOUT_MODE)
		}
	}
	return nil
}

func getSetTimeout(commandData RespArray) time.Time {
	timeoutMode := commandData.elements[COMMAND_SET_TIMEOUT_MODE_INDEX].(RespBulkString).str
	timeoutValue, _ := strconv.Atoi(commandData.elements[COMMAND_SET_TIMEOUT_INDEX].(RespBulkString).str)
	switch timeoutMode {
	case COMMAND_SET_TIMEOUT_MODE_EXPIRE_SECONDS:
		return time.Now().Add(time.Duration(timeoutValue) * time.Second)
	case COMMAND_SET_TIMEOUT_MODE_EXPIRE_MILLISECONDS:
		return time.Now().Add(time.Duration(timeoutValue) * time.Millisecond)
	case COMMAND_SET_TIMEOUT_MODE_TIMESTAMP_SECONDS:
		return time.Unix(int64(timeoutValue), 0)
	case COMMAND_SET_TIMEOUT_MODE_TIMESTAMP_MILLISECONDS:
		return time.Unix(0, int64(timeoutValue)*int64(time.Millisecond))
	default:
		// Should never happen since it was previously validated
		log.Panic(COMMAND_ERROR_UNRECOGNIZED_TIMEOUT_MODE)
		return time.Now() // Needed for compiler return verification
	}
}

func isValidCommand(command RespDataType) bool {
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

	respStr, isStr := element.(RespString)
	if isStr {
		value, err := strconv.ParseInt(respStr.str, 10, 64)
		if err != nil {
			return RespInt{value}, true
		}
	}

	return RespInt{}, false
}
