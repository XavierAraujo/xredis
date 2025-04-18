package main

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

func handleRequest(xredis *XRedis, data []byte) []byte {
	respData, _, err := deserializeRespDataType(data)
	if err != nil {
		return []byte(RespError{REQUEST_ERROR_FAILED_DESERIALIZATION}.serialize())
	}

	if !isValidRequest(respData) {
		return []byte(RespError{REQUEST_ERROR_UNEXPECTED_ARG_TYPE}.serialize())
	}

	commandData, _ := respData.(RespArray) // Cast already previously validated
	command := strings.ToUpper(commandData.Elements[REQUEST_INDEX].(RespString).Str)

	var rsp RespDataType
	switch command {
	case REQUEST_PING:
		rsp = handlePingRequest(commandData)
	case REQUEST_ECHO:
		rsp = handleEchoRequest(commandData)
	case REQUEST_SET:
		rsp = handleSetRequest(commandData, xredis)
	case REQUEST_GET:
		rsp = handleGetRequest(commandData, xredis)
	case REQUEST_DELETE:
		rsp = handleDeleteRequest(commandData, xredis)
	case REQUEST_EXISTS:
		rsp = handleExistsRequest(commandData, xredis)
	case REQUEST_INCREMENT:
		rsp = handleIncrementRequest(commandData, xredis)
	case REQUEST_DECREMENT:
		rsp = handleDecrementRequest(commandData, xredis)
	case REQUEST_LPUSH:
		rsp = handleLPushRequest(commandData, xredis)
	case REQUEST_RPUSH:
		rsp = handleRPushRequest(commandData, xredis)
	case REQUEST_SAVE:
		rsp = handleSaveRequest(commandData, xredis)
	default:
		rsp = RespError{REQUEST_ERROR_INVALID_COMMAND}
	}
	return []byte(rsp.serialize())
}

func handlePingRequest(requestData RespArray) RespDataType {
	if len(requestData.Elements) != REQUEST_PING_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	return RespString{REQUEST_PING_RSP}
}

func handleEchoRequest(requestData RespArray) RespDataType {
	if len(requestData.Elements) != REQUEST_ECHO_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	return requestData.Elements[REQUEST_ECHO_VALUE]
}

func handleSetRequest(requestData RespArray, xredis *XRedis) RespDataType {
	commandSize := len(requestData.Elements)
	if commandSize != REQUEST_SET_EXPECTED_SIZE && commandSize != REQUEST_SET_WITH_TIMEOUT_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}

	key := requestData.Elements[REQUEST_SET_KEY_INDEX].(RespString).Str
	value := requestData.Elements[REQUEST_SET_VALUE_INDEX]
	isToSetExpiration := len(requestData.Elements) == REQUEST_SET_WITH_TIMEOUT_EXPECTED_SIZE

	if isToSetExpiration {
		expirationTime, err := getSetRequestExpirationTime(requestData)
		if err != nil {
			return RespError{err.Error()}
		}
		xredis.SetWithExpiration(key, value, expirationTime)
	} else {
		xredis.Set(key, value)
	}

	return RespString{REQUEST_RESULT_OK}
}

func handleGetRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_GET_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_GET_KEY_INDEX].(RespString).Str
	return xredis.Get(key)
}

func handleExistsRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_EXISTS_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_EXISTS_KEY_INDEX].(RespString).Str
	return RespInt{int64(bool2Int(xredis.Exists(key)))}
}

func handleDeleteRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_DELETE_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_DELETE_KEY_INDEX].(RespString).Str
	return RespInt{int64(bool2Int(xredis.Delete(key)))}
}

func handleIncrementRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_INCREMENT_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_INCREMENT_KEY_INDEX].(RespString).Str
	result, err := xredis.Increment(key)
	if err != nil {
		return RespError{err.Error()}
	}
	return result
}

func handleDecrementRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_DECREMENT_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_DECREMENT_KEY_INDEX].(RespString).Str
	result, err := xredis.Decrement(key)
	if err != nil {
		return RespError{err.Error()}
	}
	return result
}

func handleLPushRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_LPUSH_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_LPUSH_KEY_INDEX].(RespString).Str
	value := requestData.Elements[REQUEST_LPUSH_VALUE_INDEX]

	err := xredis.LPush(key, value)
	if err != nil {
		return RespError{err.Error()}
	}
	return RespString{REQUEST_RESULT_OK}
}

func handleRPushRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_RPUSH_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_RPUSH_KEY_INDEX].(RespString).Str
	value := requestData.Elements[REQUEST_RPUSH_VALUE_INDEX]

	err := xredis.RPush(key, value)
	if err != nil {
		return RespError{err.Error()}
	}
	return RespString{REQUEST_RESULT_OK}
}

func handleSaveRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_SAVE_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	data := xredis.Serialize()

	file, err := os.Create(DB_DUMP_FILE)
	if err != nil {
		return RespError{err.Error()}
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return RespError{err.Error()}
	}

	return RespString{REQUEST_RESULT_OK}
}

func getSetRequestExpirationTime(requestData RespArray) (time.Time, error) {
	timeoutMode := requestData.Elements[REQUEST_SET_TIMEOUT_MODE_INDEX].(RespString).Str
	timeoutValue, err := strconv.ParseInt(requestData.Elements[REQUEST_SET_TIMEOUT_INDEX].(RespString).Str, 10, 64)
	if err != nil {
		return time.Time{}, errors.New(REQUEST_ERROR_INVALID_TIMEOUT_VALUE)
	}
	switch timeoutMode {
	case EXPIRATION_MODE_EXPIRE_SECONDS:
		return time.Now().Add(time.Duration(timeoutValue) * time.Second), nil
	case EXPIRATION_MODE_EXPIRE_MILLISECONDS:
		return time.Now().Add(time.Duration(timeoutValue) * time.Millisecond), nil
	case EXPIRATION_MODE_TIMESTAMP_SECONDS:
		return time.Unix(int64(timeoutValue), 0), nil
	case EXPIRATION_MODE_TIMESTAMP_MILLISECONDS:
		return time.Unix(0, int64(timeoutValue)*int64(time.Millisecond)), nil
	default:
		return time.Time{}, errors.New(REQUEST_ERROR_UNRECOGNIZED_TIMEOUT_MODE)
	}
}

func isValidRequest(command RespDataType) bool {
	commandData, ok := command.(RespArray)
	if !ok || len(commandData.Elements) <= 0 {
		return false
	}
	for _, element := range commandData.Elements {
		_, ok := element.(RespString)
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
