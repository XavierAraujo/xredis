package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)

type XRedis struct {
	cache     map[string]RespDataType
	commands  chan Command
	persistor Persistor
}

func NewXRedis() *XRedis {
	xredis := XRedis{make(map[string]RespDataType), make(chan Command), &DiskPersistor{}}
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
	xredis.registerDataTypeForSerialization()
	xredis.loadDbFromDumpFile()
	return &xredis
}

func (xredis *XRedis) registerDataTypeForSerialization() {
	gob.Register(RespString{})
	gob.Register(RespInt{})
	gob.Register(RespArray{})
}

func (xredis *XRedis) loadDbFromDumpFile() {
	rspChannel := make(chan RespDataType, 1)
	xredis.commands <- LoadCommand{rspChannel}
}

func (xredis *XRedis) handleSetCommand(cmd SetCommand) {
	xredis.cache[cmd.key] = cmd.value
	cmd.rspChannel <- RespString{REQUEST_RESULT_OK}
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
	if !ok || respInt.Value == math.MaxInt64 {
		cmd.rspChannel <- RespError{REQUEST_ERROR_VALUE_NOT_NUMERIC_OR_MAX_REACHED}
		close(cmd.rspChannel)
		return
	}

	newValue := RespInt{respInt.Value + 1}
	xredis.cache[cmd.key] = newValue
	cmd.rspChannel <- newValue
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleDecrementCommand(cmd DecrementCommand) {
	if xredis.cache[cmd.key] == nil {
		xredis.cache[cmd.key] = RespInt{0}
	}
	respInt, ok := tryGetAsRespInt(xredis.cache[cmd.key])
	if !ok || respInt.Value == math.MinInt64 {
		cmd.rspChannel <- RespError{REQUEST_ERROR_VALUE_NOT_NUMERIC_OR_MAX_REACHED}
		close(cmd.rspChannel)
		return
	}

	newValue := RespInt{respInt.Value - 1}
	xredis.cache[cmd.key] = newValue
	cmd.rspChannel <- newValue
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleLPushCommand(cmd LPushCommand) {
	if xredis.cache[cmd.key] == nil {
		xredis.cache[cmd.key] = RespArray{make([]RespDataType, 0)}
	}
	respArray, ok := xredis.cache[cmd.key].(RespArray)
	if !ok {
		cmd.rspChannel <- RespError{REQUEST_ERROR_VALUE_NOT_A_LIST}
		close(cmd.rspChannel)
		return
	}

	xredis.cache[cmd.key] = RespArray{append([]RespDataType{cmd.value}, respArray.Elements...)}
	cmd.rspChannel <- RespString{REQUEST_RESULT_OK}
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleRPushCommand(cmd RPushCommand) {
	if xredis.cache[cmd.key] == nil {
		xredis.cache[cmd.key] = RespArray{make([]RespDataType, 0)}
	}
	respArray, ok := xredis.cache[cmd.key].(RespArray)
	if !ok {
		cmd.rspChannel <- RespError{REQUEST_ERROR_VALUE_NOT_A_LIST}
		close(cmd.rspChannel)
		return
	}

	xredis.cache[cmd.key] = RespArray{append(respArray.Elements, cmd.value)}
	cmd.rspChannel <- RespString{REQUEST_RESULT_OK}
	close(cmd.rspChannel)
}

func (xredis *XRedis) handleSaveCommand(cmd SaveCommand) {
	defer close(cmd.rspChannel)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(xredis.cache)
	if err != nil {
		log.Println("Failed to serializing cache data: ", err)
		cmd.rspChannel <- RespString{REQUEST_RESULT_FAIL}
		return
	}

	err = xredis.persistor.save(buf.Bytes())
	if err != nil {
		log.Println("Failed to save current state: ", err)
		cmd.rspChannel <- RespString{REQUEST_RESULT_FAIL}
		return
	}

	cmd.rspChannel <- RespString{REQUEST_RESULT_OK}
}

func (xredis *XRedis) handleLoadCommand(cmd LoadCommand) {
	defer close(cmd.rspChannel)

	data, err := xredis.persistor.load()
	if err != nil {
		log.Println("Failed to load stored state: ", err)
		cmd.rspChannel <- RespString{REQUEST_RESULT_FAIL}
	}

	if data != nil {
		buffer := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buffer)
		if err := dec.Decode(&xredis.cache); err != nil {
			log.Fatalf("failed to deserialize DB dump file: %v", err)
			cmd.rspChannel <- RespString{REQUEST_RESULT_FAIL}
		}
	}

	cmd.rspChannel <- RespString{REQUEST_RESULT_OK}
}

func (xredis *XRedis) handleRequest(data []byte) RespDataType {
	respData, _, err := deserializeClientRequest(data)
	if err != nil {
		return RespError{REQUEST_ERROR_FAILED_DESERIALIZATION}
	}

	if !isValidRequest(respData) {
		return RespError{REQUEST_ERROR_UNEXPECTED_RESP_TYPES}
	}

	commandData, _ := respData.(RespArray)
	command := strings.ToUpper(commandData.Elements[REQUEST_INDEX].(RespString).Str)

	switch command {
	case REQUEST_PING:
		return handlePingRequest(commandData)
	case REQUEST_ECHO:
		return handleEchoRequest(commandData)
	case REQUEST_SET:
		return handleSetRequest(commandData, xredis)
	case REQUEST_GET:
		return handleGetRequest(commandData, xredis)
	case REQUEST_DELETE:
		return handleDeleteRequest(commandData, xredis)
	case REQUEST_EXISTS:
		return handleExistsRequest(commandData, xredis)
	case REQUEST_INCREMENT:
		return handleIncrementRequest(commandData, xredis)
	case REQUEST_DECREMENT:
		return handleDecrementRequest(commandData, xredis)
	case REQUEST_LPUSH:
		return handleLPushRequest(commandData, xredis)
	case REQUEST_RPUSH:
		return handleRPushRequest(commandData, xredis)
	case REQUEST_SAVE:
		return handleSaveRequest(commandData, xredis)
	default:
		return RespError{REQUEST_ERROR_INVALID_COMMAND}
	}
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
	err := validateSetRequestData(requestData)
	if err != nil {
		return RespError{err.Error()}
	}

	key := requestData.Elements[REQUEST_SET_KEY_INDEX].(RespString).Str
	value := requestData.Elements[REQUEST_SET_VALUE_INDEX]
	isToSetExpiration := len(requestData.Elements) == REQUEST_SET_WITH_TIMEOUT_EXPECTED_SIZE
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

func handleGetRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_GET_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_GET_KEY_INDEX].(RespString).Str
	rspChan := make(chan RespDataType)
	xredis.commands <- GetCommand{key, rspChan}
	return <-rspChan
}

func handleExistsRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_EXISTS_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_EXISTS_KEY_INDEX].(RespString).Str
	rspChan := make(chan RespDataType)
	xredis.commands <- ExistsCommand{key, rspChan}
	return <-rspChan
}

func handleDeleteRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_DELETE_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_DELETE_KEY_INDEX].(RespString).Str
	rspChan := make(chan RespDataType)
	xredis.commands <- DeleteCommand{key, rspChan}
	return <-rspChan
}

func handleIncrementRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_INCREMENT_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_INCREMENT_KEY_INDEX].(RespString).Str
	rspChan := make(chan RespDataType)
	xredis.commands <- IncrementCommand{key, rspChan}
	return <-rspChan
}

func handleDecrementRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_DECREMENT_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_DECREMENT_KEY_INDEX].(RespString).Str
	rspChan := make(chan RespDataType)
	xredis.commands <- DecrementCommand{key, rspChan}
	return <-rspChan
}

func handleLPushRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_LPUSH_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_LPUSH_KEY_INDEX].(RespString).Str
	value := requestData.Elements[REQUEST_LPUSH_VALUE_INDEX]
	rspChan := make(chan RespDataType)
	xredis.commands <- LPushCommand{key, value, rspChan}
	return <-rspChan
}

func handleRPushRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_RPUSH_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	key := requestData.Elements[REQUEST_RPUSH_KEY_INDEX].(RespString).Str
	value := requestData.Elements[REQUEST_RPUSH_VALUE_INDEX]
	rspChan := make(chan RespDataType)
	xredis.commands <- RPushCommand{key, value, rspChan}
	return <-rspChan
}

func handleSaveRequest(requestData RespArray, xredis *XRedis) RespDataType {
	if len(requestData.Elements) != REQUEST_SAVE_EXPECTED_SIZE {
		return RespError{REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER}
	}
	rspChan := make(chan RespDataType)
	xredis.commands <- SaveCommand{rspChan}
	return <-rspChan
}

func validateSetRequestData(requestData RespArray) error {
	commandSize := len(requestData.Elements)
	if commandSize != REQUEST_SET_EXPECTED_SIZE && commandSize != REQUEST_SET_WITH_TIMEOUT_EXPECTED_SIZE {
		return errors.New(REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER)
	}
	if commandSize == REQUEST_SET_WITH_TIMEOUT_EXPECTED_SIZE {
		timeoutMode := requestData.Elements[REQUEST_SET_TIMEOUT_MODE_INDEX].(RespString).Str
		switch timeoutMode {
		case REQUEST_SET_TIMEOUT_MODE_EXPIRE_SECONDS:
		case REQUEST_SET_TIMEOUT_MODE_EXPIRE_MILLISECONDS:
		case REQUEST_SET_TIMEOUT_MODE_TIMESTAMP_SECONDS:
		case REQUEST_SET_TIMEOUT_MODE_TIMESTAMP_MILLISECONDS:
			if !isInt64String(requestData.Elements[REQUEST_SET_TIMEOUT_INDEX].(RespString).Str) {
				return errors.New(REQUEST_ERROR_INVALID_TIMEOUT_VALUE)
			}
		default:
			return errors.New(REQUEST_ERROR_UNRECOGNIZED_TIMEOUT_MODE)
		}
	}
	return nil
}

func getSetRequestTimeout(requestData RespArray) time.Time {
	timeoutMode := requestData.Elements[REQUEST_SET_TIMEOUT_MODE_INDEX].(RespString).Str
	timeoutValue, _ := strconv.Atoi(requestData.Elements[REQUEST_SET_TIMEOUT_INDEX].(RespString).Str)
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
		value, err := strconv.ParseInt(respStr.Str, 10, 64)
		if err == nil {
			return RespInt{value}, true
		}
	}

	return RespInt{}, false
}
