package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicRespStringSerializer(t *testing.T) {
	respStr := RespString{"hello world"}
	expectedSerializedStr := "$11\r\nhello world\r\n"
	actualSerializedStr := respStr.serialize()
	assert.Equal(t, expectedSerializedStr, actualSerializedStr)
}

func TestEmptyRespStringSerializer(t *testing.T) {
	respStr := RespString{""}
	expectedSerializedStr := "$0\r\n\r\n"
	actualSerializedStr := respStr.serialize()
	assert.Equal(t, expectedSerializedStr, actualSerializedStr)
}

func TestRespIntSerializer(t *testing.T) {
	respInt := RespInt{10}
	expectedSerializedInt := ":10\r\n"
	actualSerializedInt := respInt.serialize()
	assert.Equal(t, expectedSerializedInt, actualSerializedInt)
}

func TestBasicRespErrorSerializer(t *testing.T) {
	respError := RespError{"errorXX"}
	expectedSerializedError := "-errorXX\r\n"
	actualSerializedError := respError.serialize()
	assert.Equal(t, expectedSerializedError, actualSerializedError)
}

func TestEmptyRespErrorSerializer(t *testing.T) {
	respError := RespError{""}
	expectedSerializedError := "-\r\n"
	actualSerializedError := respError.serialize()
	assert.Equal(t, expectedSerializedError, actualSerializedError)
}

func TestEmptyRespArraySerializer(t *testing.T) {
	respArray := RespArray{}
	expectedSerializedArray := "*0\r\n"
	actualSerializedArray := respArray.serialize()
	assert.Equal(t, expectedSerializedArray, actualSerializedArray)
}

func TestStringRespArraySerializer(t *testing.T) {
	var respStrs []RespDataType
	respStrs = append(respStrs, RespString{"bla"})
	respStrs = append(respStrs, RespString{"blo"})
	respStrs = append(respStrs, RespString{"bli"})
	respArray := RespArray{respStrs}
	expectedSerializedArray := "*3\r\n$3\r\nbla\r\n$3\r\nblo\r\n$3\r\nbli\r\n"
	actualSerializedArray := respArray.serialize()
	assert.Equal(t, expectedSerializedArray, actualSerializedArray)
}

func TestBasicStringDeserialization(t *testing.T) {
	serializedData := []byte("+hello world\r\n")
	dataType, bytesConsumed, err := deserialize(serializedData)
	assert.Nil(t, err)
	respString, ok := dataType.(RespString)
	assert.Equal(t, true, ok)
	assert.Equal(t, "hello world", respString.str)
	assert.Equal(t, 14, bytesConsumed)
}

func TestNonTerminatedStringDeserialization(t *testing.T) {
	serializedData := []byte("+hello world")
	_, _, err := deserialize(serializedData)
	assert.NotNil(t, err)
}

func TestBasicIntDeserialization(t *testing.T) {
	serializedData := []byte(":101\r\n")
	dataType, bytesConsumed, err := deserialize(serializedData)
	assert.Nil(t, err)
	respInt, ok := dataType.(RespInt)
	assert.Equal(t, true, ok)
	assert.Equal(t, int64(101), respInt.value)
	assert.Equal(t, 6, bytesConsumed)
}

func TestNonNumericIntDeserialization(t *testing.T) {
	serializedData := []byte(":101x\r\n")
	_, _, err := deserialize(serializedData)
	assert.NotNil(t, err)
}

func TestNonTerminatedIntDeserialization(t *testing.T) {
	serializedData := []byte(":101")
	_, _, err := deserialize(serializedData)
	assert.NotNil(t, err)
}

func TestBasicErrorDeserialization(t *testing.T) {
	serializedData := []byte("-error occurred\r\n")
	dataType, bytesConsumed, err := deserialize(serializedData)
	assert.Nil(t, err)
	respError, ok := dataType.(RespError)
	assert.Equal(t, true, ok)
	assert.Equal(t, "error occurred", respError.str)
	assert.Equal(t, 17, bytesConsumed)
}

func TestNonTerminatedErrorDeserialization(t *testing.T) {
	serializedData := []byte("-error occurred")
	_, _, err := deserialize(serializedData)
	assert.NotNil(t, err)
}

func TestBasicBulkStringDeserialization(t *testing.T) {
	serializedData := []byte("$16\r\nhello world bulk\r\n")
	dataType, bytesConsumed, err := deserialize(serializedData)
	assert.Nil(t, err)
	respString, ok := dataType.(RespString)
	assert.Equal(t, true, ok)
	assert.Equal(t, "hello world bulk", respString.str)
	assert.Equal(t, 23, bytesConsumed)
}

func TestBasicBulkStringArrayDeserialization(t *testing.T) {
	serializedData := []byte("*3\r\n$3\r\nbla\r\n$3\r\nblo\r\n$3\r\nbli\r\n")
	dataType, bytesConsumed, err := deserialize(serializedData)
	assert.Nil(t, err)
	respArray, ok := dataType.(RespArray)
	assert.Equal(t, true, ok)
	respString1, ok := respArray.elements[0].(RespString)
	respString2, ok := respArray.elements[1].(RespString)
	respString3, ok := respArray.elements[2].(RespString)
	assert.Equal(t, 3, len(respArray.elements))
	assert.Equal(t, "bla", respString1.str)
	assert.Equal(t, "blo", respString2.str)
	assert.Equal(t, "bli", respString3.str)
	assert.Equal(t, 31, bytesConsumed)
}

func TestMixedDataTypesArrayDeserialization(t *testing.T) {
	serializedData := []byte("*5\r\n$3\r\nbla\r\n:2025\r\n+bli\r\n-err\r\n*2\r\n$3\r\nbla\r\n$3\r\nblo\r\n")
	dataType, bytesConsumed, err := deserialize(serializedData)
	assert.Nil(t, err)
	respArray, ok := dataType.(RespArray)
	assert.Equal(t, true, ok)
	respString1, ok := respArray.elements[0].(RespString)
	respInt, ok := respArray.elements[1].(RespInt)
	respString2, ok := respArray.elements[2].(RespString)
	respError, ok := respArray.elements[3].(RespError)
	respSubArray, ok := respArray.elements[4].(RespArray)
	assert.Equal(t, 5, len(respArray.elements))
	assert.Equal(t, "bla", respString1.str)
	assert.Equal(t, int64(2025), respInt.value)
	assert.Equal(t, "bli", respString2.str)
	assert.Equal(t, "err", respError.str)
	assert.Equal(t, "bla", respSubArray.elements[0].(RespString).str)
	assert.Equal(t, "blo", respSubArray.elements[1].(RespString).str)
	assert.Equal(t, 54, bytesConsumed)
}
