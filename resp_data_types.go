package main

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
)

const SERIALIZATION_PREFIX_STRING = "+"
const SERIALIZATION_PREFIX_ERROR = "-"
const SERIALIZATION_PREFIX_INT = ":"
const SERIALIZATION_PREFIX_BULK_STRING = "$"
const SERIALIZATION_PREFIX_ARRAY = "*"

const SERIALIZATION_SEPARATOR = "\r\n"

type RespDataType interface {
	serialize() string
}

type RespString struct {
	str string
}

type RespInt struct {
	value int64
}

type RespError struct {
	str string
}

type RespArray struct {
	elements []RespDataType
}

type RespNil struct {
}

func (respString RespString) serialize() string {
	// We serialize strings always as bulk string to avoid
	// having to maintain 2 structs: one for bulk strings
	// and other for simple strings
	var builder strings.Builder
	builder.WriteString(SERIALIZATION_PREFIX_BULK_STRING)
	builder.WriteString(strconv.Itoa(len(respString.str)))
	builder.WriteString(SERIALIZATION_SEPARATOR)
	builder.WriteString(respString.str)
	builder.WriteString(SERIALIZATION_SEPARATOR)
	return builder.String()
}

func (respInt RespInt) serialize() string {
	var builder strings.Builder
	builder.WriteString(SERIALIZATION_PREFIX_INT)
	builder.WriteString(strconv.FormatInt(respInt.value, 10))
	builder.WriteString(SERIALIZATION_SEPARATOR)
	return builder.String()
}

func (respError RespError) serialize() string {
	var builder strings.Builder
	builder.WriteString(SERIALIZATION_PREFIX_ERROR)
	builder.WriteString(respError.str)
	builder.WriteString(SERIALIZATION_SEPARATOR)
	return builder.String()
}

func (respArray RespArray) serialize() string {
	var builder strings.Builder
	builder.WriteString(SERIALIZATION_PREFIX_ARRAY)
	builder.WriteString(strconv.Itoa(len(respArray.elements)))
	builder.WriteString(SERIALIZATION_SEPARATOR)
	for _, element := range respArray.elements {
		builder.WriteString(element.serialize())
	}
	return builder.String()
}

func (respNil RespNil) serialize() string {
	var builder strings.Builder
	builder.WriteString("$-1")
	builder.WriteString(SERIALIZATION_SEPARATOR)
	return builder.String()
}

func deserialize(data []byte) (RespDataType, int, error) {
	switch string(data[0]) {
	case SERIALIZATION_PREFIX_STRING:
		terminationIndex := bytes.Index(data, []byte(SERIALIZATION_SEPARATOR))
		if terminationIndex == -1 {
			return nil, 0, errors.New("Missing string termination")
		}
		bytesConsumed := terminationIndex + len(SERIALIZATION_SEPARATOR)
		return RespString{string(data[1:terminationIndex])}, bytesConsumed, nil
	case SERIALIZATION_PREFIX_INT:
		terminationIndex := bytes.Index(data, []byte(SERIALIZATION_SEPARATOR))
		if terminationIndex == -1 {
			return nil, 0, errors.New("Missing int termination")
		}
		val, err := strconv.ParseInt(string(data[1:terminationIndex]), 10, 64)
		if err != nil {
			return nil, 0, err
		}
		bytesConsumed := terminationIndex + len(SERIALIZATION_SEPARATOR)
		return RespInt{val}, bytesConsumed, nil
	case SERIALIZATION_PREFIX_ERROR:
		terminationIndex := bytes.Index(data, []byte(SERIALIZATION_SEPARATOR))
		if terminationIndex == -1 {
			return nil, 0, errors.New("Missing error termination")
		}
		bytesConsumed := terminationIndex + len(SERIALIZATION_SEPARATOR)
		return RespError{string(data[1:terminationIndex])}, bytesConsumed, nil
	case SERIALIZATION_PREFIX_BULK_STRING:
		sizeTerminationIndex := bytes.Index(data, []byte(SERIALIZATION_SEPARATOR))
		if sizeTerminationIndex == -1 {
			return nil, 0, errors.New("Missing bulk string size termination")
		}
		strSize, err := strconv.Atoi(string(data[1:sizeTerminationIndex]))
		if err != nil {
			return nil, 0, err
		}
		strInitialPos := sizeTerminationIndex + len(SERIALIZATION_SEPARATOR)
		bytesConsumed := sizeTerminationIndex + 2*len(SERIALIZATION_SEPARATOR) + strSize
		return RespString{string(data[strInitialPos : strInitialPos+strSize])}, bytesConsumed, nil
	case SERIALIZATION_PREFIX_ARRAY:
		sizeTerminationIndex := bytes.Index(data, []byte(SERIALIZATION_SEPARATOR))
		if sizeTerminationIndex == -1 {
			return nil, 0, errors.New("Missing array size termination")
		}
		arraySize, err := strconv.Atoi(string(data[1:sizeTerminationIndex]))
		if err != nil {
			return nil, 0, err
		}
		var elements []RespDataType
		totalBytesConsumed := sizeTerminationIndex + len(SERIALIZATION_SEPARATOR)
		nextElemInitialPos := sizeTerminationIndex + len(SERIALIZATION_SEPARATOR)
		for range arraySize {
			element, bytesConsumed, err := deserialize(data[nextElemInitialPos:])
			if err != nil {
				return nil, 0, err
			}
			elements = append(elements, element)
			totalBytesConsumed += bytesConsumed
			nextElemInitialPos += bytesConsumed
		}
		return RespArray{elements}, totalBytesConsumed, nil
	default:
		return nil, 0, errors.New("Unrecognized data type")
	}
}
