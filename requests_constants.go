package main

const REQUEST_PING = "PING"
const REQUEST_ECHO = "ECHO"
const REQUEST_GET = "GET"
const REQUEST_SET = "SET"
const REQUEST_EXISTS = "EXISTS"
const REQUEST_DELETE = "DEL"
const REQUEST_INCREMENT = "INCR"
const REQUEST_DECREMENT = "DECR"
const REQUEST_LPUSH = "LPUSH"
const REQUEST_RPUSH = "RPUSH"
const REQUEST_SAVE = "SAVE"

const REQUEST_PING_EXPECTED_SIZE = 1
const REQUEST_ECHO_EXPECTED_SIZE = 2
const REQUEST_GET_EXPECTED_SIZE = 2
const REQUEST_SET_EXPECTED_SIZE = 3
const REQUEST_SET_WITH_TIMEOUT_EXPECTED_SIZE = 5
const REQUEST_EXISTS_EXPECTED_SIZE = 2
const REQUEST_DELETE_EXPECTED_SIZE = 2
const REQUEST_INCREMENT_EXPECTED_SIZE = 2
const REQUEST_DECREMENT_EXPECTED_SIZE = 2
const REQUEST_LPUSH_EXPECTED_SIZE = 3
const REQUEST_RPUSH_EXPECTED_SIZE = 3
const REQUEST_SAVE_EXPECTED_SIZE = 1

const REQUEST_INDEX = 0
const REQUEST_ECHO_VALUE = 1
const REQUEST_GET_KEY_INDEX = 1
const REQUEST_SET_KEY_INDEX = 1
const REQUEST_SET_VALUE_INDEX = 2
const REQUEST_SET_TIMEOUT_MODE_INDEX = 3
const REQUEST_SET_TIMEOUT_INDEX = 4
const REQUEST_EXISTS_KEY_INDEX = 1
const REQUEST_DELETE_KEY_INDEX = 1
const REQUEST_INCREMENT_KEY_INDEX = 1
const REQUEST_DECREMENT_KEY_INDEX = 1
const REQUEST_LPUSH_KEY_INDEX = 1
const REQUEST_LPUSH_VALUE_INDEX = 2
const REQUEST_RPUSH_KEY_INDEX = 1
const REQUEST_RPUSH_VALUE_INDEX = 2

const EXPIRATION_MODE_EXPIRE_SECONDS = "EX"
const EXPIRATION_MODE_EXPIRE_MILLISECONDS = "PX"
const EXPIRATION_MODE_TIMESTAMP_SECONDS = "EXAT"
const EXPIRATION_MODE_TIMESTAMP_MILLISECONDS = "PXAT"

const REQUEST_PING_RSP = "PONG"
const REQUEST_RESULT_OK = "OK"
const REQUEST_RESULT_FAIL = "FAILED"
const REQUEST_ERROR_FAILED_DESERIALIZATION = "ERR FAILED-DESERIALIZING"
const REQUEST_ERROR_UNEXPECTED_ARG_TYPE = "ERR UNEXPECTED-ARGUMENT-TYPE"
const REQUEST_ERROR_INVALID_ARGUMENTS_NUMBER = "ERR INVALID-ARGUMENTS-NUMBER"
const REQUEST_ERROR_INVALID_COMMAND = "ERR INVALID-COMMAND"
const REQUEST_ERROR_UNRECOGNIZED_TIMEOUT_MODE = "ERR UNRECOGNIZED-TIMEOUT-MODE"
const REQUEST_ERROR_INVALID_TIMEOUT_VALUE = "ERR INVALID-TIMEOUT-VALUE"
const REQUEST_ERROR_VALUE_NOT_NUMERIC_OR_MAX_REACHED = "ERR VALUE-NOT-NUMERIC-OR-MAX-REACHED"
const REQUEST_ERROR_VALUE_NOT_A_LIST = "ERR VALUE-NOT-A-LIST"
