package main

type Command interface {
}

type SetCommand struct {
	key        string
	value      RespDataType
	rspChannel chan RespDataType
}

type GetCommand struct {
	key        string
	rspChannel chan RespDataType
}

type ExistsCommand struct {
	key        string
	rspChannel chan RespDataType
}

type DeleteCommand struct {
	key        string
	rspChannel chan RespDataType
}

type IncrementCommand struct {
	key        string
	rspChannel chan RespDataType
}

type DecrementCommand struct {
	key        string
	rspChannel chan RespDataType
}

type LPushCommand struct {
	key        string
	value      RespDataType
	rspChannel chan RespDataType
}

type RPushCommand struct {
	key        string
	value      RespDataType
	rspChannel chan RespDataType
}

type SaveCommand struct {
	rspChannel chan RespDataType
}

type LoadCommand struct {
	rspChannel chan RespDataType
}