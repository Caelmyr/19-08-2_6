package ws

import "errors"

// 定义错误
var (
	ErrSendBufferFull  = errors.New("send buffer full")
	ErrConnectionClosed = errors.New("connection closed")
	ErrDocNotFound     = errors.New("document not found")
	ErrInvalidOp       = errors.New("invalid operation")
	ErrVersionMismatch = errors.New("version mismatch")
)
