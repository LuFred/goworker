package common

import (
	"context"
	"errors"
	"fmt"
	"os"
)

var (
	ErrRunFuncNotFound             = errors.New("worker: workFunc not found")
	ErrAttemptExhausted            = errors.New("worker: the number of attempts is exhausted")
	ErrCtxCanceled                 = errors.New("worker: context canceled")
	ErrCtxDeadlineExceeded         = errors.New("worker: context deadlineExceeded")
	ErrHandlerMostBePtr            = errors.New("worker：handler must be a pointer type")
	ErrInvalidCron                 = errors.New("worker：invalid cronSchedule")
	ErrClusterRegister             = errors.New("worker：cluster register exception")
	ErrClusterUnRegister           = errors.New("worker：cluster unregister exception")
	ErrClusterBuild                = errors.New("worker：cluster build exception")
	ErrClusterRegisterInfoNotFound = errors.New("worker：cluster register info not found")
)

var (
	// ErrTryAgain 表示函数立即重试
	ErrTryAgain = errors.New("worker: [not error] func rey again")
)

func ContextErr(err error) error {
	switch err {
	case context.DeadlineExceeded:
		return ErrCtxDeadlineExceeded
	case context.Canceled:
		return ErrCtxCanceled
	}
	return fmt.Errorf("worker: UnKnown error from context packet: %v", err)
}

type ErrorType uint64

const (
	ErrorTypeAny ErrorType = iota + 1
	ErrorTypeCtx
)

type WorkerError struct {
	Inner   interface{}
	Type    ErrorType
	Message string
	Stack   string
	Data    interface{}
}

func (e WorkerError) Error() string {
	if e.Stack != "" {
		return fmt.Sprintf("msg: %s;\nstack: %s", e.Message, e.Stack)
	}

	return e.Message
}

func (e *WorkerError) IsType(t ErrorType) bool {
	return e.Type == t
}

func (e *WorkerError) SetData(data interface{}) {
	e.Data = data
}

func (e *WorkerError) SetStack(stack string) {
	e.Stack = stack
}

func WithErrorMessage(err error, msgFmt string, args ...interface{}) error {
	return &WorkerError{
		Inner:   err,
		Type:    ErrorTypeAny,
		Message: fmt.Sprintf(msgFmt, args...),
	}
}

func WrapError(err interface{}, typ ErrorType, messagef string, msgArgs ...interface{}) WorkerError {
	return WorkerError{
		Inner:   err,
		Type:    typ,
		Message: fmt.Sprintf(messagef, msgArgs...),
	}
}

// linux保留退出代码
//
//http://tldp.org/LDP/abs/html/exitcodes.html
const (
	ExitSuccess = iota
	ExitError
	ExitBadArgs = 128
)

func ExitWithError(code int, err error) {
	fmt.Fprintln(os.Stderr, "worker: error:", err)
	os.Exit(code)
}
