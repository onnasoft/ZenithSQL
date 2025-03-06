package sqlparser

import "fmt"

type SQLError struct {
	Code    int
	Message string
}

func (e *SQLError) Error() string {
	return fmt.Sprintf("SQLError [Code %d]: %s", e.Code, e.Message)
}

const (
	ErrCodeInvalidCreateTableFormat  = 1001
	ErrCodeInvalidColumnFormat       = 1002
	ErrCodeInvalidDataType           = 1003
	ErrCodeInvalidStorageOption      = 1004
	ErrCodeInvalidPrimaryKeyType     = 1005
	ErrCodeInvalidMessagePackData    = 1006
	InvalidCreateDatabaseFormatError = 1007
	InvalidDatabaseNameError         = 1008
)

func NewInvalidDatabaseNameError(sql string) error {
	return &SQLError{
		Code:    InvalidDatabaseNameError,
		Message: fmt.Sprintf("invalid database name: %s", sql),
	}
}

func NewInvalidCreateDatabaseFormatError() error {
	return &SQLError{
		Code:    InvalidCreateDatabaseFormatError,
		Message: "invalid CREATE DATABASE format",
	}
}

func NewInvalidCreateTableFormatError() error {
	return &SQLError{
		Code:    ErrCodeInvalidCreateTableFormat,
		Message: "invalid CREATE TABLE format",
	}
}

func NewInvalidColumnFormatError() error {
	return &SQLError{
		Code:    ErrCodeInvalidColumnFormat,
		Message: "invalid column format",
	}
}

func NewInvalidDataTypeError(dataType string) error {
	return &SQLError{
		Code:    ErrCodeInvalidDataType,
		Message: fmt.Sprintf("invalid data type: %s", dataType),
	}
}

func NewInvalidStorageOptionError(storage string) error {
	return &SQLError{
		Code:    ErrCodeInvalidStorageOption,
		Message: fmt.Sprintf("invalid storage option: %s", storage),
	}
}

func NewInvalidPrimaryKeyTypeError() error {
	return &SQLError{
		Code:    ErrCodeInvalidPrimaryKeyType,
		Message: "invalid primary key type",
	}
}

func NewInvalidMessagePackDataError() error {
	return &SQLError{
		Code:    ErrCodeInvalidMessagePackData,
		Message: "invalid MessagePack data",
	}
}
