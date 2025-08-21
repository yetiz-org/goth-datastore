package datastore

import (
	"github.com/yetiz-org/goth-secret"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DatabaseOperator defines the interface for database operations.
// This interface allows for both real and mock implementations,
// enabling comprehensive unit testing while maintaining API compatibility.
type DatabaseOperator interface {
	// Core database access
	DB() *gorm.DB
	Adapter() string

	// Configuration access
	GetConnParams() ConnParams
	GetMysqlParams() MysqlParams
	GetGORMParams() gorm.Config
	GetLogger() logger.Interface
	Meta() secret.DatabaseMeta

	// Configuration setters
	SetConnParams(params ConnParams)
	SetMysqlParams(params MysqlParams)
	SetGORMParams(config gorm.Config)
	SetLogger(logger logger.Interface)
}

// DatabaseProvider defines the interface for Database instances.
// This allows both real and mock Database implementations.
type DatabaseProvider interface {
	Writer() DatabaseOperator
	Reader() DatabaseOperator
}
