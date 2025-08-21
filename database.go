package datastore

import (
	"fmt"
	"sync"
	"time"

	kklogger "github.com/yetiz-org/goth-kklogger"
	"github.com/yetiz-org/goth-secret"
	"gorm.io/driver/mysql"
	"gorm.io/gorm/logger"

	"gorm.io/gorm"
)

var DefaultDatabaseMaxOpenConn = 4
var DefaultDatabaseMaxIdleConn = 2
var DefaultDatabaseConnMaxLifetime = 20000
var DefaultDatabaseConnMaxIdleTime = 0

/* Params ref: https://gorm.io/docs/connecting_to_the_database.html */
var DefaultDatabaseCharset = ""
var DefaultDatabaseDialTimeout = "3s"
var DefaultDatabaseReadTimeout = "30s"
var DefaultDatabaseWriteTimeout = "30s"
var DefaultDatabaseCollation = "utf8mb4_general_ci"
var DefaultDatabaseClientFoundRows = false
var DefaultDatabaseLoc = "Local"
var DefaultDatabaseMaxAllowedPacket = 25165824
var DefaultDatabaseParseTime = true

type Database struct {
	writer DatabaseOperator
	reader DatabaseOperator
}

func (k *Database) Writer() DatabaseOperator {
	return k.writer
}

func (k *Database) Reader() DatabaseOperator {
	return k.reader
}

type DatabaseOp struct {
	meta        secret.DatabaseMeta
	db          *gorm.DB
	opLock      sync.Mutex
	ConnParams  ConnParams
	MysqlParams MysqlParams
	GORMParams  gorm.Config
	Logger      logger.Interface
}

type MysqlParams struct {
	DriverName                    string
	ServerVersion                 string
	SkipInitializeWithVersion     bool
	DefaultStringSize             uint
	DefaultDatetimePrecision      *int
	DisableWithReturning          bool
	DisableDatetimePrecision      bool
	DontSupportRenameIndex        bool
	DontSupportRenameColumn       bool
	DontSupportForShareClause     bool
	DontSupportNullAsDefaultValue bool
	DontSupportRenameColumnUnique bool
	DontSupportDropConstraint     bool
}

type ConnParams struct {
	Charset          string
	Timeout          string
	ReadTimeout      string
	WriteTimeout     string
	Collation        string
	Loc              string
	ClientFoundRows  bool
	ParseTime        bool
	MaxAllowedPacket int
	MaxOpenConn      int
	MaxIdleConn      int
	ConnMaxLifetime  int
	ConnMaxIdleTime  int
}

func (o *DatabaseOp) DB() *gorm.DB {
	if o.db != nil {
		return o.db
	}

	o.opLock.Lock()
	defer o.opLock.Unlock()
	if o.db == nil {
		if o.db = newDBPool(o, 0); o.db == nil {
			kklogger.ErrorJ("datastore:DatabaseOp.DB", "database pool create failed")
			return nil
		}
	}

	return o.db
}

func (o *DatabaseOp) Adapter() string {
	return o.meta.Adapter
}

// GetConnParams returns the current connection parameters
func (o *DatabaseOp) GetConnParams() ConnParams {
	return o.ConnParams
}

// GetMysqlParams returns the current MySQL-specific parameters
func (o *DatabaseOp) GetMysqlParams() MysqlParams {
	return o.MysqlParams
}

// GetGORMParams returns the current GORM configuration
func (o *DatabaseOp) GetGORMParams() gorm.Config {
	return o.GORMParams
}

// GetLogger returns the current logger interface
func (o *DatabaseOp) GetLogger() logger.Interface {
	return o.Logger
}

// Meta returns the database metadata
func (o *DatabaseOp) Meta() secret.DatabaseMeta {
	return o.meta
}

// SetConnParams sets the connection parameters
func (o *DatabaseOp) SetConnParams(params ConnParams) {
	o.ConnParams = params
}

// SetMysqlParams sets the MySQL-specific parameters
func (o *DatabaseOp) SetMysqlParams(params MysqlParams) {
	o.MysqlParams = params
}

// SetGORMParams sets the GORM configuration
func (o *DatabaseOp) SetGORMParams(config gorm.Config) {
	o.GORMParams = config
}

// SetLogger sets the logger interface
func (o *DatabaseOp) SetLogger(logger logger.Interface) {
	o.Logger = logger
}

func NewDatabase(profileName string) *Database {
	profile := &secret.Database{}
	if err := secret.Load("database", profileName, profile); err != nil {
		kklogger.ErrorJ("datastore.database#Load", err.Error())
		return nil
	}

	database := new(Database)
	if profile.Writer.Adapter != "" {
		database.writer = &DatabaseOp{
			ConnParams: ConnParams{
				Charset:          DefaultDatabaseCharset,
				Timeout:          DefaultDatabaseDialTimeout,
				ReadTimeout:      DefaultDatabaseReadTimeout,
				WriteTimeout:     DefaultDatabaseWriteTimeout,
				Collation:        DefaultDatabaseCollation,
				Loc:              DefaultDatabaseLoc,
				ClientFoundRows:  DefaultDatabaseClientFoundRows,
				ParseTime:        DefaultDatabaseParseTime,
				MaxAllowedPacket: DefaultDatabaseMaxAllowedPacket,
				MaxOpenConn:      DefaultDatabaseMaxOpenConn,
				MaxIdleConn:      DefaultDatabaseMaxIdleConn,
				ConnMaxLifetime:  DefaultDatabaseConnMaxLifetime,
				ConnMaxIdleTime:  DefaultDatabaseConnMaxIdleTime,
			},
			meta: profile.Writer,
		}
	}

	if profile.Reader.Adapter != "" {
		database.reader = &DatabaseOp{
			ConnParams: ConnParams{
				Charset:          DefaultDatabaseCharset,
				Timeout:          DefaultDatabaseDialTimeout,
				ReadTimeout:      DefaultDatabaseReadTimeout,
				WriteTimeout:     DefaultDatabaseWriteTimeout,
				Collation:        DefaultDatabaseCollation,
				Loc:              DefaultDatabaseLoc,
				ClientFoundRows:  DefaultDatabaseClientFoundRows,
				ParseTime:        DefaultDatabaseParseTime,
				MaxAllowedPacket: DefaultDatabaseMaxAllowedPacket,
				MaxOpenConn:      DefaultDatabaseMaxOpenConn,
				MaxIdleConn:      DefaultDatabaseMaxIdleConn,
				ConnMaxLifetime:  DefaultDatabaseConnMaxLifetime,
				ConnMaxIdleTime:  DefaultDatabaseConnMaxIdleTime,
			},
			meta: profile.Reader,
		}
	}

	return database
}

func newDBPool(op *DatabaseOp, retry int) *gorm.DB {
	// Add nil check for op parameter to prevent panic
	if op == nil {
		kklogger.ErrorJ("datastore:Database.newDBPool", "DatabaseOp parameter is nil")
		return nil
	}

	var db *gorm.DB
	var err error
	charset := func() string {
		if op.ConnParams.Charset == "" {
			return op.meta.Params.Charset
		}

		return op.ConnParams.Charset
	}()

	switch op.meta.Adapter {
	case "mysql":
		db, err = gorm.Open(mysql.New(mysql.Config{
			DSN: fmt.Sprintf("%s:%s@(%s:%d)/%s?"+
				"charset=%s"+
				"&timeout=%s"+
				"&readTimeout=%s"+
				"&writeTimeout=%s"+
				"&collation=%s"+
				"&loc=%s"+
				"&clientFoundRows=%v"+
				"&parseTime=%v"+
				"&maxAllowedPacket=%d",
				op.meta.Params.Username,
				op.meta.Params.Password,
				op.meta.Params.Host,
				op.meta.Params.Port,
				op.meta.Params.DBName,
				charset,
				op.ConnParams.Timeout,
				op.ConnParams.ReadTimeout,
				op.ConnParams.WriteTimeout,
				op.ConnParams.Collation,
				op.ConnParams.Loc,
				op.ConnParams.ClientFoundRows,
				op.ConnParams.ParseTime,
				op.ConnParams.MaxAllowedPacket,
			),
			DriverName:                    op.MysqlParams.DriverName,
			ServerVersion:                 op.MysqlParams.ServerVersion,
			SkipInitializeWithVersion:     op.MysqlParams.SkipInitializeWithVersion,
			DefaultStringSize:             op.MysqlParams.DefaultStringSize,
			DefaultDatetimePrecision:      op.MysqlParams.DefaultDatetimePrecision,
			DisableWithReturning:          op.MysqlParams.DisableWithReturning,
			DisableDatetimePrecision:      op.MysqlParams.DisableDatetimePrecision,
			DontSupportRenameIndex:        op.MysqlParams.DontSupportRenameIndex,
			DontSupportRenameColumn:       op.MysqlParams.DontSupportRenameColumn,
			DontSupportForShareClause:     op.MysqlParams.DontSupportForShareClause,
			DontSupportNullAsDefaultValue: op.MysqlParams.DontSupportNullAsDefaultValue,
			DontSupportRenameColumnUnique: op.MysqlParams.DontSupportRenameColumnUnique,
			DontSupportDropConstraint:     op.MysqlParams.DontSupportDropConstraint,
		}), &op.GORMParams)
	default:
		kklogger.ErrorJ("datastore:Database.newDBPool", "database adapter not support")
		return nil
	}

	if err != nil {
		kklogger.ErrorJ("datastore:Database.newDBPool", err.Error())
		fmt.Println(err.Error())
		retry += 1
		if retry >= 5 {
			kklogger.ErrorJ("datastore:Database.newDBPool", "database retry too many times(> 5)")
			fmt.Println("database retry too many times(> 5)")
			return nil
		}

		time.Sleep(time.Second * 1)
		return newDBPool(op, retry)
	}

	if sqlDb, err := db.DB(); err != nil {
		kklogger.ErrorJ("datastore:Database.newDBPool", err.Error())
		fmt.Println(err.Error())
		return nil
	} else {
		sqlDb.SetMaxOpenConns(op.ConnParams.MaxOpenConn)
		sqlDb.SetMaxIdleConns(op.ConnParams.MaxIdleConn)
		sqlDb.SetConnMaxLifetime(time.Millisecond * time.Duration(op.ConnParams.ConnMaxLifetime))
		sqlDb.SetConnMaxIdleTime(time.Millisecond * time.Duration(op.ConnParams.ConnMaxIdleTime))
	}

	if op.Logger != nil {
		db.Logger = op.Logger
	}

	return db
}
