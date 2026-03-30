package datastore

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	secret "github.com/yetiz-org/goth-datastore/secrets"
	kklogger "github.com/yetiz-org/goth-kklogger"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
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
var DefaultDatabaseMultiStatements = false
var DefaultDatabaseTransactionIsolation DatabaseIsolationLevel = ""

var DefaultDatabasePostgresSSLMode = "disable"
var DefaultDatabasePostgresTimeZone = "Local"

func init() {
	envInt("GOTH_DEFAULT_DATABASE_MAX_OPEN_CONN", &DefaultDatabaseMaxOpenConn)
	envInt("GOTH_DEFAULT_DATABASE_MAX_IDLE_CONN", &DefaultDatabaseMaxIdleConn)
	envInt("GOTH_DEFAULT_DATABASE_CONN_MAX_LIFETIME", &DefaultDatabaseConnMaxLifetime)
	envInt("GOTH_DEFAULT_DATABASE_CONN_MAX_IDLE_TIME", &DefaultDatabaseConnMaxIdleTime)
	envStr("GOTH_DEFAULT_DATABASE_CHARSET", &DefaultDatabaseCharset)
	envStr("GOTH_DEFAULT_DATABASE_DIAL_TIMEOUT", &DefaultDatabaseDialTimeout)
	envStr("GOTH_DEFAULT_DATABASE_READ_TIMEOUT", &DefaultDatabaseReadTimeout)
	envStr("GOTH_DEFAULT_DATABASE_WRITE_TIMEOUT", &DefaultDatabaseWriteTimeout)
	envStr("GOTH_DEFAULT_DATABASE_COLLATION", &DefaultDatabaseCollation)
	envBool("GOTH_DEFAULT_DATABASE_CLIENT_FOUND_ROWS", &DefaultDatabaseClientFoundRows)
	envStr("GOTH_DEFAULT_DATABASE_LOC", &DefaultDatabaseLoc)
	envInt("GOTH_DEFAULT_DATABASE_MAX_ALLOWED_PACKET", &DefaultDatabaseMaxAllowedPacket)
	envBool("GOTH_DEFAULT_DATABASE_PARSE_TIME", &DefaultDatabaseParseTime)
	envBool("GOTH_DEFAULT_DATABASE_MULTI_STATEMENTS", &DefaultDatabaseMultiStatements)
	envStr("GOTH_DEFAULT_DATABASE_TRANSACTION_ISOLATION", &DefaultDatabaseTransactionIsolation)
	envStr("GOTH_DEFAULT_DATABASE_POSTGRES_SSL_MODE", &DefaultDatabasePostgresSSLMode)
	envStr("GOTH_DEFAULT_DATABASE_POSTGRES_TIME_ZONE", &DefaultDatabasePostgresTimeZone)
}

// DatabaseIsolationLevel represents a SQL transaction isolation level.
// Use the predefined constants for type safety; the zero value (empty string)
// means "use database default" and will not be appended to the DSN.
type DatabaseIsolationLevel string

const (
	DatabaseIsolationLevelReadUncommitted DatabaseIsolationLevel = "ReadUncommitted"
	DatabaseIsolationLevelReadCommitted   DatabaseIsolationLevel = "ReadCommitted"
	DatabaseIsolationLevelRepeatableRead  DatabaseIsolationLevel = "RepeatableRead"
	DatabaseIsolationLevelSerializable    DatabaseIsolationLevel = "Serializable"
)

func (l DatabaseIsolationLevel) mysqlValue() string {
	switch l {
	case DatabaseIsolationLevelReadUncommitted:
		return "'READ-UNCOMMITTED'"
	case DatabaseIsolationLevelReadCommitted:
		return "'READ-COMMITTED'"
	case DatabaseIsolationLevelRepeatableRead:
		return "'REPEATABLE-READ'"
	case DatabaseIsolationLevelSerializable:
		return "'SERIALIZABLE'"
	default:
		return ""
	}
}

func (l DatabaseIsolationLevel) postgresValue() string {
	switch l {
	case DatabaseIsolationLevelReadUncommitted:
		return "'read uncommitted'"
	case DatabaseIsolationLevelReadCommitted:
		return "'read committed'"
	case DatabaseIsolationLevelRepeatableRead:
		return "'repeatable read'"
	case DatabaseIsolationLevelSerializable:
		return "'serializable'"
	default:
		return ""
	}
}

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
	opLock      sync.RWMutex
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
	MultiStatements  bool
	MaxAllowedPacket int
	MaxOpenConn      int
	MaxIdleConn      int
	ConnMaxLifetime  int
	ConnMaxIdleTime  int
	SSLMode          string
	TimeZone         string

	// TransactionIsolation sets the default transaction isolation level.
	// The zero value (empty string) means "use database default" and is not
	// appended to the DSN. Use the DatabaseIsolationLevel* constants.
	// The value is automatically formatted per database adapter:
	//   MySQL:      &transaction_isolation='READ-COMMITTED'
	//   PostgreSQL: default_transaction_isolation='read committed'
	TransactionIsolation DatabaseIsolationLevel

	// ExtraParams holds additional DSN parameters as key-value pairs.
	// These are appended to the generated DSN string for any database adapter,
	// after all typed fields (including TransactionIsolation).
	//
	// For MySQL, entries are appended as &key=value. Any key not recognized
	// by the driver is sent as a system variable via SET on connection.
	//   Example: {"autocommit": "1", "sql_mode": "'STRICT_TRANS_TABLES'"}
	//
	// For PostgreSQL, entries are appended as key=value (space-separated).
	// Values containing spaces must be single-quoted.
	//   Example: {"application_name": "myapp", "statement_timeout": "30000"}
	ExtraParams map[string]string
}

func (o *DatabaseOp) DB() *gorm.DB {
	o.opLock.RLock()
	db := o.db
	o.opLock.RUnlock()
	if db != nil {
		return db
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
				Charset:              DefaultDatabaseCharset,
				Timeout:              DefaultDatabaseDialTimeout,
				ReadTimeout:          DefaultDatabaseReadTimeout,
				WriteTimeout:         DefaultDatabaseWriteTimeout,
				Collation:            DefaultDatabaseCollation,
				Loc:                  DefaultDatabaseLoc,
				ClientFoundRows:      DefaultDatabaseClientFoundRows,
				ParseTime:            DefaultDatabaseParseTime,
				MultiStatements:      DefaultDatabaseMultiStatements,
				MaxAllowedPacket:     DefaultDatabaseMaxAllowedPacket,
				MaxOpenConn:          DefaultDatabaseMaxOpenConn,
				MaxIdleConn:          DefaultDatabaseMaxIdleConn,
				ConnMaxLifetime:      DefaultDatabaseConnMaxLifetime,
				ConnMaxIdleTime:      DefaultDatabaseConnMaxIdleTime,
				TransactionIsolation: DefaultDatabaseTransactionIsolation,
				SSLMode:              DefaultDatabasePostgresSSLMode,
				TimeZone:             DefaultDatabasePostgresTimeZone,
			},
			meta: profile.Writer,
		}
	}

	if profile.Reader.Adapter != "" {
		database.reader = &DatabaseOp{
			ConnParams: ConnParams{
				Charset:              DefaultDatabaseCharset,
				Timeout:              DefaultDatabaseDialTimeout,
				ReadTimeout:          DefaultDatabaseReadTimeout,
				WriteTimeout:         DefaultDatabaseWriteTimeout,
				Collation:            DefaultDatabaseCollation,
				Loc:                  DefaultDatabaseLoc,
				ClientFoundRows:      DefaultDatabaseClientFoundRows,
				ParseTime:            DefaultDatabaseParseTime,
				MultiStatements:      DefaultDatabaseMultiStatements,
				MaxAllowedPacket:     DefaultDatabaseMaxAllowedPacket,
				MaxOpenConn:          DefaultDatabaseMaxOpenConn,
				MaxIdleConn:          DefaultDatabaseMaxIdleConn,
				ConnMaxLifetime:      DefaultDatabaseConnMaxLifetime,
				ConnMaxIdleTime:      DefaultDatabaseConnMaxIdleTime,
				TransactionIsolation: DefaultDatabaseTransactionIsolation,
				SSLMode:              DefaultDatabasePostgresSSLMode,
				TimeZone:             DefaultDatabasePostgresTimeZone,
			},
			meta: profile.Reader,
		}
	}

	return database
}

func buildMysqlDSN(username, password, host string, port uint, dbName, charset string, params ConnParams) string {
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?"+
		"charset=%s"+
		"&timeout=%s"+
		"&readTimeout=%s"+
		"&writeTimeout=%s"+
		"&collation=%s"+
		"&loc=%s"+
		"&clientFoundRows=%v"+
		"&parseTime=%v"+
		"&maxAllowedPacket=%d"+
		"&multiStatements=%v",
		username,
		password,
		host,
		port,
		dbName,
		charset,
		params.Timeout,
		params.ReadTimeout,
		params.WriteTimeout,
		params.Collation,
		params.Loc,
		params.ClientFoundRows,
		params.ParseTime,
		params.MaxAllowedPacket,
		params.MultiStatements,
	)

	if v := params.TransactionIsolation.mysqlValue(); v != "" {
		dsn += "&transaction_isolation=" + v
	}
	dsn += buildExtraParamsMysql(params.ExtraParams)
	return dsn
}

func buildExtraParamsMysql(extra map[string]string) string {
	if len(extra) == 0 {
		return ""
	}

	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		b.WriteString("&")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(extra[k])
	}
	return b.String()
}

func buildPostgresDSN(host, username, password, dbName string, port uint, sslMode, timeZone string, isolation DatabaseIsolationLevel, extraParams map[string]string) string {
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=%s TimeZone=%s",
		host,
		username,
		password,
		dbName,
		port,
		sslMode,
		timeZone,
	)

	if v := isolation.postgresValue(); v != "" {
		dsn += " default_transaction_isolation=" + v
	}
	dsn += buildExtraParamsPostgres(extraParams)
	return dsn
}

func buildExtraParamsPostgres(extra map[string]string) string {
	if len(extra) == 0 {
		return ""
	}

	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		b.WriteString(" ")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(extra[k])
	}
	return b.String()
}

func buildPostgresDialectorConfig(meta secret.DatabaseMeta, params ConnParams, sslMode, timeZone string) postgres.Config {
	return postgres.Config{
		DSN: buildPostgresDSN(
			meta.Params.Host,
			meta.Params.Username,
			meta.Params.Password,
			meta.Params.DBName,
			meta.Params.Port,
			sslMode,
			timeZone,
			params.TransactionIsolation,
			params.ExtraParams,
		),
		PreferSimpleProtocol: params.MultiStatements,
	}
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
			DSN: buildMysqlDSN(
				op.meta.Params.Username,
				op.meta.Params.Password,
				op.meta.Params.Host,
				op.meta.Params.Port,
				op.meta.Params.DBName,
				charset,
				op.ConnParams,
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
	case "postgres", "postgresql":
		sslMode := op.ConnParams.SSLMode
		if sslMode == "" {
			sslMode = DefaultDatabasePostgresSSLMode
		}
		timeZone := op.ConnParams.TimeZone
		if timeZone == "" {
			timeZone = DefaultDatabasePostgresTimeZone
		}
		if strings.EqualFold(timeZone, "local") {
			timeZone = "UTC"
		}

		db, err = gorm.Open(postgres.New(buildPostgresDialectorConfig(op.meta, op.ConnParams, sslMode, timeZone)), &op.GORMParams)
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
