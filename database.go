package datastore

import (
	"fmt"
	kklogger "github.com/yetiz-org/goth-kklogger"
	kksecret "github.com/yetiz-org/goth-kksecret"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

var KKDatabaseLocker = sync.Mutex{}
var KKDatabaseProfiles = sync.Map{}
var KKDBParamReaderMaxOpenConn = 64
var KKDBParamReaderMaxIdleConn = 32
var KKDBParamWriterMaxOpenConn = 64
var KKDBParamWriterMaxIdleConn = 32
var KKDBParamConnMaxLifetime = 20000
var KKDBParamConnMaxIdleTime = 0
var KKDatabaseDebug = false
var _KKDatabaseDebug = sync.Once{}

/* Params ref: https://gorm.io/docs/connecting_to_the_database.html */
var KKDBParamCharset = ""
var KKDBParamDialTimeout = "3s"
var KKDBParamReadTimeout = "30s"
var KKDBParamWriteTimeout = "30s"
var KKDBParamCollation = "utf8mb4_general_ci"
var KKDBParamClientFoundRows = false
var KKDBParamLoc = "Local"
var KKDBParamMaxAllowedPacket = 25165824
var KKDBParamParseTime = true

type OPType int

const (
	TypeReader OPType = 0
	TypeWriter OPType = 1
)

// can be improvement by add more connection
type KKDatabase struct {
	writer *KKDatabaseOp
	reader *KKDatabaseOp
}

func (k *KKDatabase) Writer() *KKDatabaseOp {
	return k.writer
}

func (k *KKDatabase) Reader() *KKDatabaseOp {
	return k.reader
}

type KKDatabaseOp struct {
	opType     OPType //0 reader, 1 writer
	meta       kksecret.DatabaseMeta
	db         *gorm.DB
	opLock     sync.Mutex
	ConnParams ConnParams
}

type ConnParams struct {
	KKDBParamCharset          string
	KKDBParamTimeout          string
	KKDBParamReadTimeout      string
	KKDBParamWriteTimeout     string
	KKDBParamCollation        string
	KKDBParamLoc              string
	KKDBParamClientFoundRows  bool
	KKDBParamParseTime        bool
	KKDBParamMaxAllowedPacket int
	KKDBParamMaxOpenConn      int
	KKDBParamMaxIdleConn      int
	KKDBParamConnMaxLifetime  int
	KKDBParamConnMaxIdleTime  int
}

func (ke *KKDatabaseOp) DB() *gorm.DB {
	if ke.ConnParams.KKDBParamConnMaxLifetime <= 0 {
		return newDBConn(ke, 0)
	}

	if ke.ConnParams.KKDBParamMaxIdleConn <= 0 {
		return newDBConn(ke, 0)
	}

	if ke.db == nil {
		ke.opLock.Lock()
		defer ke.opLock.Unlock()
		if ke.db == nil {
			if ke.db = newDBConn(ke, 0); ke.db == nil {
				return nil
			}
		}
	}

	db := ke.db.New()
	return db
}

func (ke *KKDatabaseOp) Close() error {
	if ke.db != nil {
		return ke.db.Close()
	}

	return nil
}

func (ke *KKDatabaseOp) Adapter() string {
	return ke.meta.Adapter
}

func KKDB(dbname string) *KKDatabase {
	_KKDatabaseDebug.Do(func() {
		if !KKDatabaseDebug {
			KKDatabaseDebug = _IsKKDatastoreDebug()
		}
	})

	if r, f := KKDatabaseProfiles.Load(dbname); f && !KKDatabaseDebug {
		return r.(*KKDatabase)
	}

	profile := kksecret.DatabaseProfile(dbname)
	if profile == nil {
		return nil
	}

	KKDatabaseLocker.Lock()
	defer KKDatabaseLocker.Unlock()
	if KKDatabaseDebug {
		KKDatabaseProfiles.Delete(dbname)
	}

	if r, f := KKDatabaseProfiles.Load(dbname); !f {
		database := new(KKDatabase)
		if profile.Writer.Adapter != "" {
			database.writer = &KKDatabaseOp{
				ConnParams: ConnParams{
					KKDBParamCharset:          KKDBParamCharset,
					KKDBParamTimeout:          KKDBParamDialTimeout,
					KKDBParamReadTimeout:      KKDBParamReadTimeout,
					KKDBParamWriteTimeout:     KKDBParamWriteTimeout,
					KKDBParamCollation:        KKDBParamCollation,
					KKDBParamLoc:              KKDBParamLoc,
					KKDBParamClientFoundRows:  KKDBParamClientFoundRows,
					KKDBParamParseTime:        KKDBParamParseTime,
					KKDBParamMaxAllowedPacket: KKDBParamMaxAllowedPacket,
					KKDBParamMaxOpenConn:      KKDBParamWriterMaxOpenConn,
					KKDBParamMaxIdleConn:      KKDBParamWriterMaxIdleConn,
					KKDBParamConnMaxLifetime:  KKDBParamConnMaxLifetime,
					KKDBParamConnMaxIdleTime:  KKDBParamConnMaxIdleTime,
				},
				opType: TypeWriter,
				meta:   profile.Writer,
			}
		}

		if profile.Reader.Adapter != "" {
			database.reader = &KKDatabaseOp{
				ConnParams: ConnParams{
					KKDBParamCharset:          KKDBParamCharset,
					KKDBParamTimeout:          KKDBParamDialTimeout,
					KKDBParamReadTimeout:      KKDBParamReadTimeout,
					KKDBParamWriteTimeout:     KKDBParamWriteTimeout,
					KKDBParamCollation:        KKDBParamCollation,
					KKDBParamLoc:              KKDBParamLoc,
					KKDBParamClientFoundRows:  KKDBParamClientFoundRows,
					KKDBParamParseTime:        KKDBParamParseTime,
					KKDBParamMaxAllowedPacket: KKDBParamMaxAllowedPacket,
					KKDBParamMaxOpenConn:      KKDBParamReaderMaxOpenConn,
					KKDBParamMaxIdleConn:      KKDBParamReaderMaxIdleConn,
					KKDBParamConnMaxLifetime:  KKDBParamConnMaxLifetime,
					KKDBParamConnMaxIdleTime:  KKDBParamConnMaxIdleTime,
				},
				opType: TypeReader,
				meta:   profile.Reader,
			}
		}

		KKDatabaseProfiles.Store(dbname, database)
		return database
	} else {
		return r.(*KKDatabase)
	}
}

func newDBConn(op *KKDatabaseOp, retry int) *gorm.DB {
	var db *gorm.DB
	var err error
	charset := func() string {
		if op.ConnParams.KKDBParamCharset == "" {
			return op.meta.Params.Charset
		}

		return op.ConnParams.KKDBParamCharset
	}()

	switch op.meta.Adapter {
	case "mysql":
		db, err = gorm.Open(op.meta.Adapter, fmt.Sprintf("%s:%s@(%s)/%s?"+
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
			op.meta.Params.DBName,
			charset,
			op.ConnParams.KKDBParamTimeout,
			op.ConnParams.KKDBParamReadTimeout,
			op.ConnParams.KKDBParamWriteTimeout,
			op.ConnParams.KKDBParamCollation,
			op.ConnParams.KKDBParamLoc,
			op.ConnParams.KKDBParamClientFoundRows,
			op.ConnParams.KKDBParamParseTime,
			op.ConnParams.KKDBParamMaxAllowedPacket,
		))
	default:
		kklogger.ErrorJ("KKDatabase", "adapter not support")
		return nil
	}

	if err != nil {
		kklogger.ErrorJ("KKDatabase", err.Error())
		fmt.Println(err.Error())
		retry += 1
		if retry >= 5 {
			kklogger.ErrorJ("KKDatabase", "database retry too many times(> 5)")
			fmt.Println("database retry too many times(> 5)")
			return nil
		}

		time.Sleep(time.Second * 1)
		return newDBConn(op, retry)
	}

	db.DB().SetMaxOpenConns(op.ConnParams.KKDBParamMaxOpenConn)
	db.DB().SetMaxIdleConns(op.ConnParams.KKDBParamMaxIdleConn)
	db.SetLogger(&KKDatabaseLogger{})
	db.DB().SetConnMaxLifetime(time.Millisecond * time.Duration(op.ConnParams.KKDBParamConnMaxLifetime))
	db.DB().SetConnMaxIdleTime(time.Millisecond * time.Duration(op.ConnParams.KKDBParamConnMaxIdleTime))
	db.Callback().Create().Replace("gorm:update_time_stamp", updateTimeStampForCreateCallback)
	db.Callback().Update().Replace("gorm:update_time_stamp", updateTimeStampForUpdateCallback)
	db.Callback().Delete().Replace("gorm:delete", deleteCallback)
	return db
}

func deleteCallback(scope *gorm.Scope) {
	if !scope.HasError() {
		var extraOption string
		if str, ok := scope.Get("gorm:delete_option"); ok {
			extraOption = fmt.Sprint(str)
		}

		deletedAtField, hasDeletedAtField := scope.FieldByName("DeletedAt")

		if !scope.Search.Unscoped && hasDeletedAtField {
			scope.Raw(fmt.Sprintf(
				"UPDATE %v SET %v=%v%v%v",
				scope.QuotedTableName(),
				scope.Quote(deletedAtField.DBName),
				scope.AddToVars(time.Now().Unix()),
				addExtraSpaceIfExist(scope.CombinedConditionSql()),
				addExtraSpaceIfExist(extraOption),
			)).Exec()
		} else {
			scope.Raw(fmt.Sprintf(
				"DELETE FROM %v%v%v",
				scope.QuotedTableName(),
				addExtraSpaceIfExist(scope.CombinedConditionSql()),
				addExtraSpaceIfExist(extraOption),
			)).Exec()
		}
	}
}

func addExtraSpaceIfExist(str string) string {
	if str != "" {
		return " " + str
	}
	return ""
}

func updateTimeStampForUpdateCallback(scope *gorm.Scope) {
	if _, ok := scope.Get("gorm:update_column"); !ok {
		scope.SetColumn("UpdatedAt", time.Now().Unix())
	}
}

func updateTimeStampForCreateCallback(scope *gorm.Scope) {
	if !scope.HasError() {
		now := time.Now()

		if createdAtField, ok := scope.FieldByName("CreatedAt"); ok {
			if createdAtField.IsBlank {
				createdAtField.Set(now.Unix())
			}
		}

		if updatedAtField, ok := scope.FieldByName("UpdatedAt"); ok {
			if updatedAtField.IsBlank {
				updatedAtField.Set(now.Unix())
			}
		}
	}
}

type KKDatabaseLogger struct {
}

func (k *KKDatabaseLogger) Print(v ...interface{}) {
	if val, ok := v[0].(string); ok && val == "error" {
		unit := KKDatabaseLoggerUnit{
			Logs: []string{},
		}

		for _, val := range v {
			unit.Logs = append(unit.Logs, fmt.Sprint(val))
		}

		kklogger.ErrorJ("Database.Logger", unit)
	}
}

func _IsKKDatastoreDebug() bool {
	return strings.ToUpper(os.Getenv("KKDATASTORE_DEBUG")) == "TRUE"
}

type KKDatabaseLoggerUnit struct {
	Logs []string `json:"logs"`
}
