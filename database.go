package datastore

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/kklab-com/goth-kklogger"
	"github.com/kklab-com/goth-kksecret"
)

var KKDatabaseLocker = sync.Mutex{}
var KKDatabaseProfiles = sync.Map{}
var KKDBParamReaderMaxOpenConn = 64
var KKDBParamReaderMaxIdleConn = 32
var KKDBParamWriterMaxOpenConn = 64
var KKDBParamWriterMaxIdleConn = 32
var KKDBParamConnMaxLifetime = 20000
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
	once       sync.Once
	ConnParams ConnParams
}

type ConnParams struct {
	KKDBParamCharset           string
	KKDBParamTimeout           string
	KKDBParamReadTimeout       string
	KKDBParamWriteTimeout      string
	KKDBParamCollation         string
	KKDBParamLoc               string
	KKDBParamClientFoundRows   bool
	KKDBParamParseTime         bool
	KKDBParamMaxAllowedPacket  int
	KKDBParamReaderMaxOpenConn int
	KKDBParamReaderMaxIdleConn int
	KKDBParamWriterMaxOpenConn int
	KKDBParamWriterMaxIdleConn int
	KKDBParamConnMaxLifetime   int
}

func (ke *KKDatabaseOp) DB() *gorm.DB {
	if ke.ConnParams.KKDBParamConnMaxLifetime <= 0 {
		return newDBConn(ke, 5)
	}

	if ke.opType == TypeWriter && ke.ConnParams.KKDBParamWriterMaxIdleConn <= 0 {
		return newDBConn(ke, 5)
	}

	if ke.opType == TypeReader && ke.ConnParams.KKDBParamReaderMaxIdleConn <= 0 {
		return newDBConn(ke, 5)
	}

	if ke.db == nil {
		ke.once.Do(func() {
			if ke.db == nil {
				db := newDBConn(ke, 5)
				if db == nil {
					ke.once = sync.Once{}
					return
				}

				ke.db = db
			}
		})
	}

	if ke.db == nil {
		return nil
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
		KKDatabaseDebug = _IsKKDatabaseDebug()
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
					KKDBParamCharset:           KKDBParamCharset,
					KKDBParamTimeout:           KKDBParamDialTimeout,
					KKDBParamReadTimeout:       KKDBParamReadTimeout,
					KKDBParamWriteTimeout:      KKDBParamWriteTimeout,
					KKDBParamCollation:         KKDBParamCollation,
					KKDBParamLoc:               KKDBParamLoc,
					KKDBParamClientFoundRows:   KKDBParamClientFoundRows,
					KKDBParamParseTime:         KKDBParamParseTime,
					KKDBParamMaxAllowedPacket:  KKDBParamMaxAllowedPacket,
					KKDBParamReaderMaxOpenConn: KKDBParamReaderMaxOpenConn,
					KKDBParamReaderMaxIdleConn: KKDBParamReaderMaxIdleConn,
					KKDBParamWriterMaxOpenConn: KKDBParamWriterMaxOpenConn,
					KKDBParamWriterMaxIdleConn: KKDBParamWriterMaxIdleConn,
					KKDBParamConnMaxLifetime:   KKDBParamConnMaxLifetime,
				},
				opType: TypeWriter,
				meta:   profile.Writer,
			}
		}

		if profile.Reader.Adapter != "" {
			database.reader = &KKDatabaseOp{
				ConnParams: ConnParams{
					KKDBParamCharset:           KKDBParamCharset,
					KKDBParamTimeout:           KKDBParamDialTimeout,
					KKDBParamReadTimeout:       KKDBParamReadTimeout,
					KKDBParamWriteTimeout:      KKDBParamWriteTimeout,
					KKDBParamCollation:         KKDBParamCollation,
					KKDBParamLoc:               KKDBParamLoc,
					KKDBParamClientFoundRows:   KKDBParamClientFoundRows,
					KKDBParamParseTime:         KKDBParamParseTime,
					KKDBParamMaxAllowedPacket:  KKDBParamMaxAllowedPacket,
					KKDBParamReaderMaxOpenConn: KKDBParamReaderMaxOpenConn,
					KKDBParamReaderMaxIdleConn: KKDBParamReaderMaxIdleConn,
					KKDBParamWriterMaxOpenConn: KKDBParamWriterMaxOpenConn,
					KKDBParamWriterMaxIdleConn: KKDBParamWriterMaxIdleConn,
					KKDBParamConnMaxLifetime:   KKDBParamConnMaxLifetime,
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
		if retry > 10 {
			kklogger.ErrorJ("KKDatabase", "database retry too many times(> 10)")
			fmt.Println("database retry too many times(> 10)")
			return nil
		}

		time.Sleep(time.Second * 1)
		return newDBConn(op, retry+1)
	}

	switch op.opType {
	case TypeReader:
		db.DB().SetMaxOpenConns(op.ConnParams.KKDBParamReaderMaxOpenConn)
		db.DB().SetMaxIdleConns(op.ConnParams.KKDBParamReaderMaxIdleConn)
	case TypeWriter:
		db.DB().SetMaxOpenConns(op.ConnParams.KKDBParamWriterMaxOpenConn)
		db.DB().SetMaxIdleConns(op.ConnParams.KKDBParamWriterMaxIdleConn)
	}

	db.SetLogger(&KKDatabaseLogger{})
	db.DB().SetConnMaxLifetime(time.Millisecond * time.Duration(op.ConnParams.KKDBParamConnMaxLifetime))
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
		kklogger.ErrorJ("Database", fmt.Sprint(v...))
	}
}

func _IsKKDatabaseDebug() bool {
	return strings.ToUpper(os.Getenv("KKAPP_DEBUG")) == "TRUE" ||
		strings.ToUpper(os.Getenv("KKDATABASE_DEBUG")) == "TRUE"
}
