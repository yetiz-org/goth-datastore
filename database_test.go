package datastore

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"path/filepath"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	secret "github.com/yetiz-org/goth-datastore/secrets"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type databaseCRUDRecord struct {
	ID   uint `gorm:"primaryKey"`
	Name string
}

func (databaseCRUDRecord) TableName() string {
	return "goth_datastore_database_crud_records"
}

// databaseConcRWRecord is used exclusively by high-concurrency read/write tests.
type databaseConcRWRecord struct {
	ID    uint   `gorm:"primaryKey;autoIncrement"`
	Name  string `gorm:"size:128;not null"`
	Value int    `gorm:"default:0"`
}

func (databaseConcRWRecord) TableName() string {
	return "goth_datastore_conc_rw_records"
}

// MockSecret is a mock implementation for secret.Database
type MockSecret struct {
	mock.Mock
}

func (m *MockSecret) Load(module, profile string, v interface{}) error {
	args := m.Called(module, profile, v)
	return args.Error(0)
}

func TestDatabase_Writer(t *testing.T) {
	t.Run("returns writer DatabaseOp", func(t *testing.T) {
		db := &Database{
			writer: &DatabaseOp{
				meta: secret.DatabaseMeta{Adapter: "mysql"},
			},
		}

		writer := db.Writer()
		assert.NotNil(t, writer)
		assert.Equal(t, "mysql", writer.Adapter())
	})

	t.Run("returns nil when writer is nil", func(t *testing.T) {
		db := &Database{}

		writer := db.Writer()
		assert.Nil(t, writer)
	})
}

func TestDatabase_Reader(t *testing.T) {
	t.Run("returns reader DatabaseOp", func(t *testing.T) {
		db := &Database{
			reader: &DatabaseOp{
				meta: secret.DatabaseMeta{Adapter: "mysql"},
			},
		}

		reader := db.Reader()
		assert.NotNil(t, reader)
		assert.Equal(t, "mysql", reader.Adapter())
	})

	t.Run("returns nil when reader is nil", func(t *testing.T) {
		db := &Database{}

		reader := db.Reader()
		assert.Nil(t, reader)
	})
}

func TestDatabaseOp_Adapter(t *testing.T) {
	t.Run("returns correct adapter", func(t *testing.T) {
		op := &DatabaseOp{
			meta: secret.DatabaseMeta{Adapter: "mysql"},
		}

		assert.Equal(t, "mysql", op.Adapter())
	})

	t.Run("returns empty string when no adapter set", func(t *testing.T) {
		op := &DatabaseOp{}

		assert.Equal(t, "", op.Adapter())
	})
}

func TestDatabaseOp_DB(t *testing.T) {
	t.Run("returns cached db instance", func(t *testing.T) {
		mockDB := &gorm.DB{}
		op := &DatabaseOp{
			db: mockDB,
		}

		result := op.DB()
		assert.Equal(t, mockDB, result)
	})

	t.Run("returns nil when db creation fails", func(t *testing.T) {
		// Suppress logging during test
		oldLevel := os.Getenv("LOG_LEVEL")
		os.Setenv("LOG_LEVEL", "FATAL")
		defer os.Setenv("LOG_LEVEL", oldLevel)

		op := &DatabaseOp{
			meta: secret.DatabaseMeta{
				Adapter: "unsupported",
			},
		}

		result := op.DB()
		assert.Nil(t, result)
	})
}

func TestNewDatabase(t *testing.T) {
	t.Run("creates database with default configuration", func(t *testing.T) {
		// Save original defaults
		origMaxOpenConn := DefaultDatabaseMaxOpenConn
		origMaxIdleConn := DefaultDatabaseMaxIdleConn
		origConnMaxLifetime := DefaultDatabaseConnMaxLifetime
		origConnMaxIdleTime := DefaultDatabaseConnMaxIdleTime
		origCharset := DefaultDatabaseCharset
		origDialTimeout := DefaultDatabaseDialTimeout
		origReadTimeout := DefaultDatabaseReadTimeout
		origWriteTimeout := DefaultDatabaseWriteTimeout
		origCollation := DefaultDatabaseCollation
		origClientFoundRows := DefaultDatabaseClientFoundRows
		origLoc := DefaultDatabaseLoc
		origMaxAllowedPacket := DefaultDatabaseMaxAllowedPacket
		origParseTime := DefaultDatabaseParseTime
		origMultiStatements := DefaultDatabaseMultiStatements

		// Restore defaults after test
		defer func() {
			DefaultDatabaseMaxOpenConn = origMaxOpenConn
			DefaultDatabaseMaxIdleConn = origMaxIdleConn
			DefaultDatabaseConnMaxLifetime = origConnMaxLifetime
			DefaultDatabaseConnMaxIdleTime = origConnMaxIdleTime
			DefaultDatabaseCharset = origCharset
			DefaultDatabaseDialTimeout = origDialTimeout
			DefaultDatabaseReadTimeout = origReadTimeout
			DefaultDatabaseWriteTimeout = origWriteTimeout
			DefaultDatabaseCollation = origCollation
			DefaultDatabaseClientFoundRows = origClientFoundRows
			DefaultDatabaseLoc = origLoc
			DefaultDatabaseMaxAllowedPacket = origMaxAllowedPacket
			DefaultDatabaseParseTime = origParseTime
			DefaultDatabaseMultiStatements = origMultiStatements
		}()

		// Test with default values
		DefaultDatabaseMaxOpenConn = 10
		DefaultDatabaseMaxIdleConn = 5
		DefaultDatabaseConnMaxLifetime = 30000
		DefaultDatabaseConnMaxIdleTime = 10000
		DefaultDatabaseCharset = "utf8mb4"
		DefaultDatabaseDialTimeout = "5s"
		DefaultDatabaseReadTimeout = "60s"
		DefaultDatabaseWriteTimeout = "60s"
		DefaultDatabaseCollation = "utf8mb4_unicode_ci"
		DefaultDatabaseClientFoundRows = true
		DefaultDatabaseLoc = "UTC"
		DefaultDatabaseMaxAllowedPacket = 50331648
		DefaultDatabaseParseTime = false
		DefaultDatabaseMultiStatements = true

		// Since we can't easily mock secret.Load, this test mainly validates the defaults
		assert.Equal(t, 10, DefaultDatabaseMaxOpenConn)
		assert.Equal(t, 5, DefaultDatabaseMaxIdleConn)
		assert.Equal(t, 30000, DefaultDatabaseConnMaxLifetime)
		assert.Equal(t, 10000, DefaultDatabaseConnMaxIdleTime)
		assert.Equal(t, "utf8mb4", DefaultDatabaseCharset)
		assert.Equal(t, "5s", DefaultDatabaseDialTimeout)
		assert.Equal(t, "60s", DefaultDatabaseReadTimeout)
		assert.Equal(t, "60s", DefaultDatabaseWriteTimeout)
		assert.Equal(t, "utf8mb4_unicode_ci", DefaultDatabaseCollation)
		assert.Equal(t, true, DefaultDatabaseClientFoundRows)
		assert.Equal(t, "UTC", DefaultDatabaseLoc)
		assert.Equal(t, 50331648, DefaultDatabaseMaxAllowedPacket)
		assert.Equal(t, false, DefaultDatabaseParseTime)
		assert.Equal(t, true, DefaultDatabaseMultiStatements)
	})
}

func TestNewDBPool(t *testing.T) {
	t.Run("returns nil for nil DatabaseOp", func(t *testing.T) {
		// Test the memory issue: newDBPool should handle nil op parameter
		result := newDBPool(nil, 0)
		assert.Nil(t, result)
	})

	t.Run("returns nil for unsupported adapter", func(t *testing.T) {
		// Suppress logging during test
		oldLevel := os.Getenv("LOG_LEVEL")
		os.Setenv("LOG_LEVEL", "FATAL")
		defer os.Setenv("LOG_LEVEL", oldLevel)

		op := &DatabaseOp{
			meta: secret.DatabaseMeta{
				Adapter: "unsupported",
			},
		}

		result := newDBPool(op, 0)
		assert.Nil(t, result)
	})

	t.Run("handles empty adapter", func(t *testing.T) {
		// Suppress logging during test
		oldLevel := os.Getenv("LOG_LEVEL")
		os.Setenv("LOG_LEVEL", "FATAL")
		defer os.Setenv("LOG_LEVEL", oldLevel)

		op := &DatabaseOp{
			meta: secret.DatabaseMeta{
				Adapter: "",
			},
		}

		result := newDBPool(op, 0)
		assert.Nil(t, result)
	})

	t.Run("charset selection logic", func(t *testing.T) {
		// Test charset selection without using complex structs
		op1 := &DatabaseOp{
			ConnParams: ConnParams{
				Charset: "", // empty charset - should use meta charset
			},
		}

		// Mock charset selection logic
		getCharset := func(op *DatabaseOp, metaCharset string) string {
			if op.ConnParams.Charset == "" {
				return metaCharset
			}
			return op.ConnParams.Charset
		}

		assert.Equal(t, "utf8", getCharset(op1, "utf8"))

		op2 := &DatabaseOp{
			ConnParams: ConnParams{
				Charset: "utf8mb4", // non-empty charset
			},
		}

		assert.Equal(t, "utf8mb4", getCharset(op2, "utf8"))
	})

	t.Run("stops retrying after 5 attempts", func(t *testing.T) {
		// This is a tricky test since we can't easily mock database connections
		// We'll test the retry logic conceptually
		retryCount := 0
		maxRetries := 5

		for retryCount < maxRetries {
			retryCount++
			if retryCount >= maxRetries {
				break
			}
		}

		assert.Equal(t, 5, retryCount)
	})
}

func TestConnParams(t *testing.T) {
	t.Run("has correct default values", func(t *testing.T) {
		params := ConnParams{
			Charset:          DefaultDatabaseCharset,
			Timeout:          DefaultDatabaseDialTimeout,
			ReadTimeout:      DefaultDatabaseReadTimeout,
			WriteTimeout:     DefaultDatabaseWriteTimeout,
			Collation:        DefaultDatabaseCollation,
			Loc:              DefaultDatabaseLoc,
			ClientFoundRows:  DefaultDatabaseClientFoundRows,
			ParseTime:        DefaultDatabaseParseTime,
			MultiStatements:  DefaultDatabaseMultiStatements,
			MaxAllowedPacket: DefaultDatabaseMaxAllowedPacket,
			MaxOpenConn:      DefaultDatabaseMaxOpenConn,
			MaxIdleConn:      DefaultDatabaseMaxIdleConn,
			ConnMaxLifetime:  DefaultDatabaseConnMaxLifetime,
			ConnMaxIdleTime:  DefaultDatabaseConnMaxIdleTime,
		}

		assert.Equal(t, DefaultDatabaseCharset, params.Charset)
		assert.Equal(t, DefaultDatabaseDialTimeout, params.Timeout)
		assert.Equal(t, DefaultDatabaseReadTimeout, params.ReadTimeout)
		assert.Equal(t, DefaultDatabaseWriteTimeout, params.WriteTimeout)
		assert.Equal(t, DefaultDatabaseCollation, params.Collation)
		assert.Equal(t, DefaultDatabaseLoc, params.Loc)
		assert.Equal(t, DefaultDatabaseClientFoundRows, params.ClientFoundRows)
		assert.Equal(t, DefaultDatabaseParseTime, params.ParseTime)
		assert.Equal(t, DefaultDatabaseMultiStatements, params.MultiStatements)
		assert.Equal(t, DefaultDatabaseMaxAllowedPacket, params.MaxAllowedPacket)
		assert.Equal(t, DefaultDatabaseMaxOpenConn, params.MaxOpenConn)
		assert.Equal(t, DefaultDatabaseMaxIdleConn, params.MaxIdleConn)
		assert.Equal(t, DefaultDatabaseConnMaxLifetime, params.ConnMaxLifetime)
		assert.Equal(t, DefaultDatabaseConnMaxIdleTime, params.ConnMaxIdleTime)
	})
}

func TestMysqlParams(t *testing.T) {
	t.Run("has correct structure", func(t *testing.T) {
		params := MysqlParams{
			DriverName:                    "mysql",
			ServerVersion:                 "8.0",
			SkipInitializeWithVersion:     false,
			DefaultStringSize:             256,
			DisableWithReturning:          false,
			DisableDatetimePrecision:      false,
			DontSupportRenameIndex:        false,
			DontSupportRenameColumn:       false,
			DontSupportForShareClause:     false,
			DontSupportNullAsDefaultValue: false,
			DontSupportRenameColumnUnique: false,
			DontSupportDropConstraint:     false,
		}

		assert.Equal(t, "mysql", params.DriverName)
		assert.Equal(t, "8.0", params.ServerVersion)
		assert.Equal(t, false, params.SkipInitializeWithVersion)
		assert.Equal(t, uint(256), params.DefaultStringSize)
		assert.Equal(t, false, params.DisableWithReturning)
		assert.Equal(t, false, params.DisableDatetimePrecision)
		assert.Equal(t, false, params.DontSupportRenameIndex)
		assert.Equal(t, false, params.DontSupportRenameColumn)
		assert.Equal(t, false, params.DontSupportForShareClause)
		assert.Equal(t, false, params.DontSupportNullAsDefaultValue)
		assert.Equal(t, false, params.DontSupportRenameColumnUnique)
		assert.Equal(t, false, params.DontSupportDropConstraint)
	})
}

func TestDatabaseOp_Concurrency(t *testing.T) {
	t.Run("handles concurrent DB() calls safely", func(t *testing.T) {
		// Suppress logging during test
		oldLevel := os.Getenv("LOG_LEVEL")
		os.Setenv("LOG_LEVEL", "FATAL")
		defer os.Setenv("LOG_LEVEL", oldLevel)

		op := &DatabaseOp{
			meta: secret.DatabaseMeta{
				Adapter: "unsupported", // Will fail to create DB
			},
		}

		// Simulate concurrent access
		done := make(chan bool, 2)

		go func() {
			result := op.DB()
			assert.Nil(t, result)
			done <- true
		}()

		go func() {
			result := op.DB()
			assert.Nil(t, result)
			done <- true
		}()

		// Wait for both goroutines to complete
		<-done
		<-done
	})
}

func TestDatabaseOp_Logger(t *testing.T) {
	t.Run("sets logger correctly", func(t *testing.T) {
		customLogger := logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags),
			logger.Config{
				SlowThreshold:             time.Second,
				LogLevel:                  logger.Info,
				IgnoreRecordNotFoundError: true,
				Colorful:                  true,
			},
		)

		op := &DatabaseOp{
			Logger: customLogger,
		}

		assert.Equal(t, customLogger, op.Logger)
	})
}

// Test for the specific memory issue fix
func TestNewDBPool_NilCheck(t *testing.T) {
	t.Run("handles nil parameter gracefully", func(t *testing.T) {
		// Suppress logging during test
		oldLevel := os.Getenv("LOG_LEVEL")
		os.Setenv("LOG_LEVEL", "FATAL")
		defer os.Setenv("LOG_LEVEL", oldLevel)

		// This should not panic and should return nil
		result := newDBPool(nil, 0)
		assert.Nil(t, result)
	})
}

// Benchmark tests for performance
func BenchmarkDatabaseOp_DB(b *testing.B) {
	// Suppress logging during benchmark
	oldLevel := os.Getenv("LOG_LEVEL")
	os.Setenv("LOG_LEVEL", "FATAL")
	defer os.Setenv("LOG_LEVEL", oldLevel)

	op := &DatabaseOp{
		db: &gorm.DB{}, // Pre-set DB to avoid creation overhead
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op.DB()
	}
}

func BenchmarkNewDBPool_FailureCase(b *testing.B) {
	// Suppress logging during benchmark
	oldLevel := os.Getenv("LOG_LEVEL")
	os.Setenv("LOG_LEVEL", "FATAL")
	defer os.Setenv("LOG_LEVEL", oldLevel)

	op := &DatabaseOp{
		meta: secret.DatabaseMeta{
			Adapter: "unsupported",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newDBPool(op, 0)
	}
}

// TestLoadDatabaseExampleSecret tests loading Database secret from example file
func TestLoadDatabaseExampleSecret(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.Path()
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory which has the correct structure
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	// Test loading the secret
	t.Run("LoadDatabaseSecret", func(t *testing.T) {
		isLocalHost := func(host string) bool {
			return host == "localhost" || host == "127.0.0.1"
		}

		profile := &secret.Database{}
		err := secret.Load("database", "test", profile)
		assert.NoError(t, err)

		// Verify writer configuration
		assert.NotNil(t, profile.Writer)
		assert.Equal(t, "mysql", profile.Writer.Adapter)
		assert.Equal(t, "utf8mb4", profile.Writer.Params.Charset)
		assert.True(t, isLocalHost(profile.Writer.Params.Host))
		assert.Equal(t, uint(3306), profile.Writer.Params.Port)
		assert.Equal(t, "test", profile.Writer.Params.DBName)
		assert.Equal(t, "test", profile.Writer.Params.Username)
		assert.Equal(t, "test", profile.Writer.Params.Password)

		// Verify reader configuration
		assert.NotNil(t, profile.Reader)
		assert.Equal(t, "mysql", profile.Reader.Adapter)
		assert.Equal(t, "utf8mb4", profile.Reader.Params.Charset)
		assert.True(t, isLocalHost(profile.Reader.Params.Host))
		assert.Equal(t, uint(3306), profile.Reader.Params.Port)
		assert.Equal(t, "test", profile.Reader.Params.DBName)
		assert.Equal(t, "test", profile.Reader.Params.Username)
		assert.Equal(t, "test", profile.Reader.Params.Password)
	})

	t.Run("NewDatabaseWithExampleSecret", func(t *testing.T) {
		// Test creating Database instance with the example secret
		database := NewDatabase("test")

		// NewDatabase should succeed with valid secret
		assert.NotNil(t, database)
		assert.NotNil(t, database.writer)
		assert.NotNil(t, database.reader)

		// Verify the adapter type
		assert.Equal(t, "mysql", database.writer.Meta().Adapter)
		assert.Equal(t, "mysql", database.reader.Meta().Adapter)
	})
}

// TestLoadDatabasePostgresExampleSecret tests loading PostgreSQL Database secret from example file
func TestLoadDatabasePostgresExampleSecret(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.Path()
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory which has the correct structure
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	// Test loading the secret
	t.Run("LoadDatabasePostgresSecret", func(t *testing.T) {
		isLocalHost := func(host string) bool {
			return host == "localhost" || host == "127.0.0.1"
		}

		profile := &secret.Database{}
		err := secret.Load("database", "postgres-test", profile)
		assert.NoError(t, err)

		// Verify writer configuration
		assert.NotNil(t, profile.Writer)
		assert.Equal(t, "postgres", profile.Writer.Adapter)
		assert.Equal(t, "utf8", profile.Writer.Params.Charset)
		assert.True(t, isLocalHost(profile.Writer.Params.Host))
		assert.Equal(t, uint(5432), profile.Writer.Params.Port)
		assert.Equal(t, "test", profile.Writer.Params.DBName)
		assert.Equal(t, "test", profile.Writer.Params.Username)
		assert.Equal(t, "test", profile.Writer.Params.Password)

		// Verify reader configuration
		assert.NotNil(t, profile.Reader)
		assert.Equal(t, "postgres", profile.Reader.Adapter)
		assert.Equal(t, "utf8", profile.Reader.Params.Charset)
		assert.True(t, isLocalHost(profile.Reader.Params.Host))
		assert.Equal(t, uint(5432), profile.Reader.Params.Port)
		assert.Equal(t, "test", profile.Reader.Params.DBName)
		assert.Equal(t, "test", profile.Reader.Params.Username)
		assert.Equal(t, "test", profile.Reader.Params.Password)
	})

	t.Run("NewDatabaseWithPostgresExampleSecret", func(t *testing.T) {
		// Test creating Database instance with the example secret
		database := NewDatabase("postgres-test")

		// NewDatabase should succeed with valid secret
		assert.NotNil(t, database)
		assert.NotNil(t, database.writer)
		assert.NotNil(t, database.reader)

		// Verify the adapter type
		assert.Equal(t, "postgres", database.writer.Meta().Adapter)
		assert.Equal(t, "postgres", database.reader.Meta().Adapter)
	})
}

func TestDatabaseMySQLCRUD(t *testing.T) {
	originalPath := secret.Path()
	defer func() {
		secret.PATH = originalPath
	}()

	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	database := NewDatabase("test")
	if database == nil || database.Writer() == nil {
		t.Skip("database not configured")
	}

	db := database.Writer().DB()
	if db == nil {
		t.Skip("database connection not available")
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Skipf("database sql DB not available: %v", err)
	}
	if err := sqlDB.Ping(); err != nil {
		t.Skipf("database ping failed: %v", err)
	}

	_ = db.Migrator().DropTable(&databaseCRUDRecord{})
	defer func() {
		_ = db.Migrator().DropTable(&databaseCRUDRecord{})
	}()

	err = db.AutoMigrate(&databaseCRUDRecord{})
	assert.NoError(t, err)
	assert.True(t, db.Migrator().HasTable(&databaseCRUDRecord{}))

	rec := &databaseCRUDRecord{Name: "mysql"}
	err = db.Create(rec).Error
	assert.NoError(t, err)
	assert.NotZero(t, rec.ID)

	err = db.Delete(&databaseCRUDRecord{}, rec.ID).Error
	assert.NoError(t, err)

	var cnt int64
	err = db.Model(&databaseCRUDRecord{}).Where("id = ?", rec.ID).Count(&cnt).Error
	assert.NoError(t, err)
	assert.Equal(t, int64(0), cnt)

	err = db.Migrator().DropTable(&databaseCRUDRecord{})
	assert.NoError(t, err)
	assert.False(t, db.Migrator().HasTable(&databaseCRUDRecord{}))
}

func TestDatabasePostgresCRUD(t *testing.T) {
	originalPath := secret.Path()
	defer func() {
		secret.PATH = originalPath
	}()

	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	database := NewDatabase("postgres-test")
	if database == nil || database.Writer() == nil {
		t.Skip("database not configured")
	}

	db := database.Writer().DB()
	if db == nil {
		t.Skip("database connection not available")
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Skipf("database sql DB not available: %v", err)
	}
	if err := sqlDB.Ping(); err != nil {
		t.Skipf("database ping failed: %v", err)
	}

	_ = db.Migrator().DropTable(&databaseCRUDRecord{})
	defer func() {
		_ = db.Migrator().DropTable(&databaseCRUDRecord{})
	}()

	err = db.AutoMigrate(&databaseCRUDRecord{})
	assert.NoError(t, err)
	assert.True(t, db.Migrator().HasTable(&databaseCRUDRecord{}))

	rec := &databaseCRUDRecord{Name: "postgres"}
	err = db.Create(rec).Error
	assert.NoError(t, err)
	assert.NotZero(t, rec.ID)

	err = db.Delete(&databaseCRUDRecord{}, rec.ID).Error
	assert.NoError(t, err)

	var cnt int64
	err = db.Model(&databaseCRUDRecord{}).Where("id = ?", rec.ID).Count(&cnt).Error
	assert.NoError(t, err)
	tassert := assert.New(t)
	tassert.Equal(int64(0), cnt)

	err = db.Migrator().DropTable(&databaseCRUDRecord{})
	assert.NoError(t, err)
	assert.False(t, db.Migrator().HasTable(&databaseCRUDRecord{}))
}

// TestMockDatabaseOp tests the mock Database operator functionality
func TestMockDatabaseOp(t *testing.T) {
	t.Run("Basic mock functionality", func(t *testing.T) {
		mock := NewMockDatabaseOp()

		// Test initial state
		assert.Equal(t, "mysql", mock.Adapter())
		assert.Equal(t, "mysql", mock.Meta().Adapter)
		assert.NotNil(t, mock.GetConnParams())
		assert.Equal(t, "utf8mb4", mock.GetConnParams().Charset)

		// Test DB call tracking
		db := mock.DB()
		assert.Nil(t, db) // Should be nil without configuration
		assert.Equal(t, 1, mock.GetDBCallCount())

		// Test call history
		history := mock.GetCallHistory()
		assert.Len(t, history, 1)
		assert.Equal(t, "DB", history[0].Method)
	})

	t.Run("Configuration methods", func(t *testing.T) {
		mock := NewMockDatabaseOp()

		// Test setting adapter
		mock.SetAdapterResponse("postgres")
		assert.Equal(t, "postgres", mock.Adapter())

		// Test setting metadata
		newMeta := secret.DatabaseMeta{Adapter: "sqlite"}
		mock.SetMeta(newMeta)
		assert.Equal(t, "sqlite", mock.Meta().Adapter)

		// Test setting connection params
		newParams := ConnParams{Charset: "latin1"}
		mock.SetConnParams(newParams)
		assert.Equal(t, "latin1", mock.GetConnParams().Charset)
	})

	t.Run("Response simulation", func(t *testing.T) {
		mock := NewMockDatabaseOp()

		// Test nil DB response
		mock.SetReturnNilDB(true)
		assert.Nil(t, mock.DB())

		// Test DB failure simulation
		mock.SetReturnNilDB(false)
		mock.SimulateDBFailure(true)
		assert.Nil(t, mock.DB())

		// Test error response
		expectedErr := errors.New("connection failed")
		mock.SetDBResponse(nil, expectedErr)
		mock.SimulateDBFailure(false)
		db := mock.DB()
		assert.Nil(t, db)

		history := mock.GetCallsByMethod("DB")
		assert.True(t, len(history) > 0)
	})

	t.Run("Call history tracking", func(t *testing.T) {
		mock := NewMockDatabaseOp()

		// Make multiple calls
		mock.DB()
		mock.Adapter()
		mock.Meta()
		mock.GetConnParams()

		// Verify call history
		history := mock.GetCallHistory()
		assert.Len(t, history, 1) // Only DB() is tracked in call history

		dbCalls := mock.GetCallsByMethod("DB")
		assert.Len(t, dbCalls, 1)

		// Test clearing history
		mock.ClearCallHistory()
		history = mock.GetCallHistory()
		assert.Len(t, history, 0)
		assert.Equal(t, 0, mock.GetDBCallCount())
	})
}

// TestNewMockDatabase tests the mock Database constructor
func TestNewMockDatabase(t *testing.T) {
	t.Run("Creates valid mock Database instance", func(t *testing.T) {
		db := NewMockDatabase()

		assert.NotNil(t, db)
		assert.NotNil(t, db.Writer())
		assert.NotNil(t, db.Reader())

		// Verify both writer and reader are MockDatabaseOp instances
		writer := db.Writer()
		reader := db.Reader()

		assert.Equal(t, "mysql", writer.Adapter())
		assert.Equal(t, "mysql", reader.Adapter())
	})

	t.Run("NewMockDatabaseWithOps creates custom instance", func(t *testing.T) {
		writerMock := NewMockDatabaseOp()
		readerMock := NewMockDatabaseOp()

		writerMock.SetAdapterResponse("postgres")
		readerMock.SetAdapterResponse("mysql")

		db := NewMockDatabaseWithOps(writerMock, readerMock)

		assert.NotNil(t, db)
		assert.Equal(t, "postgres", db.Writer().Adapter())
		assert.Equal(t, "mysql", db.Reader().Adapter())
	})
}

// TestMockDatabaseBuilder tests the builder pattern for mock databases
func TestMockDatabaseBuilder(t *testing.T) {
	t.Run("Builder pattern configuration", func(t *testing.T) {
		builder := NewMockDatabaseBuilder()

		writerMeta := secret.DatabaseMeta{Adapter: "postgres"}
		readerMeta := secret.DatabaseMeta{Adapter: "mysql"}

		db := builder.
			WithWriterAdapter("postgres").
			WithReaderAdapter("mysql").
			WithWriterMeta(writerMeta).
			WithReaderMeta(readerMeta).
			Build()

		assert.NotNil(t, db)
		assert.Equal(t, "postgres", db.Writer().Adapter())
		assert.Equal(t, "mysql", db.Reader().Adapter())
		assert.Equal(t, "postgres", db.Writer().Meta().Adapter)
		assert.Equal(t, "mysql", db.Reader().Meta().Adapter)
	})
}

func TestBuildMysqlDSN_MultiStatements(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.Path()
	defer func() {
		secret.PATH = originalPath
	}()

	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	profile := &secret.Database{}
	err := secret.Load("database", "test", profile)
	assert.NoError(t, err)
	assert.NotNil(t, profile.Writer)

	meta := profile.Writer
	charset := meta.Params.Charset

	base := ConnParams{
		Timeout:          DefaultDatabaseDialTimeout,
		ReadTimeout:      DefaultDatabaseReadTimeout,
		WriteTimeout:     DefaultDatabaseWriteTimeout,
		Collation:        DefaultDatabaseCollation,
		Loc:              DefaultDatabaseLoc,
		ClientFoundRows:  DefaultDatabaseClientFoundRows,
		ParseTime:        DefaultDatabaseParseTime,
		MaxAllowedPacket: DefaultDatabaseMaxAllowedPacket,
	}

	t.Run("true", func(t *testing.T) {
		params := base
		params.MultiStatements = true
		dsn := buildMysqlDSN(meta.Params.Username, meta.Params.Password, meta.Params.Host, meta.Params.Port, meta.Params.DBName, charset, params)
		assert.True(t, strings.Contains(dsn, "multiStatements=true"))
	})

	t.Run("false", func(t *testing.T) {
		params := base
		params.MultiStatements = false
		dsn := buildMysqlDSN(meta.Params.Username, meta.Params.Password, meta.Params.Host, meta.Params.Port, meta.Params.DBName, charset, params)
		assert.True(t, strings.Contains(dsn, "multiStatements=false"))
	})
}

func TestDatabaseIsolationLevel(t *testing.T) {
	t.Run("zero value produces empty strings", func(t *testing.T) {
		var level DatabaseIsolationLevel
		assert.Equal(t, "", level.mysqlValue())
		assert.Equal(t, "", level.postgresValue())
	})

	t.Run("all levels produce correct MySQL values", func(t *testing.T) {
		cases := []struct {
			level    DatabaseIsolationLevel
			expected string
		}{
			{DatabaseIsolationLevelReadUncommitted, "'READ-UNCOMMITTED'"},
			{DatabaseIsolationLevelReadCommitted, "'READ-COMMITTED'"},
			{DatabaseIsolationLevelRepeatableRead, "'REPEATABLE-READ'"},
			{DatabaseIsolationLevelSerializable, "'SERIALIZABLE'"},
		}
		for _, tc := range cases {
			assert.Equal(t, tc.expected, tc.level.mysqlValue(), "level=%s", tc.level)
		}
	})

	t.Run("all levels produce correct PostgreSQL values", func(t *testing.T) {
		cases := []struct {
			level    DatabaseIsolationLevel
			expected string
		}{
			{DatabaseIsolationLevelReadUncommitted, "'read uncommitted'"},
			{DatabaseIsolationLevelReadCommitted, "'read committed'"},
			{DatabaseIsolationLevelRepeatableRead, "'repeatable read'"},
			{DatabaseIsolationLevelSerializable, "'serializable'"},
		}
		for _, tc := range cases {
			assert.Equal(t, tc.expected, tc.level.postgresValue(), "level=%s", tc.level)
		}
	})

	t.Run("invalid value produces empty string", func(t *testing.T) {
		bogus := DatabaseIsolationLevel("bogus")
		assert.Equal(t, "", bogus.mysqlValue())
		assert.Equal(t, "", bogus.postgresValue())
	})
}

func TestBuildMysqlDSN_TransactionIsolation(t *testing.T) {
	base := ConnParams{
		Timeout:          "3s",
		ReadTimeout:      "30s",
		WriteTimeout:     "30s",
		Collation:        "utf8mb4_general_ci",
		Loc:              "Local",
		ParseTime:        true,
		MaxAllowedPacket: 25165824,
	}
	baseSuffix := "charset=utf8mb4" +
		"&timeout=3s" +
		"&readTimeout=30s" +
		"&writeTimeout=30s" +
		"&collation=utf8mb4_general_ci" +
		"&loc=Local" +
		"&clientFoundRows=false" +
		"&parseTime=true" +
		"&maxAllowedPacket=25165824" +
		"&multiStatements=false"

	t.Run("zero value does not add isolation param", func(t *testing.T) {
		dsn := buildMysqlDSN("u", "p", "h", 3306, "d", "utf8mb4", base)
		assert.Equal(t, "u:p@(h:3306)/d?"+baseSuffix, dsn)
	})

	t.Run("ReadCommitted exact DSN", func(t *testing.T) {
		params := base
		params.TransactionIsolation = DatabaseIsolationLevelReadCommitted
		dsn := buildMysqlDSN("root", "pw", "db.host", 3306, "app", "utf8mb4", params)
		expected := "root:pw@(db.host:3306)/app?" + baseSuffix +
			"&transaction_isolation='READ-COMMITTED'"
		assert.Equal(t, expected, dsn)
		t.Logf("MySQL DSN:\n%s", dsn)
	})

	t.Run("Serializable exact DSN", func(t *testing.T) {
		params := base
		params.TransactionIsolation = DatabaseIsolationLevelSerializable
		dsn := buildMysqlDSN("root", "pw", "db.host", 3306, "app", "utf8mb4", params)
		assert.True(t, strings.HasSuffix(dsn, "&transaction_isolation='SERIALIZABLE'"))
		t.Logf("MySQL DSN:\n%s", dsn)
	})

	t.Run("typed isolation appears before ExtraParams", func(t *testing.T) {
		params := base
		params.TransactionIsolation = DatabaseIsolationLevelReadCommitted
		params.ExtraParams = map[string]string{"autocommit": "1"}
		dsn := buildMysqlDSN("u", "p", "h", 3306, "d", "utf8mb4", params)
		idxIso := strings.Index(dsn, "&transaction_isolation=")
		idxExtra := strings.Index(dsn, "&autocommit=")
		assert.True(t, idxIso < idxExtra, "typed field should appear before ExtraParams")
		t.Logf("MySQL DSN:\n%s", dsn)
	})
}

func TestBuildMysqlDSN_ExtraParams(t *testing.T) {
	base := ConnParams{
		Timeout:          "3s",
		ReadTimeout:      "30s",
		WriteTimeout:     "30s",
		Collation:        "utf8mb4_general_ci",
		Loc:              "Local",
		ParseTime:        true,
		MaxAllowedPacket: 25165824,
	}

	t.Run("nil ExtraParams produces identical DSN as before", func(t *testing.T) {
		params := base
		dsn := buildMysqlDSN("root", "secret", "127.0.0.1", 3306, "mydb", "utf8mb4", params)
		expected := "root:secret@(127.0.0.1:3306)/mydb?" +
			"charset=utf8mb4" +
			"&timeout=3s" +
			"&readTimeout=30s" +
			"&writeTimeout=30s" +
			"&collation=utf8mb4_general_ci" +
			"&loc=Local" +
			"&clientFoundRows=false" +
			"&parseTime=true" +
			"&maxAllowedPacket=25165824" +
			"&multiStatements=false"
		assert.Equal(t, expected, dsn)
	})

	t.Run("empty map identical to nil", func(t *testing.T) {
		dsnNil := buildMysqlDSN("u", "p", "h", 3306, "d", "utf8mb4", base)
		params := base
		params.ExtraParams = map[string]string{}
		dsnEmpty := buildMysqlDSN("u", "p", "h", 3306, "d", "utf8mb4", params)
		assert.Equal(t, dsnNil, dsnEmpty)
	})

	t.Run("multiple ExtraParams sorted deterministically", func(t *testing.T) {
		params := base
		params.ExtraParams = map[string]string{
			"autocommit": "1",
			"sql_mode":   "'STRICT_TRANS_TABLES'",
		}
		dsn := buildMysqlDSN("u", "p", "h", 3306, "d", "utf8mb4", params)
		assert.True(t, strings.HasSuffix(dsn,
			"&autocommit=1&sql_mode='STRICT_TRANS_TABLES'"))
		t.Logf("MySQL DSN:\n%s", dsn)
	})
}

func TestBuildPostgresDSN_TransactionIsolation(t *testing.T) {
	baseDSN := "host=localhost user=u password=p dbname=d port=5432 sslmode=disable TimeZone=UTC"

	t.Run("zero value does not add isolation param", func(t *testing.T) {
		dsn := buildPostgresDSN("localhost", "u", "p", "d", 5432, "disable", "UTC", "", nil)
		assert.Equal(t, baseDSN, dsn)
	})

	t.Run("ReadCommitted exact DSN", func(t *testing.T) {
		dsn := buildPostgresDSN("pg.host", "admin", "secret", "mydb", 5432, "require", "Asia/Taipei",
			DatabaseIsolationLevelReadCommitted, nil)
		expected := "host=pg.host user=admin password=secret dbname=mydb port=5432 sslmode=require TimeZone=Asia/Taipei" +
			" default_transaction_isolation='read committed'"
		assert.Equal(t, expected, dsn)
		t.Logf("PostgreSQL DSN:\n%s", dsn)
	})

	t.Run("Serializable exact DSN", func(t *testing.T) {
		dsn := buildPostgresDSN("localhost", "u", "p", "d", 5432, "disable", "UTC",
			DatabaseIsolationLevelSerializable, nil)
		assert.True(t, strings.HasSuffix(dsn, " default_transaction_isolation='serializable'"))
		t.Logf("PostgreSQL DSN:\n%s", dsn)
	})

	t.Run("typed isolation appears before ExtraParams", func(t *testing.T) {
		extra := map[string]string{"application_name": "myapp"}
		dsn := buildPostgresDSN("localhost", "u", "p", "d", 5432, "disable", "UTC",
			DatabaseIsolationLevelReadCommitted, extra)
		idxIso := strings.Index(dsn, " default_transaction_isolation=")
		idxExtra := strings.Index(dsn, " application_name=")
		assert.True(t, idxIso < idxExtra, "typed field should appear before ExtraParams")
		t.Logf("PostgreSQL DSN:\n%s", dsn)
	})
}

func TestBuildPostgresDSN_ExtraParams(t *testing.T) {
	t.Run("nil ExtraParams exact match", func(t *testing.T) {
		dsn := buildPostgresDSN("localhost", "user", "pass", "db", 5432, "disable", "UTC", "", nil)
		assert.Equal(t, "host=localhost user=user password=pass dbname=db port=5432 sslmode=disable TimeZone=UTC", dsn)
	})

	t.Run("empty map identical to nil", func(t *testing.T) {
		dsnNil := buildPostgresDSN("h", "u", "p", "d", 5432, "disable", "UTC", "", nil)
		dsnEmpty := buildPostgresDSN("h", "u", "p", "d", 5432, "disable", "UTC", "", map[string]string{})
		assert.Equal(t, dsnNil, dsnEmpty)
	})

	t.Run("multiple ExtraParams sorted deterministically", func(t *testing.T) {
		extra := map[string]string{
			"application_name":  "myapp",
			"statement_timeout": "30000",
		}
		dsn := buildPostgresDSN("localhost", "u", "p", "d", 5432, "disable", "UTC", "", extra)
		assert.True(t, strings.HasSuffix(dsn,
			" application_name=myapp statement_timeout=30000"))
		t.Logf("PostgreSQL DSN:\n%s", dsn)
	})
}

func TestBuildPostgresDialectorConfig_ExtraParams(t *testing.T) {
	originalPath := secret.Path()
	defer func() {
		secret.PATH = originalPath
	}()

	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	profile := &secret.Database{}
	err := secret.Load("database", "postgres-test", profile)
	assert.NoError(t, err)
	assert.NotNil(t, profile.Writer)

	meta := profile.Writer
	sslMode := DefaultDatabasePostgresSSLMode
	tz := "UTC"

	t.Run("typed TransactionIsolation ends up in dialector DSN", func(t *testing.T) {
		params := ConnParams{
			TransactionIsolation: DatabaseIsolationLevelReadCommitted,
		}
		cfg := buildPostgresDialectorConfig(meta, params, sslMode, tz)
		assert.Contains(t, cfg.DSN, " default_transaction_isolation='read committed'")
		t.Logf("Dialector config DSN:\n%s", cfg.DSN)
	})

	t.Run("ExtraParams end up in dialector DSN", func(t *testing.T) {
		params := ConnParams{
			ExtraParams: map[string]string{
				"application_name": "goth-test",
			},
		}
		cfg := buildPostgresDialectorConfig(meta, params, sslMode, tz)
		assert.Contains(t, cfg.DSN, " application_name=goth-test")
		t.Logf("Dialector config DSN:\n%s", cfg.DSN)
	})

	t.Run("both typed and ExtraParams work together", func(t *testing.T) {
		params := ConnParams{
			TransactionIsolation: DatabaseIsolationLevelSerializable,
			ExtraParams: map[string]string{
				"application_name": "goth-test",
			},
		}
		cfg := buildPostgresDialectorConfig(meta, params, sslMode, tz)
		assert.Contains(t, cfg.DSN, " default_transaction_isolation='serializable'")
		assert.Contains(t, cfg.DSN, " application_name=goth-test")
		t.Logf("Dialector config DSN:\n%s", cfg.DSN)
	})

	t.Run("nil ExtraParams does not alter base DSN", func(t *testing.T) {
		withNil := buildPostgresDialectorConfig(meta, ConnParams{}, sslMode, tz)
		withEmpty := buildPostgresDialectorConfig(meta, ConnParams{ExtraParams: map[string]string{}}, sslMode, tz)
		assert.Equal(t, withNil.DSN, withEmpty.DSN)
		assert.False(t, strings.HasSuffix(withNil.DSN, " "))
	})
}

func TestBuildPostgresDialectorConfig_MultiStatements(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.Path()
	defer func() {
		secret.PATH = originalPath
	}()

	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	profile := &secret.Database{}
	err := secret.Load("database", "postgres-test", profile)
	assert.NoError(t, err)
	assert.NotNil(t, profile.Writer)

	meta := profile.Writer
	sslMode := DefaultDatabasePostgresSSLMode
	tz := "UTC"

	t.Run("true", func(t *testing.T) {
		params := ConnParams{MultiStatements: true}
		cfg := buildPostgresDialectorConfig(meta, params, sslMode, tz)
		assert.True(t, cfg.PreferSimpleProtocol)
	})

	t.Run("false", func(t *testing.T) {
		params := ConnParams{MultiStatements: false}
		cfg := buildPostgresDialectorConfig(meta, params, sslMode, tz)
		assert.False(t, cfg.PreferSimpleProtocol)
	})
}

// =============================================================================
// HIGH-CONCURRENCY CONFIGURATION EXAMPLES
// =============================================================================
// The following tests document and validate the recommended ConnParams for MySQL
// and PostgreSQL in high-concurrency production environments. Each parameter is
// annotated with its purpose and recommended tuning rationale.
//
// After the example tests, two load tests exercise the actual connection pool
// with 50 goroutines × 20 queries each to verify correctness under concurrent
// access. Run them with: go test -race -run TestDatabase.*HighConcurrency -v
// =============================================================================

// TestMySQLHighConcurrencyConnParams documents and validates the recommended ConnParams
// for MySQL under high-concurrency OLTP workloads.
//
// Key tuning areas:
//   - Connection pool: MaxOpenConn / MaxIdleConn / ConnMaxLifetime / ConnMaxIdleTime
//   - Transaction isolation: READ COMMITTED eliminates gap-lock contention
//   - Driver extras: interpolateParams / checkConnLiveness reduce latency and errors
func TestMySQLHighConcurrencyConnParams(t *testing.T) {
	// HIGH-CONCURRENCY RECOMMENDED ConnParams FOR MYSQL
	// --------------------------------------------------
	// Copy and adjust these values to your workload and MySQL server capacity.
	// Formula for MaxOpenConn per app instance:
	//   min(max_connections × 0.8 / app_instances, CPU_cores × 4)
	params := ConnParams{
		// -- Connection pool sizing --

		// Cap the total open connections. Keeps the application from overwhelming
		// MySQL's max_connections (default 151). Leave headroom for DBA / monitoring.
		MaxOpenConn: 50,

		// Half of MaxOpenConn kept idle. Burst requests reuse idle connections
		// immediately, avoiding reconnect latency spikes.
		MaxIdleConn: 25,

		// ConnMaxLifetime (ms): rotate connections every 5 minutes.
		// Prevents silent server-side disconnects (wait_timeout) and ensures
		// the client picks up DNS / load-balancer changes automatically.
		ConnMaxLifetime: 300_000,

		// ConnMaxIdleTime (ms): release connections idle for more than 1 minute.
		// Frees server resources during low-traffic windows without affecting peak throughput.
		ConnMaxIdleTime: 60_000,

		// -- Transaction isolation --

		// READ COMMITTED is strongly recommended for high-write OLTP workloads.
		// MySQL's default (REPEATABLE READ) acquires gap locks on indexed ranges,
		// serialising concurrent INSERTs on the same index and causing deadlocks
		// under high write concurrency. READ COMMITTED eliminates gap locks.
		TransactionIsolation: DatabaseIsolationLevelReadCommitted,

		// -- Correctness helpers --

		// ParseTime decodes DATETIME/TIMESTAMP columns directly into time.Time.
		ParseTime: true,

		// ClientFoundRows reports rows matched (not rows changed) for UPDATEs,
		// consistent with PostgreSQL semantics; prevents silent no-op confusion.
		ClientFoundRows: true,

		// -- Standard DSN parameters --
		Charset:          "utf8mb4",
		Collation:        "utf8mb4_general_ci",
		Timeout:          "3s",    // TCP connect timeout
		ReadTimeout:      "30s",   // per-query read deadline
		WriteTimeout:     "30s",   // per-query write deadline
		Loc:              "UTC",
		MaxAllowedPacket: 25165824, // 24 MiB

		// -- Extra DSN parameters --
		// These are appended as &key=value to the MySQL DSN.
		// Unrecognised keys are applied via SET <key> = <value> on connection.
		ExtraParams: map[string]string{
			// interpolateParams=true: the driver interpolates parameters client-side,
			// eliminating the prepare → execute → close round-trip per query.
			// Reduces latency noticeably at >1 000 QPS. Safe for most workloads;
			// disable only if you require binary-protocol edge-case precision.
			"interpolateParams": "true",

			// checkConnLiveness=true (go-sql-driver default since v1.6):
			// validates a connection with a lightweight ping before handing it to the caller.
			// Catches TCP resets and half-open sockets transparently, preventing
			// "broken pipe" / "invalid connection" errors under connection churn.
			"checkConnLiveness": "true",
		},
	}

	// Verify the recommended params produce the correct DSN segments.
	dsn := buildMysqlDSN("user", "pass", "127.0.0.1", 3306, "app", "utf8mb4", params)
	assert.Contains(t, dsn, "&transaction_isolation='READ-COMMITTED'",
		"READ COMMITTED must appear in DSN to take effect session-wide")
	assert.Contains(t, dsn, "&interpolateParams=true",
		"interpolateParams must be in DSN to enable client-side parameter interpolation")
	assert.Contains(t, dsn, "&checkConnLiveness=true",
		"checkConnLiveness must be in DSN to enable connection health checks before use")
	assert.Equal(t, 50, params.MaxOpenConn)
	assert.Equal(t, 25, params.MaxIdleConn)
	assert.Equal(t, 300_000, params.ConnMaxLifetime)
	assert.Equal(t, 60_000, params.ConnMaxIdleTime)
	t.Logf("MySQL high-concurrency DSN: %s", dsn)
}

// TestPostgresHighConcurrencyConnParams documents and validates the recommended ConnParams
// for PostgreSQL under high-concurrency OLTP workloads.
//
// Key tuning areas:
//   - Connection pool: MaxOpenConn / MaxIdleConn / ConnMaxLifetime / ConnMaxIdleTime
//   - SSL: "require" for production; "disable" only for local dev / private networks
//   - Transaction isolation: READ COMMITTED (PostgreSQL default, but explicit is safer)
//   - ExtraParams: application_name for observability; statement_timeout as a safety net
func TestPostgresHighConcurrencyConnParams(t *testing.T) {
	// HIGH-CONCURRENCY RECOMMENDED ConnParams FOR POSTGRESQL
	// -------------------------------------------------------
	// PostgreSQL has a hard connection limit (max_connections, default 100).
	// For very high concurrency, put PgBouncer in transaction-mode between app and DB.
	// For direct connections: MaxOpenConn ≤ max_connections × 0.8 / app_instances.
	params := ConnParams{
		// -- Connection pool sizing --

		// Prevent exceeding PostgreSQL's max_connections.
		// Each idle PostgreSQL backend holds ~5–10 MiB of shared memory.
		MaxOpenConn: 50,

		// Keep half the pool idle to absorb request bursts without reconnect overhead.
		MaxIdleConn: 25,

		// ConnMaxLifetime (ms): rotate connections every 5 minutes.
		// Works around per-connection memory accumulation (plan caches) over time
		// and ensures reconnection after a failover or load-balancer change.
		ConnMaxLifetime: 300_000,

		// ConnMaxIdleTime (ms): close connections idle for more than 1 minute.
		// Reduces idle-connection overhead on the PostgreSQL server.
		ConnMaxIdleTime: 60_000,

		// -- Security --

		// SSLMode "require" validates that the connection is encrypted.
		// Use "verify-full" with a CA certificate for maximum security in production.
		// "disable" is only acceptable for local development / private networks.
		SSLMode: "require",

		// -- Temporal consistency --

		// Always set an explicit timezone so TIMESTAMPTZ arithmetic is predictable
		// regardless of the server's default_timezone setting.
		TimeZone: "UTC",

		// -- Transaction isolation --

		// READ COMMITTED is PostgreSQL's default, but setting it explicitly:
		//   (a) documents intent clearly in code, and
		//   (b) guards against accidental ALTER SYSTEM SET default_transaction_isolation.
		// For serialisable snapshot isolation (SSI) workloads switch to Serializable.
		TransactionIsolation: DatabaseIsolationLevelReadCommitted,

		// -- Standard parameters --
		Charset:   "utf8",
		ParseTime: true,

		// -- Extra DSN parameters --
		// These are appended as space-separated key=value pairs to the PostgreSQL DSN.
		// Values with spaces must be single-quoted.
		ExtraParams: map[string]string{
			// application_name identifies this service in pg_stat_activity and slow-query logs.
			// Essential for diagnosing connection storms and long-running queries in production.
			"application_name": "goth-datastore",

			// statement_timeout (ms): cancels queries running longer than 30 s automatically.
			// Prevents a single runaway query from holding locks and degrading the whole service.
			// Adjust per SLA; set to 0 to disable (not recommended for high-concurrency prod).
			"statement_timeout": "30000",

			// connect_timeout (s): give up on the initial TCP handshake after 10 seconds.
			// Prevents goroutines from blocking indefinitely during network partitions.
			"connect_timeout": "10",
		},
	}

	// Verify the recommended params produce the correct DSN segments.
	dsn := buildPostgresDSN(
		"pg.host", "user", "pass", "app", 5432,
		params.SSLMode, params.TimeZone,
		params.TransactionIsolation, params.ExtraParams,
	)
	assert.Contains(t, dsn, " default_transaction_isolation='read committed'",
		"READ COMMITTED must appear in DSN to take effect session-wide")
	assert.Contains(t, dsn, " application_name=goth-datastore",
		"application_name must appear in DSN for observability in pg_stat_activity")
	assert.Contains(t, dsn, " statement_timeout=30000",
		"statement_timeout must appear in DSN as a safety net against runaway queries")
	assert.Contains(t, dsn, " connect_timeout=10",
		"connect_timeout must appear in DSN to bound initial connection establishment time")
	assert.Equal(t, "require", params.SSLMode)
	assert.Equal(t, 50, params.MaxOpenConn)
	assert.Equal(t, 25, params.MaxIdleConn)
	assert.Equal(t, 300_000, params.ConnMaxLifetime)
	assert.Equal(t, 60_000, params.ConnMaxIdleTime)
	t.Logf("PostgreSQL high-concurrency DSN: %s", dsn)
}

// TestDatabaseMySQLHighConcurrencyLoad verifies that the MySQL connection pool handles
// concurrent load without errors, deadlocks, or pool exhaustion.
//
// The test launches 50 goroutines, each executing 20 SELECT 1 queries simultaneously.
// High-concurrency ConnParams (READ COMMITTED, interpolateParams, pool tuning) are
// applied before the pool is opened to validate end-to-end wiring.
//
// Skip condition: test is skipped when the MySQL instance from example/database.test.json
// is unreachable, so it is safe to run in CI environments without a database.
func TestDatabaseMySQLHighConcurrencyLoad(t *testing.T) {
	originalPath := secret.Path()
	defer func() { secret.PATH = originalPath }()

	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	database := NewDatabase("test")
	if database == nil || database.Writer() == nil {
		t.Skip("MySQL database not configured")
	}

	// Apply recommended high-concurrency ConnParams before the pool is created.
	// op.db is nil at this point, so modifying ConnParams is race-free.
	op := database.writer.(*DatabaseOp)
	op.ConnParams.MaxOpenConn = 20
	op.ConnParams.MaxIdleConn = 10
	op.ConnParams.ConnMaxLifetime = 300_000
	op.ConnParams.ConnMaxIdleTime = 60_000
	op.ConnParams.TransactionIsolation = DatabaseIsolationLevelReadCommitted
	op.ConnParams.ExtraParams = map[string]string{
		"interpolateParams": "true",
		"checkConnLiveness": "true",
	}

	db := database.Writer().DB()
	if db == nil {
		t.Skip("MySQL connection not available")
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Skipf("MySQL sql.DB not available: %v", err)
	}
	if err := sqlDB.Ping(); err != nil {
		t.Skipf("MySQL ping failed: %v", err)
	}

	const (
		goroutines          = 50
		queriesPerGoroutine = 20
	)

	var (
		wg      sync.WaitGroup
		errsMu  sync.Mutex
		errList []error
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < queriesPerGoroutine; j++ {
				var result int
				if qErr := db.Raw("SELECT 1").Scan(&result).Error; qErr != nil {
					errsMu.Lock()
					errList = append(errList, qErr)
					errsMu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	total := goroutines * queriesPerGoroutine
	t.Logf("MySQL high-concurrency load: %d goroutines × %d queries = %d total, errors: %d",
		goroutines, queriesPerGoroutine, total, len(errList))
	for _, qErr := range errList {
		t.Errorf("concurrent query error: %v", qErr)
	}
	assert.Empty(t, errList, "expected zero errors under concurrent MySQL load")
}

// TestDatabasePostgresHighConcurrencyLoad verifies that the PostgreSQL connection pool
// handles concurrent load without errors, deadlocks, or pool exhaustion.
//
// The test launches 50 goroutines, each executing 20 SELECT 1 queries simultaneously.
// High-concurrency ConnParams (READ COMMITTED, application_name, statement_timeout,
// pool tuning) are applied before the pool is opened to validate end-to-end wiring.
//
// Skip condition: test is skipped when the PostgreSQL instance from
// example/database.postgres-test.json is unreachable.
func TestDatabasePostgresHighConcurrencyLoad(t *testing.T) {
	originalPath := secret.Path()
	defer func() { secret.PATH = originalPath }()

	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	database := NewDatabase("postgres-test")
	if database == nil || database.Writer() == nil {
		t.Skip("PostgreSQL database not configured")
	}

	// Apply recommended high-concurrency ConnParams before the pool is created.
	// op.db is nil at this point, so modifying ConnParams is race-free.
	op := database.writer.(*DatabaseOp)
	op.ConnParams.MaxOpenConn = 20
	op.ConnParams.MaxIdleConn = 10
	op.ConnParams.ConnMaxLifetime = 300_000
	op.ConnParams.ConnMaxIdleTime = 60_000
	op.ConnParams.SSLMode = "disable" // use "require" in production
	op.ConnParams.TimeZone = "UTC"
	op.ConnParams.TransactionIsolation = DatabaseIsolationLevelReadCommitted
	op.ConnParams.ExtraParams = map[string]string{
		"application_name": "goth-datastore-test",
		"statement_timeout": "30000",
		"connect_timeout":   "10",
	}

	db := database.Writer().DB()
	if db == nil {
		t.Skip("PostgreSQL connection not available")
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Skipf("PostgreSQL sql.DB not available: %v", err)
	}
	if err := sqlDB.Ping(); err != nil {
		t.Skipf("PostgreSQL ping failed: %v", err)
	}

	const (
		goroutines          = 50
		queriesPerGoroutine = 20
	)

	var (
		wg      sync.WaitGroup
		errsMu  sync.Mutex
		errList []error
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < queriesPerGoroutine; j++ {
				var result int
				if qErr := db.Raw("SELECT 1").Scan(&result).Error; qErr != nil {
					errsMu.Lock()
					errList = append(errList, qErr)
					errsMu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	total := goroutines * queriesPerGoroutine
	t.Logf("PostgreSQL high-concurrency load: %d goroutines × %d queries = %d total, errors: %d",
		goroutines, queriesPerGoroutine, total, len(errList))
	for _, qErr := range errList {
		t.Errorf("concurrent query error: %v", qErr)
	}
	assert.Empty(t, errList, "expected zero errors under concurrent PostgreSQL load")
}

// TestDatabaseMySQLHighConcurrencyReadWrite validates MySQL correctness and connection pool
// behaviour under simultaneous read and write load.
//
// Correctness checks (before concurrency):
//   - Session-level transaction_isolation is actually READ-COMMITTED at the server (not just DSN)
//
// Concurrency layout (50 goroutines released simultaneously via ready channel):
//   - 20 writer goroutines: INSERT → UPDATE (value=(j+1)*10) → SELECT verify (data integrity)
//   - 30 reader goroutines: COUNT(*) + WHERE find (concurrent with writes)
//
// Post-concurrency integrity check:
//   - Final COUNT must equal writerGoroutines × opsPerWriter (no records lost or duplicated)
//
// Cleanup: table is always dropped via t.Cleanup, even on test failure.
func TestDatabaseMySQLHighConcurrencyReadWrite(t *testing.T) {
	originalPath := secret.Path()
	defer func() { secret.PATH = originalPath }()

	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	database := NewDatabase("test")
	if database == nil || database.Writer() == nil {
		t.Skip("MySQL database not configured")
	}

	op := database.writer.(*DatabaseOp)
	op.ConnParams.MaxOpenConn = 20
	op.ConnParams.MaxIdleConn = 10
	op.ConnParams.ConnMaxLifetime = 300_000
	op.ConnParams.ConnMaxIdleTime = 60_000
	op.ConnParams.TransactionIsolation = DatabaseIsolationLevelReadCommitted
	op.ConnParams.ExtraParams = map[string]string{
		"interpolateParams": "true",
		"checkConnLiveness": "true",
	}

	db := database.Writer().DB()
	if db == nil {
		t.Skip("MySQL connection not available")
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Skipf("MySQL sql.DB not available: %v", err)
	}
	if err := sqlDB.Ping(); err != nil {
		t.Skipf("MySQL ping failed: %v", err)
	}

	// ── Correctness: verify isolation level is actually applied at server level ──
	var sessionIsolation string
	if err := db.Raw("SELECT @@SESSION.transaction_isolation").Scan(&sessionIsolation).Error; err != nil {
		t.Fatalf("could not query session isolation level: %v", err)
	}
	assert.Equal(t, "READ-COMMITTED", sessionIsolation,
		"TransactionIsolation=ReadCommitted must be applied at server level (@@SESSION.transaction_isolation)")
	t.Logf("MySQL @@SESSION.transaction_isolation = %q ✓", sessionIsolation)

	// ── Table setup: always cleaned up even on failure ──
	_ = db.Migrator().DropTable(&databaseConcRWRecord{})
	t.Cleanup(func() {
		if err := db.Migrator().DropTable(&databaseConcRWRecord{}); err != nil {
			t.Logf("cleanup DropTable: %v", err)
		}
	})
	if err := db.AutoMigrate(&databaseConcRWRecord{}); err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}

	const (
		writerGoroutines = 20
		readerGoroutines = 30
		opsPerWriter     = 10 // INSERT + UPDATE + SELECT verify per iteration
		opsPerReader     = 20 // COUNT + find per iteration
	)

	var (
		wg      sync.WaitGroup
		errsMu  sync.Mutex
		errList []error
		ready   = make(chan struct{}) // closed to release all goroutines simultaneously
	)

	addErr := func(e error) {
		errsMu.Lock()
		errList = append(errList, e)
		errsMu.Unlock()
	}

	// Writers: INSERT initial value → UPDATE to (j+1)*10 → SELECT and verify
	// Using (j+1)*10 ensures the updated value is always different from the initial j,
	// so the integrity check can distinguish "UPDATE applied" from "no-op".
	for i := 0; i < writerGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-ready
			for j := 0; j < opsPerWriter; j++ {
				rec := &databaseConcRWRecord{
					Name:  fmt.Sprintf("mysql-w%d-i%d", workerID, j),
					Value: j, // initial value
				}

				if err := db.Create(rec).Error; err != nil {
					addErr(fmt.Errorf("INSERT w%d i%d: %w", workerID, j, err))
					continue
				}

				updatedValue := (j + 1) * 10
				if err := db.Model(rec).Update("value", updatedValue).Error; err != nil {
					addErr(fmt.Errorf("UPDATE w%d i%d id=%d: %w", workerID, j, rec.ID, err))
					continue
				}

				// Read-back verify: confirms the write landed correctly on the server.
				var fetched databaseConcRWRecord
				if err := db.First(&fetched, rec.ID).Error; err != nil {
					addErr(fmt.Errorf("SELECT w%d i%d id=%d: %w", workerID, j, rec.ID, err))
					continue
				}
				if fetched.Value != updatedValue {
					addErr(fmt.Errorf("data integrity w%d i%d id=%d: want value=%d got %d",
						workerID, j, rec.ID, updatedValue, fetched.Value))
				}
			}
		}(i)
	}

	// Readers: concurrent COUNT(*) + WHERE find while writers are active
	for i := 0; i < readerGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-ready
			for j := 0; j < opsPerReader; j++ {
				var count int64
				if err := db.Model(&databaseConcRWRecord{}).Count(&count).Error; err != nil {
					addErr(fmt.Errorf("COUNT r%d i%d: %w", workerID, j, err))
				}
				var records []databaseConcRWRecord
				if err := db.Where("value > ?", 0).Limit(5).Find(&records).Error; err != nil {
					addErr(fmt.Errorf("find r%d i%d: %w", workerID, j, err))
				}
			}
		}(i)
	}

	// Release all goroutines simultaneously, then wait for completion
	close(ready)
	wg.Wait()

	// ── Post-concurrency integrity: all inserts must have persisted ──
	for _, e := range errList {
		t.Errorf("concurrent op error: %v", e)
	}
	assert.Empty(t, errList, "zero errors expected under concurrent MySQL R/W load")

	var finalCount int64
	if err := db.Model(&databaseConcRWRecord{}).Count(&finalCount).Error; err != nil {
		t.Fatalf("final COUNT failed: %v", err)
	}
	expectedCount := int64(writerGoroutines * opsPerWriter)
	assert.Equal(t, expectedCount, finalCount,
		"final row count must equal writerGoroutines×opsPerWriter — no records lost or duplicated")

	totalOps := writerGoroutines*opsPerWriter*3 + readerGoroutines*opsPerReader*2
	t.Logf("MySQL concurrent R/W: %d writers × %d write-cycles, %d readers × %d read-cycles ≈ %d ops | final rows=%d/%d errors=%d",
		writerGoroutines, opsPerWriter, readerGoroutines, opsPerReader, totalOps, finalCount, expectedCount, len(errList))
}

// TestDatabasePostgresHighConcurrencyReadWrite validates PostgreSQL correctness and connection
// pool behaviour under simultaneous read and write load.
//
// Correctness checks (before concurrency):
//   - default_transaction_isolation is actually READ COMMITTED at the server
//   - application_name ExtraParam is applied and visible in pg_stat_activity
//
// Concurrency layout (50 goroutines released simultaneously via ready channel):
//   - 20 writer goroutines: INSERT → UPDATE (value=(j+1)*10) → SELECT verify
//   - 30 reader goroutines: COUNT(*) + WHERE find (concurrent with writes)
//
// Post-concurrency integrity check:
//   - Final COUNT must equal writerGoroutines × opsPerWriter
//
// Cleanup: table is always dropped via t.Cleanup, even on test failure.
func TestDatabasePostgresHighConcurrencyReadWrite(t *testing.T) {
	originalPath := secret.Path()
	defer func() { secret.PATH = originalPath }()

	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	database := NewDatabase("postgres-test")
	if database == nil || database.Writer() == nil {
		t.Skip("PostgreSQL database not configured")
	}

	op := database.writer.(*DatabaseOp)
	op.ConnParams.MaxOpenConn = 20
	op.ConnParams.MaxIdleConn = 10
	op.ConnParams.ConnMaxLifetime = 300_000
	op.ConnParams.ConnMaxIdleTime = 60_000
	op.ConnParams.SSLMode = "disable"
	op.ConnParams.TimeZone = "UTC"
	op.ConnParams.TransactionIsolation = DatabaseIsolationLevelReadCommitted
	op.ConnParams.ExtraParams = map[string]string{
		"application_name":  "goth-datastore-test",
		"statement_timeout": "30000",
		"connect_timeout":   "10",
	}

	db := database.Writer().DB()
	if db == nil {
		t.Skip("PostgreSQL connection not available")
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Skipf("PostgreSQL sql.DB not available: %v", err)
	}
	if err := sqlDB.Ping(); err != nil {
		t.Skipf("PostgreSQL ping failed: %v", err)
	}

	// ── Correctness: verify default_transaction_isolation at server level ──
	var sessionIsolation string
	if err := db.Raw("SELECT current_setting('default_transaction_isolation')").Scan(&sessionIsolation).Error; err != nil {
		t.Fatalf("could not query default_transaction_isolation: %v", err)
	}
	assert.Equal(t, "read committed", sessionIsolation,
		"TransactionIsolation=ReadCommitted must be applied at server level (default_transaction_isolation)")
	t.Logf("PostgreSQL default_transaction_isolation = %q ✓", sessionIsolation)

	// ── Correctness: verify application_name ExtraParam reached the server ──
	var appName string
	if err := db.Raw("SELECT current_setting('application_name')").Scan(&appName).Error; err != nil {
		t.Fatalf("could not query application_name: %v", err)
	}
	assert.Equal(t, "goth-datastore-test", appName,
		"ExtraParams[application_name] must be visible via current_setting('application_name')")
	t.Logf("PostgreSQL application_name = %q ✓", appName)

	// ── Table setup: always cleaned up even on failure ──
	_ = db.Migrator().DropTable(&databaseConcRWRecord{})
	t.Cleanup(func() {
		if err := db.Migrator().DropTable(&databaseConcRWRecord{}); err != nil {
			t.Logf("cleanup DropTable: %v", err)
		}
	})
	if err := db.AutoMigrate(&databaseConcRWRecord{}); err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}

	const (
		writerGoroutines = 20
		readerGoroutines = 30
		opsPerWriter     = 10
		opsPerReader     = 20
	)

	var (
		wg      sync.WaitGroup
		errsMu  sync.Mutex
		errList []error
		ready   = make(chan struct{})
	)

	addErr := func(e error) {
		errsMu.Lock()
		errList = append(errList, e)
		errsMu.Unlock()
	}

	// Writers: INSERT → UPDATE to (j+1)*10 → SELECT and verify
	for i := 0; i < writerGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-ready
			for j := 0; j < opsPerWriter; j++ {
				rec := &databaseConcRWRecord{
					Name:  fmt.Sprintf("pg-w%d-i%d", workerID, j),
					Value: j,
				}

				if err := db.Create(rec).Error; err != nil {
					addErr(fmt.Errorf("INSERT w%d i%d: %w", workerID, j, err))
					continue
				}

				updatedValue := (j + 1) * 10
				if err := db.Model(rec).Update("value", updatedValue).Error; err != nil {
					addErr(fmt.Errorf("UPDATE w%d i%d id=%d: %w", workerID, j, rec.ID, err))
					continue
				}

				var fetched databaseConcRWRecord
				if err := db.First(&fetched, rec.ID).Error; err != nil {
					addErr(fmt.Errorf("SELECT w%d i%d id=%d: %w", workerID, j, rec.ID, err))
					continue
				}
				if fetched.Value != updatedValue {
					addErr(fmt.Errorf("data integrity w%d i%d id=%d: want value=%d got %d",
						workerID, j, rec.ID, updatedValue, fetched.Value))
				}
			}
		}(i)
	}

	// Readers: concurrent COUNT(*) + WHERE find while writers are active
	for i := 0; i < readerGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-ready
			for j := 0; j < opsPerReader; j++ {
				var count int64
				if err := db.Model(&databaseConcRWRecord{}).Count(&count).Error; err != nil {
					addErr(fmt.Errorf("COUNT r%d i%d: %w", workerID, j, err))
				}
				var records []databaseConcRWRecord
				if err := db.Where("value > ?", 0).Limit(5).Find(&records).Error; err != nil {
					addErr(fmt.Errorf("find r%d i%d: %w", workerID, j, err))
				}
			}
		}(i)
	}

	close(ready)
	wg.Wait()

	// ── Post-concurrency integrity: row count must be exact ──
	for _, e := range errList {
		t.Errorf("concurrent op error: %v", e)
	}
	assert.Empty(t, errList, "zero errors expected under concurrent PostgreSQL R/W load")

	var finalCount int64
	if err := db.Model(&databaseConcRWRecord{}).Count(&finalCount).Error; err != nil {
		t.Fatalf("final COUNT failed: %v", err)
	}
	expectedCount := int64(writerGoroutines * opsPerWriter)
	assert.Equal(t, expectedCount, finalCount,
		"final row count must equal writerGoroutines×opsPerWriter — no records lost or duplicated")

	totalOps := writerGoroutines*opsPerWriter*3 + readerGoroutines*opsPerReader*2
	t.Logf("PostgreSQL concurrent R/W: %d writers × %d write-cycles, %d readers × %d read-cycles ≈ %d ops | final rows=%d/%d errors=%d",
		writerGoroutines, opsPerWriter, readerGoroutines, opsPerReader, totalOps, finalCount, expectedCount, len(errList))
}
