package datastore

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ── envStr ────────────────────────────────────────────────────────────────────

func TestEnvStr(t *testing.T) {
	t.Run("overrides when env is set", func(t *testing.T) {
		v := "original"
		t.Setenv("_TEST_GOTH_STR", "changed")
		envStr("_TEST_GOTH_STR", &v)
		assert.Equal(t, "changed", v)
	})

	t.Run("preserves value when env is unset", func(t *testing.T) {
		os.Unsetenv("_TEST_GOTH_STR_UNSET")
		v := "original"
		envStr("_TEST_GOTH_STR_UNSET", &v)
		assert.Equal(t, "original", v)
	})

	t.Run("preserves value when env is empty string", func(t *testing.T) {
		t.Setenv("_TEST_GOTH_STR_EMPTY", "")
		v := "original"
		envStr("_TEST_GOTH_STR_EMPTY", &v)
		assert.Equal(t, "original", v)
	})

	t.Run("works with custom ~string type (DatabaseIsolationLevel)", func(t *testing.T) {
		v := DatabaseIsolationLevel("ReadUncommitted")
		t.Setenv("_TEST_GOTH_ISOLATION", "ReadCommitted")
		envStr("_TEST_GOTH_ISOLATION", &v)
		assert.Equal(t, DatabaseIsolationLevel("ReadCommitted"), v)
	})
}

// ── envInt ────────────────────────────────────────────────────────────────────

func TestEnvInt(t *testing.T) {
	t.Run("overrides when env is set", func(t *testing.T) {
		v := 4
		t.Setenv("_TEST_GOTH_INT", "16")
		envInt("_TEST_GOTH_INT", &v)
		assert.Equal(t, 16, v)
	})

	t.Run("preserves value when env is unset", func(t *testing.T) {
		os.Unsetenv("_TEST_GOTH_INT_UNSET")
		v := 4
		envInt("_TEST_GOTH_INT_UNSET", &v)
		assert.Equal(t, 4, v)
	})

	t.Run("preserves value when env is empty string", func(t *testing.T) {
		t.Setenv("_TEST_GOTH_INT_EMPTY", "")
		v := 4
		envInt("_TEST_GOTH_INT_EMPTY", &v)
		assert.Equal(t, 4, v)
	})

	t.Run("preserves value on non-numeric input", func(t *testing.T) {
		t.Setenv("_TEST_GOTH_INT_BAD", "notanint")
		v := 4
		envInt("_TEST_GOTH_INT_BAD", &v)
		assert.Equal(t, 4, v)
	})

	t.Run("preserves value on float input", func(t *testing.T) {
		t.Setenv("_TEST_GOTH_INT_FLOAT", "3.14")
		v := 4
		envInt("_TEST_GOTH_INT_FLOAT", &v)
		assert.Equal(t, 4, v)
	})

	t.Run("accepts zero", func(t *testing.T) {
		t.Setenv("_TEST_GOTH_INT_ZERO", "0")
		v := 99
		envInt("_TEST_GOTH_INT_ZERO", &v)
		assert.Equal(t, 0, v)
	})

	t.Run("accepts negative", func(t *testing.T) {
		t.Setenv("_TEST_GOTH_INT_NEG", "-1")
		v := 99
		envInt("_TEST_GOTH_INT_NEG", &v)
		assert.Equal(t, -1, v)
	})
}

// ── envBool ───────────────────────────────────────────────────────────────────

func TestEnvBool(t *testing.T) {
	cases := []struct {
		input    string
		expected bool
	}{
		{"true", true},
		{"false", false},
		{"1", true},
		{"0", false},
		{"TRUE", true},
		{"FALSE", false},
		{"True", true},
		{"False", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run("parses "+tc.input, func(t *testing.T) {
			v := !tc.expected
			t.Setenv("_TEST_GOTH_BOOL", tc.input)
			envBool("_TEST_GOTH_BOOL", &v)
			assert.Equal(t, tc.expected, v)
		})
	}

	t.Run("preserves value when env is unset", func(t *testing.T) {
		os.Unsetenv("_TEST_GOTH_BOOL_UNSET")
		v := true
		envBool("_TEST_GOTH_BOOL_UNSET", &v)
		assert.True(t, v)
	})

	t.Run("preserves value when env is empty string", func(t *testing.T) {
		t.Setenv("_TEST_GOTH_BOOL_EMPTY", "")
		v := true
		envBool("_TEST_GOTH_BOOL_EMPTY", &v)
		assert.True(t, v)
	})

	t.Run("preserves value on invalid input", func(t *testing.T) {
		t.Setenv("_TEST_GOTH_BOOL_BAD", "notabool")
		v := true
		envBool("_TEST_GOTH_BOOL_BAD", &v)
		assert.True(t, v)
	})
}

// ── init() env mapping integration ───────────────────────────────────────────

// TestDatabaseEnvOverrides verifies that every GOTH_DEFAULT_DATABASE_* env var
// correctly overrides its corresponding Default* variable, replicating the init()
// mapping without re-running init() itself.
func TestDatabaseEnvOverrides(t *testing.T) {
	// Save and restore all defaults after the test.
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
	origTransactionIsolation := DefaultDatabaseTransactionIsolation
	origPostgresSSLMode := DefaultDatabasePostgresSSLMode
	origPostgresTimeZone := DefaultDatabasePostgresTimeZone
	t.Cleanup(func() {
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
		DefaultDatabaseTransactionIsolation = origTransactionIsolation
		DefaultDatabasePostgresSSLMode = origPostgresSSLMode
		DefaultDatabasePostgresTimeZone = origPostgresTimeZone
	})

	// Set all GOTH_ env vars.
	t.Setenv("GOTH_DEFAULT_DATABASE_MAX_OPEN_CONN", "32")
	t.Setenv("GOTH_DEFAULT_DATABASE_MAX_IDLE_CONN", "8")
	t.Setenv("GOTH_DEFAULT_DATABASE_CONN_MAX_LIFETIME", "60000")
	t.Setenv("GOTH_DEFAULT_DATABASE_CONN_MAX_IDLE_TIME", "5000")
	t.Setenv("GOTH_DEFAULT_DATABASE_CHARSET", "latin1")
	t.Setenv("GOTH_DEFAULT_DATABASE_DIAL_TIMEOUT", "5s")
	t.Setenv("GOTH_DEFAULT_DATABASE_READ_TIMEOUT", "60s")
	t.Setenv("GOTH_DEFAULT_DATABASE_WRITE_TIMEOUT", "60s")
	t.Setenv("GOTH_DEFAULT_DATABASE_COLLATION", "utf8mb4_unicode_ci")
	t.Setenv("GOTH_DEFAULT_DATABASE_CLIENT_FOUND_ROWS", "true")
	t.Setenv("GOTH_DEFAULT_DATABASE_LOC", "UTC")
	t.Setenv("GOTH_DEFAULT_DATABASE_MAX_ALLOWED_PACKET", "50331648")
	t.Setenv("GOTH_DEFAULT_DATABASE_PARSE_TIME", "false")
	t.Setenv("GOTH_DEFAULT_DATABASE_MULTI_STATEMENTS", "true")
	t.Setenv("GOTH_DEFAULT_DATABASE_TRANSACTION_ISOLATION", "ReadCommitted")
	t.Setenv("GOTH_DEFAULT_DATABASE_POSTGRES_SSL_MODE", "require")
	t.Setenv("GOTH_DEFAULT_DATABASE_POSTGRES_TIME_ZONE", "Asia/Taipei")

	// Replay the same helper calls as init().
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

	assert.Equal(t, 32, DefaultDatabaseMaxOpenConn)
	assert.Equal(t, 8, DefaultDatabaseMaxIdleConn)
	assert.Equal(t, 60000, DefaultDatabaseConnMaxLifetime)
	assert.Equal(t, 5000, DefaultDatabaseConnMaxIdleTime)
	assert.Equal(t, "latin1", DefaultDatabaseCharset)
	assert.Equal(t, "5s", DefaultDatabaseDialTimeout)
	assert.Equal(t, "60s", DefaultDatabaseReadTimeout)
	assert.Equal(t, "60s", DefaultDatabaseWriteTimeout)
	assert.Equal(t, "utf8mb4_unicode_ci", DefaultDatabaseCollation)
	assert.Equal(t, true, DefaultDatabaseClientFoundRows)
	assert.Equal(t, "UTC", DefaultDatabaseLoc)
	assert.Equal(t, 50331648, DefaultDatabaseMaxAllowedPacket)
	assert.Equal(t, false, DefaultDatabaseParseTime)
	assert.Equal(t, true, DefaultDatabaseMultiStatements)
	assert.Equal(t, DatabaseIsolationLevel("ReadCommitted"), DefaultDatabaseTransactionIsolation)
	assert.Equal(t, "require", DefaultDatabasePostgresSSLMode)
	assert.Equal(t, "Asia/Taipei", DefaultDatabasePostgresTimeZone)
}

// TestDatabaseEnvOverrides_Partial verifies that unset env vars leave their
// corresponding defaults unchanged.
func TestDatabaseEnvOverrides_Partial(t *testing.T) {
	origMaxOpenConn := DefaultDatabaseMaxOpenConn
	origReadTimeout := DefaultDatabaseReadTimeout
	t.Cleanup(func() {
		DefaultDatabaseMaxOpenConn = origMaxOpenConn
		DefaultDatabaseReadTimeout = origReadTimeout
	})

	os.Unsetenv("GOTH_DEFAULT_DATABASE_MAX_OPEN_CONN")
	os.Unsetenv("GOTH_DEFAULT_DATABASE_READ_TIMEOUT")

	envInt("GOTH_DEFAULT_DATABASE_MAX_OPEN_CONN", &DefaultDatabaseMaxOpenConn)
	envStr("GOTH_DEFAULT_DATABASE_READ_TIMEOUT", &DefaultDatabaseReadTimeout)

	assert.Equal(t, origMaxOpenConn, DefaultDatabaseMaxOpenConn, "unset env must not change MaxOpenConn")
	assert.Equal(t, origReadTimeout, DefaultDatabaseReadTimeout, "unset env must not change ReadTimeout")
}

// TestDatabaseEnvOverrides_InvalidValues verifies that syntactically invalid env
// values are silently ignored and the original default is preserved.
func TestDatabaseEnvOverrides_InvalidValues(t *testing.T) {
	origMaxOpenConn := DefaultDatabaseMaxOpenConn
	origParseTime := DefaultDatabaseParseTime
	t.Cleanup(func() {
		DefaultDatabaseMaxOpenConn = origMaxOpenConn
		DefaultDatabaseParseTime = origParseTime
	})

	t.Setenv("GOTH_DEFAULT_DATABASE_MAX_OPEN_CONN", "not-a-number")
	t.Setenv("GOTH_DEFAULT_DATABASE_PARSE_TIME", "not-a-bool")

	envInt("GOTH_DEFAULT_DATABASE_MAX_OPEN_CONN", &DefaultDatabaseMaxOpenConn)
	envBool("GOTH_DEFAULT_DATABASE_PARSE_TIME", &DefaultDatabaseParseTime)

	assert.Equal(t, origMaxOpenConn, DefaultDatabaseMaxOpenConn, "invalid int env must not change MaxOpenConn")
	assert.Equal(t, origParseTime, DefaultDatabaseParseTime, "invalid bool env must not change ParseTime")
}
