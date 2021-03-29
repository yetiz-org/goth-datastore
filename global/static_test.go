package global

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlobal(t *testing.T) {
	Set("k", "v")
	Set("100", 100)
	assert.Equal(t, "v", Get("k"))
	assert.Equal(t, "100", GetString("100"))
	c := 0
	Range(func(key, value interface{}) bool {
		c++
		return true
	})

	assert.Equal(t, 2, c)
	assert.True(t, Exist("k"))
	assert.False(t, Exist("v"))
}
