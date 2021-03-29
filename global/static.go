package global

import (
	"fmt"
	"sync"
)

var (
	static Static
)

func Get(key string) interface{} {
	return static.Get(key)
}

func Exist(key string) bool {
	return static.Exist(key)
}

func Set(key string, value interface{}) {
	static.Set(key, value)
}

func Range(f func(key, value interface{}) bool) {
	static.params.Range(f)
}

func GetString(key string) string {
	return static.GetString(key)
}

type Static struct {
	params sync.Map
}

func (s *Static) Get(key string) interface{} {
	if val, found := s.params.Load(key); found {
		return val
	}

	return nil
}

func (s *Static) Exist(key string) bool {
	_, found := s.params.Load(key)
	return found
}

func (s *Static) Set(key string, value interface{}) *Static {
	s.params.Store(key, value)
	return s
}

func (s *Static) Range(f func(key, value interface{}) bool) {
	s.params.Range(f)
}

func (s *Static) GetString(key string) string {
	if v := s.Get(key); v != nil {
		return fmt.Sprintf("%v", v)
	}

	return ""
}
