package flow

import (
	"time"

	"github.com/patrickmn/go-cache"
)

type ConfCacher interface {
	SetDefault(key string, value any)
	Set(key string, value any, expire time.Duration)
	Get(key string) (any, bool)
}

type confCacherLocal struct {
	*cache.Cache
}

func NewConfCacherLocal() *confCacherLocal {
	c := cache.New(5*time.Minute, 10*time.Minute)
	return &confCacherLocal{
		Cache: c,
	}
}

func (c *confCacherLocal) SetDefault(name string, value any) {
	c.Cache.SetDefault(name, value)
}

func (c *confCacherLocal) Set(name string, value any, expire time.Duration) {
	c.Cache.Set(name, value, expire)
}

func (c *confCacherLocal) Get(name string) (any, bool) {
	return c.Cache.Get(name)
}
