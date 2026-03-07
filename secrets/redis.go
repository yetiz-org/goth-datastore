package secret

import (
	"fmt"
	"strings"
)

const (
	RedisModeSingle      = "single"
	RedisModeReplication = "replication"
	RedisModeCluster     = "cluster"
)

type Redis struct {
	DefaultSecret
	Mode     string             `json:"mode"`
	Username string             `json:"username"`
	Password string             `json:"password"`
	DB       int                `json:"db"`
	Master   RedisMeta          `json:"master"`
	Slave    RedisMeta          `json:"slave"`
	Cluster  RedisClusterSecret `json:"cluster"`
}

type RedisMeta struct {
	Host string `json:"host"`
	Port uint   `json:"port"`
}

type RedisClusterSecret struct {
	Addrs          []string `json:"addrs"`
	ReadOnly       bool     `json:"read_only"`
	RouteByLatency bool     `json:"route_by_latency"`
	RouteRandomly  bool     `json:"route_randomly"`
}

type RedisProfile = Redis

func LoadRedisProfile(name string) (*Redis, error) {
	return LoadRedisProfileWithFS(name, defaultFS)
}

func LoadRedisProfileWithFS(name string, fs FileSystem) (*Redis, error) {
	profile := &Redis{}
	if err := LoadWithFS("redis", name, profile, fs); err != nil {
		return nil, err
	}
	profile.Normalize()
	return profile, nil
}

func (p *Redis) Normalize() {
	p.Mode = strings.ToLower(strings.TrimSpace(p.Mode))
	if p.Mode == "" {
		if len(p.Cluster.Addrs) > 0 {
			p.Mode = RedisModeCluster
		} else if p.Master.Host != "" && p.Slave.Host != "" && !sameRedisMeta(p.Master, p.Slave) {
			p.Mode = RedisModeReplication
		} else {
			p.Mode = RedisModeSingle
		}
	}

	if p.Mode == RedisModeCluster {
		p.Cluster.Addrs = normalizeRedisAddrs(p.Cluster.Addrs)
		return
	}

	if p.Slave.Host == "" {
		p.Slave = p.Master
	}
}

func (p *Redis) MasterAddrs() []string {
	if p.Mode == RedisModeCluster {
		return append([]string(nil), p.Cluster.Addrs...)
	}
	if p.Master.Host == "" {
		return nil
	}
	return []string{fmt.Sprintf("%s:%d", p.Master.Host, p.Master.Port)}
}

func (p *Redis) SlaveAddrs() []string {
	if p.Mode == RedisModeCluster {
		return append([]string(nil), p.Cluster.Addrs...)
	}
	if p.Slave.Host == "" {
		return nil
	}
	return []string{fmt.Sprintf("%s:%d", p.Slave.Host, p.Slave.Port)}
}

func normalizeRedisAddrs(addrs []string) []string {
	result := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		trimmed := strings.TrimSpace(addr)
		if trimmed == "" {
			continue
		}
		result = append(result, trimmed)
	}
	return result
}

func sameRedisMeta(a, b RedisMeta) bool {
	return strings.TrimSpace(a.Host) == strings.TrimSpace(b.Host) && a.Port == b.Port
}
