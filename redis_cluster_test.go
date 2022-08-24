package diseven

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

var (
	redisDbKVRead  *redis.ClusterClient
	redisDbKVWrite *redis.ClusterClient
)

var addrHostsRead = []string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"}
var addrHostsWrite = []string{":7000", ":7001", ":7002"}
var ctx = context.Background()

func init() {
	redisDbKVRead = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:         addrHostsRead,
		DialTimeout:   10 * time.Second,
		ReadTimeout:   30 * time.Second,
		WriteTimeout:  30 * time.Second,
		RouteRandomly: true,
	})

	redisDbKVWrite = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        addrHostsWrite,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	})

	// check Redis
	if redisDbKVRead.Incr(context.Background(), "REDIS_PING").Val() <= 0 {
		log.Fatalln("redisDbKVRead not valid")
	}

	if redisDbKVWrite.Incr(context.Background(), "REDIS_PING").Val() <= 0 {
		log.Fatalln("redisDbKVWrite not valid")
	}

	log.Println("Connected to redis cluster")
}

func Test(t *testing.T) {
	d := mustNewDisEvent(3)
	keyHashTag := d.GenKeyWithHashTag(fmt.Sprintf("key1"))
	if err := redisDbKVWrite.MSet(ctx, keyHashTag, "value").Err(); err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, "value", redisDbKVRead.Get(ctx, keyHashTag).Val())
	log.Println("Set Success")
}

func BenchmarkPutKey(b *testing.B) {
	d := mustNewDisEvent(3)
	var num = 0
	for num < 100000 {
		keyHashTag := d.GenKeyWithHashTag(fmt.Sprintf("key%d", num))
		if err := redisDbKVWrite.MSet(ctx, keyHashTag, fmt.Sprintf("value%d", num)).Err(); err != nil {
			b.Error(err)
			return
		}
		num++
	}
}

func BenchmarkGetKey(b *testing.B) {
	d := mustNewDisEvent(3)
	var num = 0
	var keys []string
	for num < 100000 {
		keys = append(keys, fmt.Sprintf("key%d", num))
		num++
	}
	objectKeyHash := d.GetMultiKeyQuery(keys)
	for k, v := range objectKeyHash {
		fmt.Printf("Query multi key with hash tag %v \n", int(k))
		resp := redisDbKVRead.MGet(ctx, v...)
		if err := resp.Err(); err != nil {
			b.Error(err)
			continue
		}
	}
}
