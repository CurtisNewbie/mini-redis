package main

import (
	"sync"
	"time"
)

var (
	mem    = map[string]any{}
	expire = &sync.Map{} // [string]int64{}, key -> unix millisecond

	expireJobOnce sync.Once
)

func SetExpire(k string, exp int64) {
	expire.Store(k, exp)
}

func CalcExp(exp int64, dur time.Duration) int64 {
	return time.Now().Add(dur * time.Duration(exp)).UnixMilli()
}

func DelExpired(k string) {
	v, ok := expire.Load(k)
	if !ok {
		return
	}
	exp := v.(int64)
	now := time.Now().UnixMilli()
	if now-exp > 0 {
		QueueCommand(&Command{
			typ: ServerCommand,
			action: func() {
				Debugf("Key %v expired, now %v, exp: %v", k, now, exp)
				expire.Delete(k)
				delete(mem, k)
			},
		})
	}
}

func ScheduleExpire() {
	expireJobOnce.Do(func() {
		go func() {
			t := time.NewTicker(100 * time.Millisecond) // 10 times / second
			defer t.Stop()
			for {
				select {
				case <-t.C:
					runBatchExpire()
				}
			}
		}()
	})
}

func runBatchExpire() {
	start := time.Now().UnixMilli()
	n := 0
	expire.Range(func(key, value any) bool {
		// check time for every 15 iteration
		if n > 14 {
			n = 0
			// has been running for over 5ms
			if time.Now().UnixMilli()-start > 5 {
				return false
			}
		}
		DelExpired(key.(string))
		n += 1
		return true
	})
}
