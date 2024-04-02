package main

import (
	"sync"
	"time"
)

var (
	mem    = map[string]any{}
	expire = map[string]int64{} // [string]int64{}, key -> unix millisecond

	expireJobOnce sync.Once
)

func SetExpire(k string, exp int64) {
	Debugf("Set %v TTL %v", k, exp)
	expire[k] = exp
}

func CalcExp(exp int64, dur time.Duration) int64 {
	return time.Now().Add(dur * time.Duration(exp)).UnixMilli()
}

func DelExpired(k string) {
	v, ok := expire[k]
	if !ok {
		return
	}
	if IsTimeExpired(v) {
		Debugf("Key %v found expired in explicit check", k)
		QueueDelete(k)
	}
}

func QueueDelete(k string) {
	QueueCommand(&Command{
		typ: ServerCommand,
		action: func() {
			DelKey(k)
		},
	})
}

func DelKey(k string) {
	delete(expire, k)
	delete(mem, k)
}

func ScheduleExpire() {
	expireJobOnce.Do(func() {
		go func() {
			t := time.NewTicker(100 * time.Millisecond) // 10 times / second
			defer t.Stop()
			for range t.C {
				runBatchExpire()
			}
		}()
	})
}

func runBatchExpire() {
	start := time.Now().UnixMilli()
	n := 0

	// ranging map (read-only) is actually thread-safe as long as we don't blindly modify the map without applying any synchronization
	for k := range expire {

		// check time for every 15 iteration
		if n > 14 {

			// has been running for over 5ms
			if time.Now().UnixMilli()-start > 5 {
				return
			}
			n = 0
		}
		DelExpired(k)
		n += 1
	}
}

func Lookup(k string) (any, bool) {
	mv, ok := mem[k]
	if ok {
		ev, eok := expire[k]
		if eok && IsTimeExpired(ev) {
			Debugf("Key %v found expired during lookup", k)
			DelKey(k)
			return nil, false
		}
	}
	return mv, ok
}

func SetVal(k string, v any) {
	mem[k] = v
}

func IsTimeExpired(n int64) bool {
	return time.Now().UnixMilli()-n > 0
}

func LoadTTL(k string, millisec bool) int64 {
	v, ok := expire[k]
	if ok {
		gap := v - time.Now().UnixMilli()
		if gap < 0 {
			return -2 // key not found
		}
		if millisec {
			return gap
		} else {
			return int64(float64(gap) / float64(1000))
		}
	}
	return -1 // have no associated expire
}
