package main

import (
	"fmt"
	"github.com/victorlin12345/cache-playground/cache"
	"github.com/victorlin12345/cache-playground/utils"
	"log"
	"time"
)

type Lego struct {
	ID          int64
	SeriesID    int64
	DisplayName string
}

func main() {

	cache := cache.NewCache()

	TOY1 := Lego{
		ID:          1234,
		SeriesID:    4321,
		DisplayName: "StarWar",
	}

	CacheKeyDataLockFormat := "%s:%s"

	lockKey := fmt.Sprintf(CacheKeyDataLockFormat, "TOY", TOY1.ID)
	// lock expired after 10 seconds. And will timeout after 10 seconds anyway else
	isAvail := cache.Lock(lockKey, 10, 10)

	if isAvail {

		// set "TOY" key with TOY1 and timeout 60 seconds to redis cache
		err := cache.CacheWithTimeout("TOY", utils.SerializeGoObjectGOB(TOY1), 60)
		if err != nil {
			log.Fatalf("cache error %s", err.Error())
		}

	}

	err := cache.RemoveLock(lockKey)

	if err != nil {
		log.Fatalf("redis lock error %s", err.Error())
	}

	timer := time.NewTimer(2 * time.Second)

	ch := make(chan bool)
	// run 30 seconds every 2 second retrieve key TOY from cache
	go func(timer *time.Timer) {
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				// read from cache by "TOY" key
				res := cache.Read("TOY")

				var resToy Lego
				err = utils.DeserializeGoObjectGOB(res, &resToy)

				if err != nil {
					log.Fatalf("deserialize error: %s", err.Error())
				}

				fmt.Println(resToy)
				timer.Reset(2 * time.Second)
			case stop := <-ch:
				if stop {
					fmt.Println("timer Stop")
					return
				}
			}
		}
	}(timer)

	time.Sleep(30 * time.Second)
	ch <- true
	close(ch)
	time.Sleep(1 * time.Second)
}
