package mstore

import (
	"log"
	"time"
)

func runGC() {
	ticker := time.NewTicker(GC_INTERVAL)
	defer func() {
		ticker.Stop()
	}()

	for range ticker.C {
	again:
		if err := db.RunValueLogGC(DISCARD_RATIO); err != nil {
			msg := "data store garbage collection failed"
			// logger.Error(err, &msg)
			log.Print(msg)
		} else {
			goto again
		}
		db.Sync()
	}
}
