package raft

import (
	"log"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Log format
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStartTime time.Time
var level int

type logTopic string
const (
	dClient logTopic = "CLNT"
	dCommit logTopic = "CMIT"
	dLeader logTopic = "LEAD"
)

func init() {
	level = getVerbosity()
	debugStartTime = time.Now()
	
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a...interface{}) {
	if level >= 1 {
		time := time.Since(debugStartTime).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}