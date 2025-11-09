package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func getRandMS(l, r int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(r-l) + l
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
