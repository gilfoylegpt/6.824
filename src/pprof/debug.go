package pprof

import (
	"net/http"
	_ "net/http/pprof"
	"runtime"
)

const pprofAddr string = ":7890"

func StartHTTPDebuger() {
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	http.ListenAndServe(pprofAddr, nil)
}
