package core

import (
	"fmt"
	"time"

	"dts/config"
)

// Version numbers
var MajorVersion = 0
var MinorVersion = 1
var PatchVersion = 0

// Version string
var Version = fmt.Sprintf("%d.%d.%d", MajorVersion, MinorVersion, PatchVersion)

// Indicates whether core.Init() has been called
var initialized = false

// The time the application started.
var startTime time.Time

// Initializes application utilities.
func Init(yamlConfig []byte) error {

	if !initialized {
		startTime = time.Now()
		initialized = true
	}
	return config.Init(yamlConfig)
}

// Returns the application's uptime in seconds.
func Uptime() float64 {
	return time.Since(startTime).Seconds()
}
