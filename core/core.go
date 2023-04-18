package core

import (
	"fmt"
)

// Version numbers
var MajorVersion = 0
var MinorVersion = 1
var PatchVersion = 0

// Version string
var Version = fmt.Sprintf("%d.%d.%d", MajorVersion, MinorVersion, PatchVersion)
