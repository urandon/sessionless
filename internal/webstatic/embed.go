package webstatic

import "embed"

// The frontend build is copied into dist before compiling web-bff. Keeping the
// embed root in this package makes the runtime image independent of Node.
//
//go:embed all:dist
var embeddedAssets embed.FS
