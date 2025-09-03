// Package main provides development automation.
package main

import (
	_ "embed"

	//mage:import dev
	"github.com/advdv/stdgo/stdmage/stdmagedev"
)

func init() {
	stdmagedev.Init("dev")
}
