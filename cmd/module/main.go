package main

import (
	sensor "go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/module"
	"go.viam.com/rdk/resource"
	"sensorbundle/resources/sensormonitor"
	"sensorbundle/resources/statefulsensor"
)

func main() {
	// ModularMain can take multiple APIModel arguments, if your module implements multiple models.
	module.ModularMain(
		resource.APIModel{API: sensor.API, Model: statefulsensor.Model},
		resource.APIModel{API: sensor.API, Model: sensormonitor.Model},
	)
}
