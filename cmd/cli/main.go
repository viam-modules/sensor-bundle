package main

import (
	"context"
	sensor "go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	"sensorbundle/resources/statefulsensor"
)

func main() {
	err := realMain()
	if err != nil {
		panic(err)
	}
}

func realMain() error {
	ctx := context.Background()
	logger := logging.NewLogger("cli")

	deps := resource.Dependencies{}
	// can load these from a remote machine if you need

	cfg := statefulsensor.Config{}

	thing, err := statefulsensor.New(ctx, deps, sensor.Named("foo"), &cfg, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := thing.Close(ctx); err != nil {
			logger.Warnf("closing resource: %v", err)
		}
	}()

	return nil
}
