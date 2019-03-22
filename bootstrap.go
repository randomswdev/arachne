// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package arachne

import (
	coreLog "log"
	"os"
	"time"

	"github.com/uber/arachne/config"
	d "github.com/uber/arachne/defines"
	"github.com/uber/arachne/internal/log"
	"github.com/uber/arachne/internal/util"
	"github.com/uber/arachne/pkg/engine"

	"go.uber.org/zap"
)

// Run is the entry point for initiating any Arachne service.
func Run(ec *config.Extended, opts ...Option) {
	var (
		gl  config.Global
		err error
	)

	bl, err := zap.NewProduction()
	if err != nil {
		coreLog.Fatal(err)
	}
	bootstrapLogger := &log.Logger{
		Logger:    bl,
		PIDPath:   "",
		RemovePID: util.RemovePID,
	}

	util.PrintBanner()

	gl.CLI = config.ParseCliArgs(bootstrapLogger, d.ArachneService, d.ArachneVersion)
	apply(&gl, opts...)
	gl.App, err = config.Get(gl.CLI, ec, bootstrapLogger)
	if err != nil {
		bootstrapLogger.Error("error reading the configuration file",
			zap.String("file", *gl.CLI.ConfigFile),
			zap.Error(err))
		os.Exit(1)
	}

	logger, err := log.CreateLogger(gl.App.Logging, gl.App.PIDPath, util.RemovePID)
	if err != nil {
		bootstrapLogger.Fatal("unable to initialize Arachne Logger", zap.Error(err))
		os.Exit(1)
	}

	// Channel to be informed if Unix signal has been received.
	sigC := make(chan struct{}, 1)
	util.UnixSignals(sigC, logger)

	// Check if another Arachne process is already running.
	// Pass bootstrapLogger so that the arachne PID file is not removed.
	if err := util.CheckPID(gl.App.PIDPath, bootstrapLogger); err != nil {
		os.Exit(1)
	}

	sr, err := gl.App.Metrics.NewReporter(logger.Logger)
	if err != nil {
		logger.Error("error initializing stats", zap.Error(err))
	}

	engineCallbacks := newEngineCallbacks(&gl, logger)

	engine := engine.NewEngine(&gl.Engine, logger, engineCallbacks, sr, engineModeFromCLIConfig(gl.CLI), sigC)

	logger.Info("Starting up arachne")

	engine.Run()

	// Clean-up
	sr.Close()
	util.RemovePID(gl.App.PIDPath, logger)

	logger.Info("Exiting arachne")

	os.Exit(0)
}

type engineCallbacks struct {
	configRefreshTicker *time.Ticker
	gl                  *config.Global
	logger              *log.Logger
}

func newEngineCallbacks(gl *config.Global, logger *log.Logger) *engineCallbacks {
	return &engineCallbacks{
		gl:     gl,
		logger: logger,
	}
}

func (ec *engineCallbacks) FetchRemoteList(_ *config.Engine, stopChannel <-chan struct{}, logger *log.Logger) (<-chan struct{}, error) {
	err := config.FetchRemoteList(ec.gl, d.MaxNumRemoteTargets, d.MaxNumSrcTCPPorts,
		d.MinBatchInterval, d.HTTPResponseHeaderTimeout, d.OrchestratorRESTConf, stopChannel, ec.logger)
	if err != nil {
		return nil, err
	}

	ec.configRefreshTicker = time.NewTicker(ec.gl.RemoteConfig.PollOrchestratorInterval.Success)

	ch := make(chan struct{})

	go func() {
		select {
		case <-ec.configRefreshTicker.C:
			ch <- struct{}{}
		}
	}()

	return ch, err
}

func (ec *engineCallbacks) FetchRemoteListNeeded() {
	ec.configRefreshTicker.Stop()
	log.ResetLogFiles(ec.gl.App.Logging.OutputPaths, d.LogFileSizeMaxMB, d.LogFileSizeKeepKB, ec.logger)
}

func (ec *engineCallbacks) Stopping() {
	ec.configRefreshTicker.Stop()
}

func engineModeFromCLIConfig(conf *config.CLIConfig) engine.EngineMode {
	if *conf.ReceiverOnlyMode {
		if *conf.SenderOnlyMode {
			return engine.EngineSendReceiveMode
		}
		return engine.EngineReceiveOnlyMode
	}
	return engine.EngineSendOnlyMode
}
