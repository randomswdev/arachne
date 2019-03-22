package engine

import (
	"sync"
	"time"

	"github.com/uber/arachne/collector"
	"github.com/uber/arachne/config"
	d "github.com/uber/arachne/defines"
	"github.com/uber/arachne/internal/ip"
	"github.com/uber/arachne/internal/log"
	"github.com/uber/arachne/internal/tcp"
	"github.com/uber/arachne/internal/util"
	"github.com/uber/arachne/metrics"
	"go.uber.org/zap"
)

type EngineMode int

const (
	EngineReceiveOnlyMode EngineMode = iota
	EngineSendOnlyMode
	EngineSendReceiveMode
)

type EngineCallbacks interface {
	FetchRemoteList(*config.Engine, <-chan struct{}, *log.Logger) (<-chan struct{}, error)
	FetchRemoteListNeeded()
	Stopping()
}

type Engine struct {
	conf             *config.Engine
	logger           *log.Logger
	callbacks        EngineCallbacks
	reporter         metrics.Reporter
	mode             EngineMode
	stopNotification <-chan struct{}
}

func NewEngine(conf *config.Engine, logger *log.Logger, c EngineCallbacks, sr metrics.Reporter,
	mode EngineMode, stopNotification <-chan struct{}) *Engine {
	return &Engine{
		conf:             conf,
		logger:           logger,
		callbacks:        c,
		reporter:         sr,
		mode:             mode,
		stopNotification: stopNotification,
	}
}

func (e *Engine) Run() {
	// Hold raw socket connection for IPv4 packets
	var connIPv4 *ip.Conn

	for {
		var (
			err                 error
			currentDSCP         ip.DSCPValue
			dnsWg               sync.WaitGroup
			finishedCycleUpload sync.WaitGroup
		)

		// Channels to tell goroutines to terminate
		killC := new(util.KillChannels)

		// If Orchestrator mode enabled, fetch JSON configuration file, otherwise try
		// to retrieve default local file
		configRefreshC, err := e.callbacks.FetchRemoteList(e.conf, e.stopNotification, e.logger)
		if err != nil {
			break
		}
		e.logger.Debug("Global JSON configuration", zap.Any("configuration", e.conf.RemoteConfig))

		if len(e.conf.Remotes) == 0 {
			e.logger.Debug("No targets to be echoed have been specified")
			e.mode = EngineReceiveOnlyMode
		}

		if e.conf.RemoteConfig.ResolveDNS && e.mode != EngineReceiveOnlyMode {
			// Refresh DNS resolutions
			dnsRefresh := time.NewTicker(d.DNSRefreshInterval)
			dnsWg.Add(1)
			killC.DNSRefresh = make(chan struct{})
			config.ResolveDNSTargets(e.conf.Remotes, e.conf.RemoteConfig, dnsRefresh, &dnsWg,
				killC.DNSRefresh, e.logger)
			dnsWg.Wait()
			e.logger.Debug("Remotes after DNS resolution include",
				zap.Int("count", len(e.conf.Remotes)),
				zap.Any("remotes", e.conf.Remotes))
		}

		// Channels for Collector to receive Probes and Responses from.
		sentC := make(chan tcp.Message, d.ChannelOutBufferSize)
		rcvdC := make(chan tcp.Message, d.ChannelInBufferSize)

		// Connection for IPv4 packets
		if connIPv4 == nil {
			connIPv4 = ip.NewConn(
				d.AfInet,
				e.conf.RemoteConfig.TargetTCPPort,
				e.conf.RemoteConfig.InterfaceName,
				e.conf.RemoteConfig.SrcAddress,
				e.logger)
		}

		// Actual echoing is a percentage of the total configured batch cycle duration.
		realBatchInterval := time.Duration(float32(e.conf.RemoteConfig.BatchInterval) *
			d.BatchIntervalEchoingPerc)
		uploadBatchInterval := time.Duration(float32(e.conf.RemoteConfig.BatchInterval) *
			d.BatchIntervalUploadStats)
		batchEndCycle := time.NewTicker(uploadBatchInterval)
		completeCycleUpload := make(chan bool, 1)

		if e.mode == EngineSendReceiveMode {
			// Start gathering and reporting results.
			killC.Collector = make(chan struct{})
			collector.Run(e.conf, sentC, rcvdC, e.conf.Remotes, &currentDSCP, reporter, completeCycleUpload,
				&finishedCycleUpload, killC.Collector, e.logger)
		}

		if e.mode != EngineSendOnlyMode {
			// Listen for responses or probes from other IPv4 arachne agents.
			killC.Receiver = make(chan struct{})
			err = tcp.Receiver(connIPv4, sentC, rcvdC, killC.Receiver, e.logger)
			if err != nil {
				e.logger.Fatal("IPv4 receiver failed to start", zap.Error(err))
			}
			e.logger.Debug("IPv4 receiver now ready...")
			//TODO IPv6 receiver
		}

		if e.mode != EngineReceiveOnlyMode {
			e.logger.Debug("Echoing...")
			// Start echoing all targets.
			killC.Echo = make(chan struct{})
			tcp.EchoTargets(e.conf, connIPv4, e.conf.RemoteConfig.TargetTCPPort,
				e.conf.RemoteConfig.SrcTCPPortRange, e.conf.RemoteConfig.QoSEnabled, &currentDSCP,
				realBatchInterval, batchEndCycle, sentC, e.mode != EngineReceiveOnlyMode,
				completeCycleUpload, &finishedCycleUpload, killC.Echo, e.logger)
		}

		select {
		case <-configRefreshC:
			util.CleanUpRefresh(killC, e.mode != EngineSendOnlyMode,
				e.mode != EngineReceiveOnlyMode, e.conf.RemoteConfig.ResolveDNS)
			e.callbacks.FetchRemoteListNeeded()
			e.logger.Info("Refreshing target list file, if needed")
		case <-e.stopNotification:
			e.logger.Debug("Engine stop requested")
			e.callbacks.Stopping()
			util.CleanUpRefresh(killC, e.mode != EngineSendOnlyMode, e.mode != EngineReceiveOnlyMode,
				e.conf.RemoteConfig.ResolveDNS)
			connIPv4.Close(e.logger)
			e.logger.Info("Exiting")
			break
		}
	}
}
