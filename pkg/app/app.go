package app

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/free5gc/go-upf/internal/ees"
	"github.com/free5gc/go-upf/internal/forwarder"
	"github.com/free5gc/go-upf/internal/forwarder/perio"
	"github.com/free5gc/go-upf/internal/logger"
	"github.com/free5gc/go-upf/internal/pfcp"
	"github.com/free5gc/go-upf/pkg/factory"
)

type UpfApp struct {
	ctx        context.Context
	wg         sync.WaitGroup
	cfg        *factory.Config
	driver     forwarder.Driver
	pfcpServer *pfcp.PfcpServer
}

func NewApp(cfg *factory.Config) (*UpfApp, error) {
	upf := &UpfApp{
		cfg: cfg,
	}
	upf.SetLogLevel(cfg.Logger.Level)
	upf.SetLogReportCaller(cfg.Logger.ReportCaller)
	return upf, nil
}

func (u *UpfApp) Config() *factory.Config {
	return u.cfg
}

func (a *UpfApp) SetLogLevel(level string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		logger.MainLog.Warnf("Log level [%s] is invalid", level)
		return
	}

	logger.MainLog.Infof("Log level is set to [%s]", level)
	if lvl == logger.Log.GetLevel() {
		return
	}

	logger.Log.SetLevel(lvl)
}

func (a *UpfApp) SetLogReportCaller(reportCaller bool) {
	logger.MainLog.Infof("Report Caller is set to [%v]", reportCaller)
	if reportCaller == logger.Log.ReportCaller {
		return
	}

	logger.Log.SetReportCaller(reportCaller)
}

/*
	func (u *UpfApp) Run() error {
		var cancel context.CancelFunc
		u.ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		u.wg.Add(1)
		// Go Routine is spawned here for listening for cancellation event on
		// context
		go u.listenShutdownEvent()

		var err error
		u.driver, err = forwarder.NewDriver(&u.wg, u.cfg)
		if err != nil {
			return err
		}

		u.pfcpServer = pfcp.NewPfcpServer(u.cfg, u.driver)
		u.driver.HandleReport(u.pfcpServer)
		u.pfcpServer.Start(&u.wg)

		logger.MainLog.Infoln("UPF started")

		// Wait for interrupt signal to gracefully shutdown
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh

		// Receive the interrupt signal
		logger.MainLog.Infof("Shutdown UPF ...")
		// Notify each goroutine and wait them stopped
		cancel()
		u.WaitRoutineStopped()
		logger.MainLog.Infof("UPF exited")
		return nil
	}
*/
func (u *UpfApp) listenShutdownEvent() {
	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.MainLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}

		u.wg.Done()
	}()

	<-u.ctx.Done()
	if u.pfcpServer != nil {
		u.pfcpServer.Stop()
	}
	if u.driver != nil {
		u.driver.Close()
	}
}

func (u *UpfApp) WaitRoutineStopped() {
	u.wg.Wait()
	u.Terminate()
}

func (u *UpfApp) Start() {
	if err := u.Run(); err != nil {
		logger.MainLog.Errorf("UPF Run err: %v", err)
	}
}

func (u *UpfApp) Terminate() {
	logger.MainLog.Infof("Terminating UPF...")
	logger.MainLog.Infof("UPF terminated")
}

func (u *UpfApp) Run() error {
	var cancel context.CancelFunc

	u.ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	u.wg.Add(1)
	// Go Routine is spawned here for listening for cancellation event on
	// context
	go u.listenShutdownEvent()
	// ... (Original context setting) ...

	var err error
	u.driver, err = forwarder.NewDriver(&u.wg, u.cfg)
	if err != nil {
		return err
	}

	u.pfcpServer = pfcp.NewPfcpServer(u.cfg, u.driver)

	// [New] Dispatcher wiring
	reportDispatcher := NewDispatcher(u.pfcpServer)
	u.driver.HandleReport(reportDispatcher)

	u.pfcpServer.Start(&u.wg)

	// =========================================================================
	// [New] EES initialization logic — branches on Mode: vanilla vs pseudo
	// =========================================================================
	if u.cfg.EES != nil && u.cfg.EES.Enabled {
		logger.MainLog.Infof("Starting EES Module (Hybrid Mode)...")

		// 1. Use existing EesLog
		eesLogger := logger.EesLog

		// 2. Create Store / Notifier (shared by both modes)
		subscriptionStore := ees.NewSubscriptionStore("")
		notifier := ees.NewNotifier(eesLogger)

		period := 10
		if u.cfg.EES.PeriodSec > 0 {
			period = u.cfg.EES.PeriodSec
		}

		listenAddr := u.cfg.EES.ListenAddr
		if listenAddr == "" {
			listenAddr = ":8088"
		}

		// 3. Initialize PseudoDriver (Default to "pre_data" if not set)
		var pseudoDriver *ees.PseudoDriver
		parquetDir := u.cfg.EES.ParquetDir
		if parquetDir == "" {
			parquetDir = "pre_data"
		}
		if _, err := os.Stat(parquetDir); !os.IsNotExist(err) {
			pseudoDriver = ees.NewPseudoDriver(parquetDir, notifier, eesLogger)
			logger.MainLog.Infof("EES PseudoDriver (Hybrid Mode) enabled with parquet directory: %s", parquetDir)
		} else {
			logger.MainLog.Infof("EES PseudoDriver disabled (parquet directory not found: %s)", parquetDir)
		}

		// 4. Initialize Kernel integration components
		// Pure Push mode: Reports come from kernel
		localNode := u.pfcpServer.GetLocalNode()
		sessionProvider := localNode

		// Get perioServer from driver for URR period queries
		var perioServer *perio.Server
		if gtp5gDriver, ok := u.driver.(*forwarder.Gtp5g); ok {
			perioServer = gtp5gDriver.GetPerioServer()
		}

		// 5. Create Aggregator (Handles both kernel and pseudoDriver data)
		aggregator := ees.NewAggregator(
			subscriptionStore,
			time.Duration(period)*time.Second,
			notifier,
			eesLogger,
			sessionProvider,
			perioServer,
			pseudoDriver,
		)

		if pseudoDriver != nil {
			pseudoDriver.SetAggregator(aggregator)
		}

		// 6. Register EES Handler to Dispatcher (kernel → Aggregator)
		eesHandler := ees.NewHandler(aggregator, eesLogger)
		reportDispatcher.RegisterEESHandler(eesHandler, aggregator)

		// 7. Register perioServer callback
		if perioServer != nil {
			reportDispatcher.SetPerioServer(perioServer)

			// Register callback to adjust aggregator period when URR is added
			perioServer.SetOnURRAdded(func(urrid uint32, period time.Duration) {
				if urrid == 2 {
					logger.MainLog.Infof("EES: URR %d added with period %v, triggering aggregator adjustment", urrid, period)
					aggregator.AdjustReportPeriod(period)
				}
			})
		}

		// 8. Start Aggregator (Handles background buffering/dispatching)
		go aggregator.Run(u.ctx)

		// 9. Start API Server
		apiServer := ees.NewServer(subscriptionStore, aggregator, eesLogger, pseudoDriver)
		go func() {
			if err := apiServer.Serve(listenAddr); err != nil {
				logger.MainLog.Errorf("EES API Server Error: %v", err)
			}
		}()

		logger.MainLog.Infof("EES started at %s with period %ds (Hybrid Parallel Mode)", listenAddr, period)
	}
	// =========================================================================

	logger.MainLog.Infoln("UPF started")

	// ... (Subsequent Signal handling remains unchanged)
	// Wait for interrupt signal to gracefully shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	// Receive the interrupt signal
	logger.MainLog.Infof("Shutdown UPF ...")
	// Notify each goroutine and wait them stopped
	cancel()
	u.WaitRoutineStopped()
	logger.MainLog.Infof("UPF exited")
	return nil
}
