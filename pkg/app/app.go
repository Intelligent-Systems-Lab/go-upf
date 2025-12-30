package app

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/free5gc/go-upf/internal/forwarder"
	"github.com/free5gc/go-upf/internal/logger"
	"github.com/free5gc/go-upf/internal/pfcp"
	"github.com/free5gc/go-upf/pkg/factory"

	"time" // [新增]

	"github.com/free5gc/go-upf/internal/ees" // [新增]
	"go.uber.org/zap"                        // [新增]
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
	// ... (原有 context 設定) ...

	var err error
	u.driver, err = forwarder.NewDriver(&u.wg, u.cfg)
	if err != nil {
		return err
	}

	u.pfcpServer = pfcp.NewPfcpServer(u.cfg, u.driver)
	u.driver.HandleReport(u.pfcpServer)
	u.pfcpServer.Start(&u.wg)

	// =========================================================================
	// [新增] EES 初始化邏輯 (遷移自 main.go，並改為依賴注入)
	// =========================================================================
	if u.cfg.EES != nil && u.cfg.EES.Enabled {
		logger.MainLog.Infoln("Starting EES Module...")

		// 1. 建立 Logger
		eesLogger, _ := zap.NewDevelopment() // 簡單處理 error

		// 2. 建立 Active Source (依賴注入核心)
		// 這裡注入了 u.driver (ForwarderDriver) 和 u.pfcpServer.LocalNode (SessionProvider)
		// 注意：需確保 pfcpServer 暴露了 LocalNode，或是透過 Getter 獲取
		// 假設 pfcpServer 結構中 lnode 是 public (Lnode) 或有 GetLocalNode() 方法
		// 由於你的 pfcpServer 定義 lnode 是小寫 (private) ，
		// 你可能需要先去 internal/pfcp/pfcp.go 增加一個 GetLocalNode() 方法，
		// 或者暫時將 lnode 改為 Lnode (Public)。
		// 這裡假設你加了一個 GetLocalNode()：

		pfcpSource := ees.NewActivePFCPSource(u.driver, u.pfcpServer.GetLocalNode())

		// 3. 建立 Store / Notifier / Aggregator
		subscriptionStore := ees.NewSubscriptionStore("")
		notifier := ees.NewNotifier(eesLogger)

		period := 10
		if u.cfg.EES.PeriodSec > 0 {
			period = u.cfg.EES.PeriodSec
		}

		aggregator := ees.NewAggregator(
			pfcpSource, // 傳入新的 Active Source
			subscriptionStore,
			time.Duration(period)*time.Second,
			notifier,
			eesLogger,
		)

		// 4. 啟動 Aggregator 和 API Server
		// 注意：這裡應該使用 u.wg 來管理 goroutine，或使用獨立的 context
		go aggregator.Run(u.ctx)

		listenAddr := u.cfg.EES.ListenAddr
		if listenAddr == "" {
			listenAddr = ":8088"
		}
		apiServer := ees.NewServer(subscriptionStore, aggregator, eesLogger)

		go func() {
			if err := apiServer.Serve(listenAddr); err != nil {
				logger.MainLog.Errorf("EES API Server Error: %v", err)
			}
		}()

		logger.MainLog.Infof("EES started at %s with period %ds", listenAddr, period)
	}
	// =========================================================================

	logger.MainLog.Infoln("UPF started")

	// ... (後續 Signal 處理保持不變)
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
