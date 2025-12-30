package cmd

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"pharos-exporter/internal"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

func runStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	rpcURL := fs.String("rpc", "https://atlantic-rpc.dplabs-internal.com/", "JSON-RPC endpoint")
	myBlsKey := fs.String("my-bls-key", "", "my BLS pubkey (0x...)")
	checkBlockProof := fs.Bool("check-block-proof", true, "check signedBlsKeys metrics")
	checkValidatorSet := fs.Bool("check-validator-set", true, "check validator set metrics")
	checkPropose := fs.Bool("check-propose", true, "check propose metrics")
	checkEndorse := fs.Bool("check-endorse", true, "check endorse metrics")
	logPath := fs.String("log-path", "", "path to log file to tail")
	logFromStart := fs.Bool("log-from-start", false, "start reading log from beginning (default: false)")
	rpcPollInterval := fs.Duration("rpc-poll-interval", time.Second, "poll interval for latest block")
	logPollInterval := fs.Duration("log-poll-interval", time.Second, "poll interval for log tailing")
	exporterPort := fs.String("exporter-port", "9123", "metrics listen port")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *logPath == "" {
		return errors.New("log-path is required")
	}

	internal.RegisterMetrics()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)

	tracker, err := internal.NewBlockTracker(internal.BlockTrackerConfig{
		RPCURL:            *rpcURL,
		MyBlsKey:          *myBlsKey,
		CheckBlockProof:   *checkBlockProof,
		CheckValidatorSet: *checkValidatorSet,
		PollInterval:      *rpcPollInterval,
	})
	if err != nil {
		return err
	}
	g.Go(func() error {
		return tracker.Start(gctx)
	})

	tailer, err := internal.NewLogTailer(internal.LogTailerConfig{
		Path:         *logPath,
		PollInterval: *logPollInterval,
		Output:       os.Stdout,
		FromStart:    *logFromStart,
		CheckPropose: *checkPropose,
		CheckEndorse: *checkEndorse,
	})
	if err != nil {
		return err
	}
	g.Go(func() error {
		return tailer.Start(gctx)
	})

	log.Printf("Metrics exposed at http://%s:%s/metrics", resolvePublicIP(), *exporterPort)
	server := &http.Server{
		Addr:    ":" + *exporterPort,
		Handler: promhttp.Handler(),
	}
	g.Go(func() error {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func resolvePublicIP() string {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("https://ifconfig.me/ip")
	if err == nil {
		defer resp.Body.Close()
		b, rerr := io.ReadAll(resp.Body)
		if rerr == nil {
			ip := strings.TrimSpace(string(b))
			if ip != "" {
				return ip
			}
		}
	}
	return "127.0.0.1"
}
