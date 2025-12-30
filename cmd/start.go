package cmd

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pharos-exporter/internal"

	"golang.org/x/sync/errgroup"
)

func runStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	rpcURL := fs.String("rpc", "https://atlantic-rpc.dplabs-internal.com/", "JSON-RPC endpoint")
	myBlsKey := fs.String("my-bls-key", "", "my BLS pubkey (0x...)")
	checkBlockProof := fs.Bool("check-block-proof", true, "check signedBlsKeys trigger")
	checkValidatorSet := fs.Bool("check-validator-set", true, "check validator set trigger")
	logPath := fs.String("log-path", "", "path to log file to tail")
	logFromStart := fs.Bool("log-from-start", false, "start reading log from beginning (default: false)")
	rpcPollInterval := fs.Duration("rpc-poll-interval", time.Second, "poll interval for latest block")
	logPollInterval := fs.Duration("log-poll-interval", time.Second, "poll interval for log tailing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *logPath == "" {
		return errors.New("log-path is required")
	}

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
	})
	if err != nil {
		return err
	}
	g.Go(func() error {
		return tailer.Start(gctx)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
