// Example stream-server serves a ledger track over HTTP as HLS and DASH, wiring
// the public stream package to a local filesystem store and the standard
// library's HTTP server. It is a development example — no auth, no TLS, proxy
// delivery only — not a production server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store/fsstore"
	"github.com/okdaichi/qumo-ledger/stream"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "stream-server:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("stream-server", flag.ExitOnError)
	root := flags.String("root", ".", "storage root directory")
	track := flags.String("track", "", "track path, for example live/cam1/video")
	addr := flags.String("addr", ":8080", "listen address")
	init := flags.String("init", "", "path to an fMP4 init segment to serve (optional)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *track == "" {
		flags.Usage()
		return fmt.Errorf("-track is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	objects, err := fsstore.New(*root)
	if err != nil {
		return err
	}

	opened, err := ledger.Open(ctx, objects, ledger.TrackPath(*track), ledger.Config{Logger: slog.Default()})
	if err != nil {
		return err
	}

	opts := stream.Options{}
	if *init != "" {
		bytes, err := os.ReadFile(*init)
		if err != nil {
			return fmt.Errorf("read init segment: %w", err)
		}
		opts.InitSegment = stream.InitSegment{Bytes: bytes}
	}

	handler, err := stream.NewHandler(opened, opts)
	if err != nil {
		return err
	}

	httpServer := &http.Server{Addr: *addr, Handler: handler}
	go func() {
		<-ctx.Done()
		// not actionable: Shutdown's error only reports a closed listener, and
		// the process is exiting anyway.
		_ = httpServer.Shutdown(context.Background())
	}()

	base := "http://" + *addr
	fmt.Printf("serving %s as HLS and DASH\n", *track)
	fmt.Printf("  HLS:  %s/%s/playlist.m3u8\n", base, *track)
	fmt.Printf("  DASH: %s/%s/manifest.mpd\n", base, *track)

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
