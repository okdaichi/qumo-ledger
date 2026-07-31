// Command qumo-ledger inspects tracks stored by the ledger.
//
// Because a track is readable with nothing but object-store access, this tool
// is a convenience rather than a component: it uses the same public API any
// reader would, and nothing in the storage format depends on it existing.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/okdaichi/qumo-ledger/internal/version"
	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/objectstore/fsstore"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "qumo-ledger:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch args[0] {
	case "inspect":
		return inspect(ctx, args[1:])
	case "follow":
		return follow(ctx, args[1:])
	case "version":
		info := version.Get()
		fmt.Printf("qumo-ledger %s (%s, built %s, %s)\n", info.Version, info.Commit, info.Date, info.Go)
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `qumo-ledger — inspect temporal tracks

Usage:
  qumo-ledger inspect -root <dir> -track <path>   summarize a track
  qumo-ledger follow  -root <dir> -track <path>   tail commits as they land
  qumo-ledger version

Only the local filesystem backend is wired up so far. Object-store backends
implement the same interface, so nothing here is specific to local storage.
`)
}

func inspect(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("inspect", flag.ExitOnError)
	root := flags.String("root", ".", "storage root directory")
	track := flags.String("track", "", "track path, for example live/cam1/video")
	groups := flags.Bool("groups", false, "list every group rather than a summary")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *track == "" {
		return fmt.Errorf("-track is required")
	}

	reader, err := openReader(ctx, *root, *track)
	if err != nil {
		return err
	}

	manifest := reader.Root()
	fmt.Printf("track       %s\n", manifest.Track)
	fmt.Printf("timescale   %d units/sec (%s)\n", manifest.Timescale, manifest.TimeSource)
	fmt.Printf("encoding    %s %s\n", manifest.Encoding, manifest.MIME)
	fmt.Printf("epoch       %d\n", manifest.Epoch)
	fmt.Printf("sealed      %d manifest(s), open region starts at delta %d\n", len(manifest.Sealed), manifest.OpenFrom)

	if head, err := reader.Head(ctx); err == nil {
		fmt.Printf("head        delta %d, latest group %s\n", head.Delta, head.Latest)
	} else {
		// A missing head is survivable by design, so say so rather than failing.
		fmt.Printf("head        unavailable (%v)\n", err)
	}

	if !*groups {
		return nil
	}

	fmt.Println()
	out := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(out, "GROUP\tMEDIA\tWALLCLOCK\tOBJECTS\tSIZE")

	for group, err := range reader.Groups(ctx) {
		if err != nil {
			out.Flush()
			return err
		}

		media := strconv.FormatInt(group.T0, 10)
		if group.HasDuration() {
			media += ".." + strconv.FormatInt(group.MediaEnd(), 10)
		}

		// Both anchors are optional, so say so rather than printing the epoch.
		wallclock := "-"
		if group.HasWallclock() {
			wallclock = time.Unix(0, group.W0).UTC().Format(time.RFC3339Nano)
		}

		fmt.Fprintf(out, "%s\t%s\t%s\t%d\t%d\n",
			group.GroupRef, media, wallclock, group.ObjectCount, group.Size)
	}

	return out.Flush()
}

func follow(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("follow", flag.ExitOnError)
	root := flags.String("root", ".", "storage root directory")
	track := flags.String("track", "", "track path, for example live/cam1/video")
	from := flags.Uint64("from", 0, "first delta to read")
	interval := flags.Duration("interval", ledger.DefaultPollInterval, "poll interval")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *track == "" {
		return fmt.Errorf("-track is required")
	}

	reader, err := openReader(ctx, *root, *track)
	if err != nil {
		return err
	}

	for delta, err := range reader.Follow(ctx, *from, *interval) {
		if err != nil {
			return err
		}
		for _, group := range delta.Groups {
			fmt.Printf("delta %d  group %s  media %d+%d  %d objects  %d bytes\n",
				delta.Seq, group.GroupRef, group.T0, group.Duration, group.ObjectCount, group.Size)
		}
	}

	return nil
}

func openReader(ctx context.Context, root, track string) (*ledger.Reader, error) {
	store, err := fsstore.New(root)
	if err != nil {
		return nil, err
	}

	return ledger.OpenReader(ctx, store, ledger.TrackPath(track))
}
