// Command qumo-ledger inspects tracks stored by the ledger.
//
// Because a track is readable with nothing but object-store access, this tool
// is a convenience rather than a component: it uses the same public API any
// reader would, and nothing in the storage format depends on it existing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/okdaichi/qumo-ledger/internal/version"
	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store/fsstore"
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
  qumo-ledger follow  -root <dir> -track <path>   tail groups as they land
                      [-tip] [-group <ref>]      start at the tip, or resume
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

	meta := reader.Root()
	fmt.Printf("track       %s\n", meta.Track)
	fmt.Printf("timescale   %d units/sec (%s)\n", meta.Timescale, meta.TimeSource)
	fmt.Printf("encoding    %s %s\n", meta.Encoding, meta.MIME)
	fmt.Printf("epoch       %d\n", meta.Epoch)

	if !*groups {
		return nil
	}

	fmt.Println()
	out := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(out, "GROUP\tMEDIA\tWALLCLOCK\tOBJECTS\tSIZE")

	reader.SeekStart()
	for {
		group, err := reader.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			out.Flush()
			return err
		}

		// Duration is optional; without one a group is just its anchor.
		media := strconv.FormatInt(group.MediaTime, 10)
		if group.Duration > 0 {
			media += ".." + strconv.FormatInt(group.MediaTime+group.Duration, 10)
		}

		// A zero wallclock means no anchor, not the Unix epoch.
		wallclock := "-"
		if group.Wallclock != 0 {
			wallclock = time.Unix(0, group.Wallclock).UTC().Format(time.RFC3339Nano)
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
	group := flags.String("group", "", "resume after a group ref printed by an earlier run, e.g. e000001-g00000003")
	tip := flags.Bool("tip", false, "start after everything already committed")
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

	// With no position flag an unflagged run replays everything and then tails.
	switch {
	case *group != "":
		from, err := ledger.ParseGroupRef(*group)
		if err != nil {
			return err
		}
		if err := reader.SeekGroup(ctx, from); err != nil {
			return err
		}
	case *tip:
		if err := reader.SeekTip(ctx); err != nil {
			return err
		}
	default:
		reader.SeekStart()
	}

	// Following is polling, because object stores do not push. Next is
	// non-blocking — it returns io.EOF at the tip — so the wait lives here.
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		group, err := reader.Next(ctx)
		if errors.Is(err, io.EOF) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				continue
			}
		}
		if err != nil {
			return err
		}
		fmt.Printf("%s  group %s  media %d+%d  %d objects  %d bytes\n",
			reader.Position(), group.GroupRef, group.MediaTime, group.Duration,
			group.ObjectCount, group.Size)
	}
}

func openReader(ctx context.Context, root, path string) (*ledger.Reader, error) {
	objects, err := fsstore.New(root)
	if err != nil {
		return nil, err
	}

	track, err := ledger.Open(ctx, objects, ledger.TrackPath(path), ledger.Config{})
	if err != nil {
		return nil, err
	}
	return track.Reader(ctx)
}
