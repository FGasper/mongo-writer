package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/MatusOllah/slogcolor"
	"github.com/fatih/color"
	"github.com/urfave/cli/v3"
	"golang.org/x/term"
)

const (
	dbName = "test"
)

func main() {
	cmd := &cli.Command{
		Name:  "mongo-writer",
		Usage: "A MongoDB load generator",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "uri",
				Value:   "mongodb://localhost",
				Usage:   "MongoDB connection URI",
				Sources: cli.EnvVars("MONGO_URI"),
			},
			&cli.IntSliceFlag{
				Name:    "docSizes",
				Aliases: []string{"s"},
				Value:   docSizes,
				Usage:   "Document sizes (in bytes) to generate",
			},
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "Enable debug level logging",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "create-data",
				Aliases: []string{"cd"},
				Usage:   "Create initial data via bulk inserts",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:    "docsPerBatch",
						Aliases: []string{"d"},
						Value:   100_000,
						Usage:   "Number of documents to insert per batch",
					},
					&cli.BoolFlag{
						Name:  "serverSideAgg",
						Usage: "Use server-side aggregation (MongoDB 4.2+)",
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					applySharedFlags(cmd)
					newDocsCount := cmd.Int("docsPerBatch")
					useServerSideAgg = cmd.Bool("serverSideAgg")
					return runCreateData(ctx, cmd, newDocsCount)
				},
			},
			{
				Name:    "modify-data",
				Aliases: []string{"md"},
				Usage:   "Continuously modify data via workers",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:    "workers",
						Aliases: []string{"w"},
						Value:   startWorkers,
						Usage:   "Number of initial concurrent workers",
					},
					&cli.IntFlag{
						Name:    "docsPerBatch",
						Aliases: []string{"d"},
						Value:   newDocsCount,
						Usage:   "Number of documents to insert per batch",
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					applySharedFlags(cmd)
					startWorkers = cmd.Int("workers")
					newDocsCount = cmd.Int("docsPerBatch")
					return runModifyData(ctx)
				},
			},
		},
	}

	// EXECUTE
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Printf("err: %v\n\n", err)
		log.Fatal(err)
	}
}

func applySharedFlags(cmd *cli.Command) {
	root := cmd.Root()
	uri = root.String("uri")
	docSizes = root.IntSlice("docSizes")

	if root.Bool("debug") {
		logLevel = slog.LevelDebug
	}
}

// setupLogging configures the global logger with color support.
func setupLogging() {
	logOpts := *slogcolor.DefaultOptions
	logOpts.SrcFileMode = slogcolor.Nop
	logOpts.LevelTags = map[slog.Level]string{
		slog.LevelDebug: color.CyanString("DBG"),
		slog.LevelInfo:  color.GreenString("INF"),
		slog.LevelWarn:  color.YellowString("WRN"),
		slog.LevelError: color.RedString("ERR"),
	}
	logOpts.Level = logLevel

	crlfWriter := CRLFWriter{os.Stdout}
	slog.SetDefault(
		slog.New(
			slogcolor.NewHandler(crlfWriter, &logOpts),
		),
	)
}

// setupTerminalRawMode enables raw terminal mode and returns a cleanup function.
func setupTerminalRawMode() (*term.State, func(), error) {
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return nil, nil, err
	}

	restoreTerm := func() {
		fmt.Printf("------- restoring terminal\n")
		_ = term.Restore(int(os.Stdin.Fd()), oldState)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGALRM)
	go func() {
		<-c
		restoreTerm()
		os.Exit(1)
	}()

	return oldState, restoreTerm, nil
}

// handleKeyboardInput processes terminal input in raw mode.
// If setupComplete is false, arrow keys are ignored.
// onArrow is called for arrow keys (when setupComplete is true).
func handleKeyboardInput(restoreTerm func(), onArrow func(isUp bool) error, oldState *term.State, setupComplete bool) error {
	b := make([]byte, 3)

	for {
		n, err := os.Stdin.Read(b)
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}

		if n == 0 {
			continue
		}

		switch b[0] {
		case '\x03': // CTRL-C
			return nil
		case '\x1a': // CTRL-Z
			restoreTerm()

			// Register for SIGCONT before stopping
			contC := make(chan os.Signal, 1)
			signal.Notify(contC, syscall.SIGCONT)

			// Now stop — terminal is already restored
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(syscall.SIGSTOP)

			// Block until SIGCONT (fg command)
			<-contC
			signal.Stop(contC)

			// Re-enter raw mode
			var err2 error
			oldState, err2 = term.MakeRaw(int(os.Stdin.Fd()))
			if err2 != nil {
				return fmt.Errorf("re-entering raw mode: %w", err2)
			}
		case '\x0d': // Enter
			slog.Info("Status: ready")
		case '\x1b': // Escape sequence
			if n >= 3 && b[1] == '[' {
				switch b[2] {
				case 'A': // up arrow
					if err := onArrow(true); err != nil {
						return err
					}
				case 'B': // down arrow
					if err := onArrow(false); err != nil {
						return err
					}
				}
			}
		}
	}
}
