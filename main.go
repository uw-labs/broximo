package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"github.com/uw-labs/broximo/backend"
	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {
	app := cli.NewApp()
	app.Name = "broximo"
	app.Description = "Badger backed proximo server."

	app.Flags = []cli.Flag{
		&cli.IntFlag{
			Name:    "port",
			EnvVars: []string{"BROXIMO_PORT"},
			Value:   6868,
		},
		&cli.StringFlag{
			Name:     "badger-dir",
			EnvVars:  []string{"BROXIMO_BADGER_DIR"},
			Required: true,
		},
		&cli.Int64Flag{
			Name:    "badger-max-cache-size-mb",
			EnvVars: []string{"BROXIMO_BADGER_MAX_CACHE_SIZE_MB"},
		},
	}
	app.Action = func(c *cli.Context) error {
		b, err := backend.New(backend.Config{
			BadgerDBPath:       c.String("badger-dir"),
			BadgerMaxCacheSize: c.Int64("badger-max-cache-size-mb"),
		})
		if err != nil {
			return err
		}
		defer b.Close()

		return listenAndServe(b, c.Int("port"))
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
	log.Println("Server terminated cleanly")
}

func listenAndServe(backend proximo.AsyncSinkSourceFactory, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}
	defer lis.Close()

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
	}
	grpcServer := grpc.NewServer(opts...)
	defer grpcServer.Stop()

	proto.RegisterMessageSourceServer(grpcServer, &proximo.SourceServer{SourceFactory: backend})
	proto.RegisterMessageSinkServer(grpcServer, &proximo.SinkServer{SinkFactory: backend})

	errCh := make(chan error, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() { errCh <- grpcServer.Serve(lis) }()
	select {
	case err := <-errCh:
		return errors.Wrap(err, "failed to serve grpc")
	case <-sigCh:
		return nil
	}
}
