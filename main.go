package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/uw-labs/broximo/backend"
	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var logger *logrus.Logger

func main() {
	app := cli.NewApp()
	app.Name = "broximo"
	app.Description = "Badger backed proximo server."

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "log-level",
			EnvVars: []string{"BROXIMO_LOG_LEVEL"},
			Value:   "info",
		},
		&cli.StringFlag{
			Name:    "log-formatter",
			EnvVars: []string{"BROXIMO_LOG_FORMATTER"},
			Value:   "text",
		},
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
	app.Before = func(c *cli.Context) error {
		return setLogger(c)
	}
	app.Action = func(c *cli.Context) error {
		b, err := backend.New(backend.Config{
			Logger:             logger,
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
		logrus.Fatal(err)
	}
	logger.Println("Server terminated cleanly")
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
	logger.Infof("Server started on port %v", port)

	select {
	case err := <-errCh:
		return errors.Wrap(err, "failed to serve grpc")
	case <-sigCh:
		return nil
	}
}

func setLogger(c *cli.Context) error {
	logger = logrus.New()
	logLevel := strings.ToLower(c.String("log-level"))
	if strings.ToLower(logLevel) == "off" {
		logger.SetOutput(ioutil.Discard)
		return nil
	}
	ll, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return err
	}
	logger.SetLevel(ll)

	var format logrus.Formatter
	switch strings.ToLower(c.String("log-formatter")) {
	case "json":
		format = &logrus.JSONFormatter{}
	case "text":
		format = &logrus.TextFormatter{}
	default:
		return errors.Errorf("Unknown log formatter: %s", c.String("log-formatter"))
	}
	logger.SetFormatter(format)

	return nil
}
