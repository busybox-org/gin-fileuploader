package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/pires/go-proxyproto"
	"github.com/xmapst/logx"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	"github.com/busybox-org/gin-fileuploader/common"
	tusx "github.com/busybox-org/gin-fileuploader/handler"
	memorylocker "github.com/busybox-org/gin-fileuploader/locker/memory"
	filestore "github.com/busybox-org/gin-fileuploader/storage/file"
)

//go:embed index.html
var indexHtml []byte

var (
	host      string
	port      int
	uploadDir string
)

func main() {
	flag.StringVar(&host, "host", "0.0.0.0", "listen host addr")
	flag.IntVar(&port, "port", 8080, "listen port")
	flag.StringVar(&uploadDir, "upload-dir", "./uploads", "upload dir")
	flag.Parse()

	serverCtx, cancelServerCtx := context.WithCancelCause(context.Background())
	_ = os.MkdirAll(uploadDir, os.FileMode(0754))
	logx.Infoln("starting...")
	locker := memorylocker.New()
	_ = os.MkdirAll(filepath.Join(uploadDir, ".data"), os.FileMode(0755))
	dialector := sqlite.Open(filepath.Join(uploadDir, ".data", "db.sqlite"))
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable:       true,
			NoLowerCase:         false,
			IdentifierMaxLength: 256,
		},
		Logger: logger.New(logx.GetSubLogger(), logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			Colorful:                  false,
			IgnoreRecordNotFoundError: true,
			LogLevel:                  logger.Error,
		}),
		SkipDefaultTransaction: true,
		FullSaveAssociations:   true,
		TranslateError:         true,
	}
	gdb, err := gorm.Open(dialector, config)
	if err != nil {
		logx.Fatalln(err)
	}
	defer func() {
		db, err := gdb.DB()
		if err == nil {
			_ = db.Close()
		}
	}()

	store, err := filestore.New(uploadDir, gdb, locker)
	if err != nil {
		logx.Fatalln("failed to create file store", err)
	}
	store.Cleanup(serverCtx, 1*time.Hour)
	tusxHandler, err := tusx.New(&tusx.SConfig{
		BasePath: "/api/v1/files",
		Store:    store,
		Logger:   logx.GetSubLogger(),
	})
	if err != nil {
		logx.Fatalln("failed to create tusx handler", err)
		os.Exit(255)
	}
	tusxHandler.SubscribeCompleteUploads(serverCtx, func(event common.HookEvent) error {
		logx.Infow("upload completed",
			"id", event.Upload.ID,
			"size", event.Upload.Size,
			"offset", event.Upload.Offset,
			"meta", event.Upload.MetaData,
		)
		return nil
	})

	gin.SetMode(gin.ReleaseMode)
	gin.DisableConsoleColor()
	handler := gin.New()
	handler.Use(apiRecovery, apiLogger, cors.Default())
	handler.Any("/api/v1/files", gin.WrapH(tusxHandler))
	handler.Any("/api/v1/files/*any", gin.WrapH(tusxHandler))
	handler.Any("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/html")
		_, _ = c.Writer.Write(indexHtml)
	})

	ln, err := net.Listen("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	if err != nil {
		logx.Fatalln("failed to listen", err)
	}
	logx.Infoln("listen on", ln.Addr().String())

	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 60 * time.Second,
		IdleTimeout:       60 * time.Second,
		ReadTimeout:       0,
		WriteTimeout:      0,
		MaxHeaderBytes:    15 << 20, // 15MB
		BaseContext: func(_ net.Listener) context.Context {
			return serverCtx
		},
	}
	shutdownComplete := setupSignalHandler(server, cancelServerCtx)
	err = server.Serve(&proxyproto.Listener{Listener: ln})
	if errors.Is(err, http.ErrServerClosed) {
		<-shutdownComplete
	} else if err != nil {
		logx.Fatalln("failed to serve", err)
	}
}

func setupSignalHandler(server *http.Server, cancelServerCtx context.CancelCauseFunc) <-chan struct{} {
	shutdownComplete := make(chan struct{})

	// We read up to two signals, so use a capacity of 2 here to not miss any signal
	c := make(chan os.Signal, 2)

	// os.Interrupt is mapped to SIGINT on Unix and to the termination instructions on Windows.
	// On Unix we also listen to SIGTERM.
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// When closing the server, cancel its context so all open requests shut down as well.
	// See context.go for the logic.
	server.RegisterOnShutdown(func() {
		cancelServerCtx(http.ErrServerClosed)
	})

	go func() {
		// First interrupt signal
		<-c
		logx.Infoln("Received interrupt signal. Shutting down tusd...")

		// Wait for second interrupt signal, while also shutting down the existing server
		go func() {
			<-c
			logx.Infoln("Received second interrupt signal. Exiting immediately!")
			os.Exit(1)
		}()

		// Shutdown the server, but with a user-specified timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := server.Shutdown(ctx)

		if err == nil {
			logx.Infoln("Shutdown completed. Goodbye!")
		} else if errors.Is(err, context.DeadlineExceeded) {
			logx.Infoln("Shutdown timeout exceeded. Exiting immediately!")
		} else {
			logx.Errorln("Failed to shutdown gracefully: ", "err", err)
		}

		close(shutdownComplete)
	}()

	return shutdownComplete
}

func apiLogger(c *gin.Context) {
	start := time.Now()
	c.Next()
	latency := time.Since(start)

	status := c.Writer.Status()
	clientIP := c.ClientIP()
	method := c.Request.Method
	proto := c.Request.Proto
	path := c.Request.URL.String()
	userAgent := c.Request.UserAgent()

	if len(c.Errors) > 0 {
		for _, err := range c.Errors.Errors() {
			logx.Errorln(clientIP, method, proto, status, path, latency, err)
		}
		c.AbortWithStatus(http.StatusInternalServerError)
	} else {
		logx.Infoln(clientIP, method, proto, status, path, latency, userAgent)
	}
}

func apiRecovery(c *gin.Context) {
	defer func() {
		if err := recover(); err != nil {
			handlePanic(c, err)
		}
	}()
	c.Next()
}

func handlePanic(c *gin.Context, err interface{}) {
	if isBrokenPipeError(err) {
		httpRequest, _ := httputil.DumpRequest(c.Request, false)
		logx.Errorln("Broken pipe:", c.Request.URL.Path, string(httpRequest), err)
		c.Abort() // Avoid returning InternalServerError for broken pipes
		return
	}

	// Log panic details and return 500
	httpRequest, _ := httputil.DumpRequest(c.Request, false)
	logx.Errorln("[Recovery from panic]",
		time.Now().Format(time.RFC3339),
		string(httpRequest),
		string(debug.Stack()),
		err,
	)
	c.AbortWithStatus(http.StatusInternalServerError)
}

func isBrokenPipeError(err interface{}) bool {
	ne, ok := err.(*net.OpError)
	if !ok {
		return false
	}
	se, ok := ne.Err.(*os.SyscallError)
	if !ok {
		return false
	}

	errMsg := strings.ToLower(se.Error())
	return strings.Contains(errMsg, "broken pipe") || strings.Contains(errMsg, "connection reset by peer")
}
