package storage

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/busybox-org/gin-fileuploader/common"
)

type IStorage interface {
	NewUpload(ctx context.Context, info common.FileInfo) (upload IUpload, err error)
	GetUpload(ctx context.Context, id string) (upload IUpload, err error)
	Cleanup(ctx context.Context, expiredBefore time.Duration)
}

type IUpload interface {
	GetInfo(ctx context.Context) (common.FileInfo, error)
	GetReader(ctx context.Context) (io.ReadCloser, error)
	WriteChunk(ctx context.Context, offset int64, src io.Reader) (int64, error)
	ConcatUploads(ctx context.Context, partialUploads []IUpload) error
	ServeContent(ctx context.Context, w http.ResponseWriter, r *http.Request) error
	Terminate(ctx context.Context) error
}
