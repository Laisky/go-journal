package journal

import (
	"time"

	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/pkg/errors"
)

const (
	defaultBufDir = "/var/go-fluentd"
	// FlushInterval interval to flush serializer
	deafultFlushInterval = 5 * time.Second
	// defaultRotateCheckInterval interval to rotate journal files
	defaultRotateCheckInterval = 1 * time.Second
	defaultRotateDuration      = 1 * time.Minute
	defaultBufSizeBytes        = 1024 * 1024 * 200
	defaultCommittedIDTTL      = 5 * time.Minute
	defaultName                = "journal"
)

// option configuration of Journal
type option struct {
	bufDirPath   string
	bufSizeBytes int64
	// isAggresiveGC force gc when reset legacy loader
	isAggresiveGC,
	// isCompress [beta] enable gc when writing journal
	isCompress bool
	// interval to flush serializer
	flushInterval,
	rotateDuration time.Duration
	rotateCheckInterval time.Duration
	// committedIDTTL remain ids in memory until ttl, to reduce duplicate msg
	committedIDTTL time.Duration
	name           string
}

func newOption() *option {
	return &option{
		bufDirPath:          defaultBufDir,
		rotateDuration:      defaultRotateDuration,
		bufSizeBytes:        defaultBufSizeBytes,
		isAggresiveGC:       true,
		isCompress:          false,
		flushInterval:       deafultFlushInterval,
		committedIDTTL:      defaultCommittedIDTTL,
		name:                defaultName,
		rotateCheckInterval: defaultRotateCheckInterval,
	}
}

type OptionFunc func(*option) error

func WithRotateDuration(d time.Duration) OptionFunc {
	return func(o *option) error {
		if d < o.rotateDuration {
			utils.Logger.Warn("rotateDuration may too short")
		}

		o.rotateDuration = d
		return nil
	}
}

func WithRotateCheckInterval(d time.Duration) OptionFunc {
	return func(o *option) error {
		if d < o.rotateCheckInterval {
			utils.Logger.Warn("rotateCheckInterval may too short")
		}

		o.rotateCheckInterval = d
		return nil
	}
}

func WithCommitIDTTL(d time.Duration) OptionFunc {
	return func(o *option) error {
		if d < o.committedIDTTL {
			utils.Logger.Warn("committedIDTTL may too short")
		}

		o.committedIDTTL = d
		return nil
	}
}

func WithFlushInterval(d time.Duration) OptionFunc {
	return func(o *option) error {
		if d < o.flushInterval {
			utils.Logger.Warn("flushInterval may too short")
		}

		o.flushInterval = d
		return nil
	}
}

func WithBufDirPath(path string) OptionFunc {
	return func(o *option) (err error) {
		if err = fileutil.IsDirWriteable(path); err != nil {
			return errors.Wrapf(err, "check is writable for `%s`", path)
		}

		o.bufDirPath = path
		return nil
	}
}

func WithName(name string) OptionFunc {
	return func(o *option) (err error) {
		o.name = name
		return nil
	}
}

func WithBufSizeByte(bufSize int64) OptionFunc {
	return func(o *option) (err error) {
		if bufSize < 50*1024*1024 {
			utils.Logger.Warn("buf size bytes too small", zap.Int64("bytes", bufSize))
		}

		o.bufSizeBytes = bufSize
		return nil
	}
}

func WithIsAggresiveGC(is bool) OptionFunc {
	return func(o *option) (err error) {
		o.isAggresiveGC = is
		return nil
	}
}

func WithIsCompress(is bool) OptionFunc {
	return func(o *option) (err error) {
		o.isCompress = is
		return nil
	}
}
