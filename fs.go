package journal

// fs.go
// create directory and journal id & data files.

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/pkg/errors"
)

var (
	// dataFileNameReg journal data file name pattern
	dataFileNameReg = regexp.MustCompile(`^\d{8}_\d{8}\.buf(.gz)?$`)
	// idsFileNameReg journal id file name pattern
	idsFileNameReg  = regexp.MustCompile(`^\d{8}_\d{8}\.ids(.gz)?$`)
	fileGzSuffixReg = regexp.MustCompile(`\.gz$`)

	defaultFileNameTimeLayout = "20060102"
	// defaultFileNameTimeLayoutWithTZ = "20060102-0700"
)

func isFileGZ(fname string) bool {
	return fileGzSuffixReg.MatchString(fname)
}

// PrepareDir `mkdir -p`
func PrepareDir(path string) error {
	ou := syscall.Umask(0)
	defer syscall.Umask(ou)

	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(path, DirMode); err != nil {
			return errors.Wrapf(err, "create directory `%s` with mod `%d`", path, DirMode)
		}

		Logger.Info("create new directory", zap.String("path", path))
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "get stat of path `%s`", path)
	}

	if !info.IsDir() {
		return fmt.Errorf("is not directory `%s`", path)
	}

	return nil
}

// bufFileStat current journal files' stats
type bufFileStat struct {
	NewDataFp, NewIDsFp             *os.File
	OldDataFnames, OldIDsDataFnames []string
}

// PrepareNewBufFile create new data & id files, and update bufFileStat.
// * if `isScan=true`, will scan directory to find existing buf files,
//   then generate new buf files.
//
// * if `isScan=false`, keep old buf files, directly generate new file without scan directory.
func PrepareNewBufFile(dirPath string, oldFsStat *bufFileStat, isScan, isGz bool, sizeBytes int64) (fsStat *bufFileStat, err error) {
	logger := Logger.With(
		zap.String("dirpath", dirPath),
		zap.Bool("is_scan", isScan),
		zap.Bool("is_gz", isGz),
	)
	logger.Debug("call PrepareNewBufFile")
	fsStat = &bufFileStat{}

	// scan directories
	var (
		latestDataFName, latestIDsFName string
		fname, absFname                 string
		fs                              []os.FileInfo
	)
	// scan existing buf files.
	// update legacyLoader or first run.
	if isScan || oldFsStat == nil {
		if fs, err = ioutil.ReadDir(dirPath); err != nil {
			return nil, errors.Wrapf(err, "read files in dir `%s`", dirPath)
		}

		for _, f := range fs {
			_, fname = filepath.Split(f.Name())
			absFname = path.Join(dirPath, fname)

			// macos fs bug, could get removed files
			if _, err := os.Stat(absFname); os.IsNotExist(err) {
				logger.Warn("file not exists", zap.String("fname", fname))
				return nil, nil
			}

			if dataFileNameReg.MatchString(fname) {
				logger.Debug("find data file", zap.String("file", fname))
				fsStat.OldDataFnames = append(fsStat.OldDataFnames, absFname)
				if fname > latestDataFName {
					latestDataFName = fname
				}

			} else if idsFileNameReg.MatchString(fname) {
				logger.Debug("find ids file", zap.String("file", fname))
				fsStat.OldIDsDataFnames = append(fsStat.OldIDsDataFnames, absFname)
				if fname > latestIDsFName {
					latestIDsFName = fname
				}

			} else {
				logger.Warn("unknown file in buf directory", zap.String("file", fname))
			}
		}

		logger.Debug("scan journal files",
			zap.String("latest_data_file", latestDataFName),
			zap.String("latest_ids_file", latestIDsFName),
			zap.Strings("data_fs", fsStat.OldDataFnames),
			zap.Strings("ids_fs", fsStat.OldIDsDataFnames))
	} else {
		// do not change old file names
		_, latestDataFName = filepath.Split(oldFsStat.NewDataFp.Name())
		_, latestIDsFName = filepath.Split(oldFsStat.NewIDsFp.Name())
	}

	// generate new buf data file name
	// `latestxxxFName` means new buf file name now
	now := utils.Clock.GetUTCNow()
	if latestDataFName == "" {
		latestDataFName = now.Format(defaultFileNameTimeLayout) + "_00000001.buf"
	} else {
		if latestDataFName, err = GenerateNewBufFName(now, latestDataFName); err != nil {
			return nil, errors.Wrapf(err, "generate new data fname `%s`", latestDataFName)
		}
	}

	// generate new buf ids file name
	if latestIDsFName == "" {
		latestIDsFName = now.Format(defaultFileNameTimeLayout) + "_00000001.ids"
	} else {
		if latestIDsFName, err = GenerateNewBufFName(now, latestIDsFName); err != nil {
			return nil, errors.Wrapf(err, "generate new ids fname `%s`", latestIDsFName)
		}
	}

	if isGz {
		latestDataFName = appendGzSuffix(latestDataFName)
		latestIDsFName = appendGzSuffix(latestIDsFName)
	}

	if fsStat.NewDataFp, err = OpenBufFile(filepath.Join(dirPath, latestDataFName), sizeBytes/2); err != nil {
		return nil, err
	}

	if fsStat.NewIDsFp, err = OpenBufFile(filepath.Join(dirPath, latestIDsFName), 0); err != nil {
		return nil, err
	}

	logger.Debug("create new buf files",
		zap.String("ids_file", latestIDsFName),
		zap.String("data_file", latestDataFName))
	return fsStat, nil
}

func appendGzSuffix(fname string) string {
	if !strings.HasSuffix(strings.ToLower(fname), ".gz") {
		fname += ".gz"
	}

	return fname
}

// OpenBufFile create and open file
func OpenBufFile(filepath string, preallocateBytes int64) (fp *os.File, err error) {
	Logger.Debug("create file with preallocate",
		zap.Int64("preallocate", preallocateBytes),
		zap.String("file", filepath))
	if fp, err = os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, FileMode); err != nil {
		return nil, errors.Wrapf(err, "open file: %+v", filepath)
	}

	if preallocateBytes != 0 {
		if err = fileutil.Preallocate(fp, preallocateBytes, false); err != nil {
			return nil, errors.Wrapf(err, "tpreallocate file bytes `%d`", preallocateBytes)
		}
	}

	return fp, nil
}

// GenerateNewBufFName return new buf file name depends on current time
// file name looks like `yyyymmddnnnn.ids`, nnnn begin from 0001 for each day
func GenerateNewBufFName(now time.Time, oldFName string) (string, error) {
	Logger.Debug("GenerateNewBufFName", zap.Time("now", now), zap.String("oldFName", oldFName))
	finfo := strings.SplitN(oldFName, ".", 2) // {name, ext}
	if len(finfo) < 2 {
		return oldFName, fmt.Errorf("oldFname `%s` not correct", oldFName)
	}

	fts := finfo[0][:8]
	fidx := finfo[0][9:]
	fext := strings.ToLower(finfo[1])
	if now.Format(defaultFileNameTimeLayout) != fts {
		return now.Format(defaultFileNameTimeLayout) + "_00000001." + fext, nil
	}

	idx, err := strconv.ParseInt(fidx, 10, 64)
	if err != nil {
		return oldFName, errors.Wrapf(err, "parse buf file's idx `%s` got error", fidx)
	}

	return fmt.Sprintf("%s_%08d.%s", fts, idx+1, fext), nil
}
