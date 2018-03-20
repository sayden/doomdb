package doom

import (
	"engo.io/engo/math"
	"github.com/juju/errors"
	"github.com/thehivecorporation/log"
	"io/ioutil"
	"os"
)

func startup() error {
	// Cleanup empty filesStats first
	cleanEmptyFilesOnFolder(STORAGE_PATH)
	cleanEmptyFilesOnFolder(TEMP_PATH)

	// Retrieve small SSTable files into a big slice
	orderedLines, _, err := contentOfFilesLessThanSizeAndWithFilenames(STORAGE_PATH, SSTABLES_PREFIX, MAX_SSTABLES_SIZE)
	if err != nil {
		err = errors.Annotate(err, "Could not get content of small SSTable files")
		return err
	}

	//  1. Create WAL file
WAL:
	wal, err := NewWAL()
	if err != nil {
		err = errors.Annotate(err, "Could not create WAL file")
		return err
	}

	var accBytes int64
	if len(orderedLines) == 0 {
		goto checkOldWAL
	}

	for i, l := range orderedLines {
		if accBytes >= MAX_SSTABLES_SIZE {
			wal.refFile.Close()
			orderedLines = orderedLines[i:]
			goto WAL
		}

		if n, err := wal.Write([]byte(l)); err != nil {
			err = errors.Annotatef(err, "Could not write to WAL file '%s'", wal.refFile.Name())
			return err
		} else {
			accBytes += int64(n)
		}
	}

	// Check for old WAL filesStats
checkOldWAL:
	orderedLines, n, err := contentOfFilesLessThanSizeAndWithFilenames(TEMP_PATH, WAL_PREFIX, math.MaxInt64)
	if err != nil {
		err = errors.Annotate(err, "Could not get content of small SSTable files")
		return err
	}

	if n >= MAX_SSTABLES_SIZE {
		//Enough data to write another SSTable file
	}

	// WAL filesStats found, join into the opened one and delete them
	// Load indexes into memory
	return nil
}

func contentOfFilesLessThanSizeAndWithFilenames(path, containing string, size int64) ([]string, int64, error) {
	filesStats, err := ioutil.ReadDir(path)
	if err != nil {
		err = errors.Annotatef(err, "Could not read path '%s'", path)
		return nil, 0, err
	}

	//Get content of SSTables which are too small
	var totalContentSize int64
	content := make([]string, 0)
	for _, cf := range filesStats {
		if cf.Size() != 0 && cf.Size() < MAX_SSTABLES_SIZE {
			f, err := os.Open(cf.Name())
			if err != nil {
				log.WithError(err).Error("Could not read table file")
				continue
			}

			ls, n, _ := readFileLineByLine(f)
			if err != nil {
				log.WithError(err).Errorf("Error reading content of file '%s'", cf.Name())
				continue
			}

			totalContentSize += n
			content = append(content, ls...)
		}
	}

	return content, totalContentSize, nil
}
