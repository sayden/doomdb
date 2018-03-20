package doom

import (
	"bufio"
	"fmt"
	"github.com/juju/errors"
	"github.com/thehivecorporation/log"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

// cleanEmptyFilesOnFolder searches for files with size 0 on f and removes them
func cleanEmptyFilesOnFolder(f string) {
	files, err := ioutil.ReadDir(f)
	if err != nil {
		log.WithError(err).Errorf("Could not read folder %s", f)
	}

	for _, cf := range files {
		if cf.Size() == 0 {
			log.Warnf("Removing file '%s'", cf.Name())
			os.Remove(fmt.Sprintf("%s/%s", f, cf.Name()))
		}
	}
}

func readWALFilesFromFolder(f string, s *MemTable) (err error) {
	files, err := ioutil.ReadDir(f)
	if err != nil {
		err = errors.Annotatef(err, "Could not read folder %s", f)
		return
	}

	for _, cf := range files {
		filePath := fmt.Sprintf("%s/%s", s.tempFolder, cf.Name())
		isWALFile := strings.Contains(cf.Name(), "write_ahead_log")
		isNotCurrentWALFile := filePath != s.walFile.Name()

		//Only process 'write_ahead_log' files that aren't the current opened one
		if !cf.IsDir() && isWALFile && isNotCurrentWALFile {
			log.Infof("Indexing WAL file '%s' into MemTable", cf.Name())

			readWALFileToMemTable(filePath, s)
		}
	}

	return
}

func readWALFileToMemTable(filePath string, s *MemTable) {
	f, err := os.Open(filePath)
	if err != nil {
		log.WithError(err).Fatalf("WAL file named '%s' found but couldn't be opened. You must check the "+
			"contents of this file or remove it and try again if its information isn't critical", filePath)
	}

	// Close and delete old WAL file after reading it successfully
	defer func() {
		if err := f.Close(); err != nil {
			log.WithError(err).Error("Error closing WAL file")
		}

		if err := deleteFile(f); err != nil {
			log.WithError(err).Error("Could not delete WAL file")
		}
	}()

	reader := bufio.NewReader(f)

	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		// Proper WAL lines has syntax 'key value\n' so the minimum possible characters to try insertion are four 'k v\n'
		// Omit 'corrupted' line if it doesn't pass this check
		if !(strings.Contains(line, " ") && len(line) > 4) {
			continue
		}

		if err2 := s.Insert(line[:len(line)-1]); err2 != nil {
			log.WithError(err2).WithField("line", line).Fatal("Could not insert data from old WAL file")
		}
	}

	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}
}

func createDbFiles(storageFolder, tempFolder string) (storageFile *os.File, walFile *os.File, err error) {
	if storageFile, err = createSStableFileOn(storageFolder); err != nil {
		return
	}

	if walFile, err = createWALFileOn(tempFolder); err != nil {
		err = errors.Annotatef(err, "Error trying to create a temp file for WAL")
	}

	return
}

func extractSuffix(prefix, s string) string {
	pos := strings.LastIndex(s, prefix)
	return s[pos+len(prefix):]
}

func fileNameBasedOnTempFile(baseFilename, path, orPrefix, newPrefix string) string {
	suffix := extractSuffix(orPrefix, baseFilename)
	return fmt.Sprintf("%s/%s%s", path, newPrefix, suffix)
}

func createIndexFile(storageFilename, storagePath string) (indexFile *os.File, err error) {
	suffix := extractSuffix(WAL_PREFIX, storageFilename)
	newFile := fmt.Sprintf("%s/%s%s", storagePath, INDEX_PREFIX, suffix)

	if indexFile, err = os.Create(newFile); err != nil {
		err = errors.Annotatef(err, "Could not create index file '%s'", newFile)
	}

	return
}

func createWALFileOn(tempFolder string) (walFile *os.File, err error) {
	if walFile, err = ioutil.TempFile(tempFolder, WAL_PREFIX); err != nil {
		err = errors.Annotatef(err, "Error trying to create a temp file for WAL")
	}

	return
}

func createSSTableFileWithSuffix(storageFolder, filename string) (*os.File, error) {
	return os.Create(fmt.Sprintf("%s/%s%s", storageFolder, SSTABLES_PREFIX, extractSuffix(WAL_PREFIX, filename)))
}

func createSStableFileOn(storageFolder string) (newSStable *os.File, err error) {
	if newSStable, err = ioutil.TempFile(storageFolder, SSTABLES_PREFIX); err != nil {
		err = errors.Annotatef(err, "Could not create SSTable file")
	}

	return
}
