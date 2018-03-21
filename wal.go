package doom

import (
	"bufio"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/thehivecorporation/log"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)

func NewWAL() (*wal, error) {
	walFile, err := ioutil.TempFile(TEMP_PATH, WAL_PREFIX)
	if err != nil {
		err = errors.Annotate(err, "Error creating file for WAL")
	}

	return &wal{walFile}, err
}

type wal struct {
	refFile *os.File
}

func (w *wal) Write(p []byte) (n int, err error) {
	return w.refFile.Write(append(p, '\n'))
}

//Persist should flush the ordered content of a WAL file to disk
func (w *wal) Persist() (fs []string, err error) {
	fs = make([]string, 0)

	s, _ := w.refFile.Stat()
	log.WithField("size", s.Size()).Debug("Flusing WAL file")

	lines, _, err := readFileLineByLine(w.refFile)
	if err != nil {
		err = errors.Annotate(err, "Error trying to read lines from file")
		return
	}

	if err := w.refFile.Close(); err != nil {
		log.WithError(err).Errorf("Error closing '%s' file", w.refFile.Name())
	}

	sort.Strings(lines)
	log.WithField("lines", len(lines)).Debug("Total lines found")

	var lastLineWritten int

startFlush:

	//Now we need to store the contents of the slice and create an index with its keys plus their offsets
	ssTableFile, err := ioutil.TempFile(STORAGE_PATH, SSTABLES_PREFIX)
	if err != nil {
		err = errors.Annotatef(err, "Could not create sstable file on '%s' to write WAL file to", STORAGE_PATH)
	}
	log.WithField("name", ssTableFile.Name()).Debug("File created")
	defer ssTableFile.Close()
	fs = append(fs, ssTableFile.Name())

	// Create an index object
	indexFilename := fileNameBasedOnTempFile(ssTableFile.Name(), STORAGE_PATH, SSTABLES_PREFIX, INDEX_PREFIX)
	sstableIndex := SSTableIndex{
		Indices: make([]*SSTableSingleIndex, 0),
	}

	//Iterate over each line from WAL to create an index entry and write the contents to the SSTable file
	var accBytes int64
	var n int
	for i := lastLineWritten; i < len(lines); i++ {

		if accBytes >= MAX_SSTABLES_SIZE {
			//We need to create a new SSTable and Index files
			break
		}

		// Write to in-memory index
		writeStringToSSTableIndex(lines[i], &sstableIndex, accBytes, indexFilename)

		// Write to the SSTable file too
		if n, err = writeStringToSSTableDisk(lines[i], ssTableFile); err != nil {
			err = errors.Annotatef(err, "Could not write Index and SSTable files")
			removeFiles(indexFilename, ssTableFile.Name())
			return
		}

		accBytes += int64(n)
		lastLineWritten = i
	}

	lastLineWritten++

	//Close SSTable file
	if err := ssTableFile.Close(); err != nil {
		log.WithError(err).Errorf("Error closing SStable file '%s'", ssTableFile.Name())
	}

	//Write index to disk and close it
	if err = writeSSTableIndexToDisk(&sstableIndex, indexFilename); err != nil {
		err = errors.Annotate(err, "Could not write index to disk")
		removeFiles(append(fs, indexFilename)...)
		return
	}

	//Create a new file if more lines are left
	if lastLineWritten < len(lines) {
		goto startFlush
	}

	//Finally, delete the WAL file. It is already stored as sstable and has an index file associated
	log.WithField("name", w.refFile.Name()).Debug("Removing WAL file")
	if err := os.Remove(w.refFile.Name()); err != nil {
		err = errors.Annotate(err, "Could not delete WAL file")
	}

	return
}

func removeFiles(fs ...string) {
	for _, f := range fs {
		if err := os.Remove(f); err != nil {
			log.WithError(err).Errorf("Error deleting file '%s'", f)
		}
	}
}

func writeSSTableIndexToDisk(index *SSTableIndex, indexFilename string) (err error) {
	var indexFile *os.File
	indexFile, err = os.Create(indexFilename)
	if err != nil {
		err = errors.Annotatef(err, "Could not create index file '%s'", indexFilename)
		return
	}
	log.WithField("name", indexFile.Name()).Debug("File created")
	defer indexFile.Close()

	var byt []byte
	if byt, err = proto.Marshal(index); err != nil {
		err = errors.Annotatef(err, "Cannot marshal indices into bytes")
	} else {
		if _, err = indexFile.Write(byt); err != nil {
			err = errors.Annotatef(err, "Could not write to index file '%s'", indexFile.Name())
		}
	}

	return
}

func writeStringToSSTableDisk(line string, sstableFile *os.File) (n int, err error) {
	if n, err = sstableFile.WriteString(line); err != nil {
		err = errors.Annotatef(err, "Error writing line '%s' to sstable file. Aborting. Removing index and sstable file, leaving WAL")

		defer os.Remove(sstableFile.Name())
	}

	return
}

func writeStringToSSTableIndex(line string, index *SSTableIndex, accBytes int64, indexFileName string) {
	index.Indices = append(index.Indices, &SSTableSingleIndex{
		Key:      getKey(line),
		Offset:   accBytes,
		FileName: indexFileName,
	})
}

//readFileLineByLine returns an slice with the contents of the file divided by line
func readFileLineByLine(f io.ReadSeeker) (ls []string, size int64, err error) {
	//Return to beginning of file to start reading
	if _, err = f.Seek(0, 0); err != nil {
		err = errors.Annotate(err, "Error seeking beginning of file")
		return
	}

	reader := bufio.NewReader(f)
	ls = make([]string, 0)

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

		size += int64(len(line))
		ls = append(ls, line)
	}

	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	} else {
		err = nil
	}

	return
}
