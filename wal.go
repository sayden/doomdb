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

//Persist should flushes the ordered content of a WAL file to disk
func (w *wal) Persist() (err error) {
	lines, err := readFileLineByLine(w.refFile)
	if err != nil {
		err = errors.Annotate(err, "Error trying to read lines from file")
		return
	}

	sort.Strings(lines)

	//Now we need to store the contents of the slice and create an index with its keys plus their offsets
	//Create the index file
	indexFile, err := createIndexFile(w.refFile.Name(), STORAGE_PATH)
	if err != nil {
		err = errors.Annotatef(err, "Could not create index file from WAL file '%s'", w.refFile.Name())
		return
	}
	defer indexFile.Close()

	sstableFile, err := createSSTableFileWithSuffix(STORAGE_PATH, w.refFile.Name())
	if err != nil {
		err = errors.Annotatef(err, "Could not create sstable file on '%s' to write WAL file to", STORAGE_PATH)
	}
	defer sstableFile.Close()

	// Create an index object
	sstableIndex := SSTableIndex{
		Indices: make([]*SSTableSingleIndex, len(lines)),
	}

	//Iterate over each line to create an index entry and write the contents to the sstable file
	var accBytes int64
	for i, line := range lines {
		sstableIndex.Indices[i] = &SSTableSingleIndex{
			Key:      getKey(line),
			Offset:   accBytes,
			FileName: indexFile.Name(),
		}

		accBytes += int64(len(line))

		//Write to the SSTable file too
		if _, err = sstableFile.WriteString(line); err != nil {
			err = errors.Annotatef(err, "Error writing line '%s' to sstable file. Aborting. Removing index and sstable file, leaving WAL")

			defer os.Remove(sstableFile.Name())
			defer os.Remove(indexFile.Name())

			return
		}
	}

	//Write index to disk
	var byt []byte
	if byt, err = proto.Marshal(&sstableIndex); err != nil {
		err = errors.Annotatef(err, "Cannot marshal indices into bytes")
		return
	} else {
		if _, err := indexFile.Write(byt); err != nil {
			err = errors.Annotatef(err, "Could not write to index file '%s'", indexFile.Name())
		}
	}

	//Finally, delete the WAL file. It is already stored as sstable and has an index file associated
	os.Remove(w.refFile.Name())

	return
}

//readFileLineByLine returns an slice with the contents of the file divided by line
func readFileLineByLine(f *os.File) (ls []string, err error) {
	defer func() {
		if err := f.Close(); err != nil {
			log.WithError(err).Errorf("Error closing '%s' file", f.Name())
		}
	}()

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

		ls = append(ls, line)
	}

	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	} else {
		err = nil
	}

	return
}
