package doom

import (
	"os"
	"bufio"
	"strings"
	"io"
	"fmt"
	"github.com/thehivecorporation/log"
	"github.com/juju/errors"
	"sort"
)

type wal struct {
	refFile  *os.File
	filePath string
}

func (w *wal) Write(p []byte) (n int, err error) {
	return w.refFile.Write(p)
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
