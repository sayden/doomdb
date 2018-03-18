package doom

import (
	"os"
	"io/ioutil"
	"github.com/thehivecorporation/log"
	"github.com/juju/errors"
)

// mergeSSTablesFilePaths TODO
func mergeSSTablesFilePaths(storageFolder string) (err error) {
	files, err := ioutil.ReadDir(storageFolder)
	if err != nil {
		err = errors.Annotatef(err, "Could not read folder %s", storageFolder)
		return
	}

	// Open non empty files that aren't too big already
	tables := make([]*os.File, 0)
	for _, cf := range files {
		if cf.Size() != 0 && cf.Size() < 2048 {
			f, err := os.Open(cf.Name())
			if err != nil {
				log.WithError(err).Error("Could not read table file")
				continue
			}

			tables = append(tables, f)
		}
	}

	// Stop if no candidates were found
	if len(tables) == 0 {
		return errors.New("No candidates tables found to merge")
	}

	//Now, the contents of each file must be inserted in a new array to sort it and create a new file

	entries := make([]string, 0)
	for _, f := range tables {
		tempEntries, err := readLines(f)
		if err != nil {
			log.WithError(err).Errorf("Error trying to read content of file '%s'", f.Name())
			continue
		}
		entries = append(entries, tempEntries...)
	}

	//newSStable, err := createSStableFileOn(storageFolder)
	//if err != nil {
	//	err = errors.Annotate(err, "Could not create new sstable to merge")
	//}

	return
}

func readLines(f *os.File)(ls []string, err error){
	return nil, nil
}
