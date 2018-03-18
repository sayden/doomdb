package doom

import (
	"bufio"
	"fmt"
	"github.com/juju/errors"
	"github.com/thehivecorporation/log"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)

func New(tempFolder, storageFolder string) (s *SortedEntry, err error) {
	s = &SortedEntry{
		Index:         make(map[string]*Entry),
		tempFolder:    tempFolder,
		storageFolder: storageFolder,
	}

	cleanEmptyFilesOnTempFolder(tempFolder)

	if s.StorageFile, s.walFile, err = createDbFiles(storageFolder, tempFolder); err != nil {
		log.WithError(err).Fatal("Error creating storage files")
	}

	s.writer = io.MultiWriter(s.walFile, s)

	if err := readTempFolder(tempFolder, s); err != nil {
		log.WithError(err).Fatal("Could not read WAL file")
	}

	return
}

func cleanEmptyFilesOnTempFolder(f string) {
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

func readTempFolder(f string, s *SortedEntry) (err error) {
	files, err := ioutil.ReadDir(f)
	if err != nil {
		log.WithError(err).Errorf("Could not read folder %s", f)
	}

	for _, cf := range files {
		filePath := fmt.Sprintf("%s/%s", s.tempFolder, cf.Name())
		if !cf.IsDir() && strings.Contains(cf.Name(), "write_ahead_log") && filePath != s.walFile.Name() {
			log.Infof("Opening file '%s'", cf.Name())

			var f *os.File
			if f, err = os.Open(filePath); err != nil {
				log.WithError(err).Fatalf("WAL file named '%s' found but couldn't be opened", cf.Name())
			}

			readFileToWAL(f, s)

			if err := deleteFile(f); err != nil {
				log.WithError(err).Error("Could not delete WAL file")
			}
		}
	}

	return
}

func readFileToWAL(f *os.File, s *SortedEntry) {
	defer f.Close()
	reader := bufio.NewReader(f)

	var line string
	var err error
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		if !(strings.Contains(line, " ") && len(line) > 4) {
			break
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
	if storageFile, err = ioutil.TempFile(storageFolder, "sstable"); err != nil {
		err = errors.Annotatef(err, "Error trying to create a temp file for SSTable")
		return
	}

	if walFile, err = ioutil.TempFile(tempFolder, "write_ahead_log"); err != nil {
		err = errors.Annotatef(err, "Error trying to create a temp file for WAL")
	}

	return
}

type SortedEntry struct {
	tempFolder, storageFolder string
	E                         Entries
	AccBytes                  int64
	Index                     map[string]*Entry
	StorageFile               *os.File
	walFile                   *os.File
	writer                    io.Writer
}

func (s *SortedEntry) Close() error {
	s.walFile.Close()
	s.StorageFile.Close()

	return nil
}

func (s *SortedEntry) Set(key string, value *Entry) {
	s.Index[key] = value
}

func (s *SortedEntry) Get(key string) *Entry {
	return s.Index[key]
}

func (s *SortedEntry) Insert(d string) (err error) {
	if _, err = s.writer.Write([]byte(fmt.Sprintf("%s\n", d))); err != nil {
		err = errors.Annotate(err, "Error writing to pipe writer")
	}

	return
}

func (s *SortedEntry) Write(p []byte) (n int, err error) {
	e := Entry{
		Key:    getKey(string(p)),
		Length: int64(len(p)),
		Data:   p,
	}

	s.Add(e)
	s.Set(e.Key, &e)

	sort.Sort(s)

	return int(e.Length), nil
}

func (s SortedEntry) Len() int {
	return len(s.E.Entries)
}

func (s SortedEntry) Less(i, j int) bool {
	return s.E.Entries[i].Key < s.E.Entries[j].Key
}

func (s SortedEntry) Swap(i, j int) {
	a := s.E.Entries[i]
	s.E.Entries[i] = s.E.Entries[j]
	s.E.Entries[j] = a
}

func (s *SortedEntry) Add(e Entry) *Entry {
	s.E.Entries = append(s.E.Entries, &e)
	s.AccBytes += e.Length

	return &e
}

func (s *SortedEntry) Persist() (err error) {
	s.walFile.Close()
	defer s.StorageFile.Close()

	b := make([]byte, s.AccBytes)

	var accBytes int64
	for i := 0; i < len(s.E.Entries); i++ {
		s.persistEntry(b, i, &accBytes)
	}

	if _, err = s.StorageFile.Write(b); err != nil {
		err = errors.Annotatef(err, "Error trying to persist data on sstable file. Deleting sstable file")

		err2 := deleteFile(s.StorageFile)
		if err2 != nil {
			err = errors.Annotatef(err, err2.Error())
		}
	} else {
		err = deleteFile(s.walFile)
		if err != nil {
			err = errors.Annotatef(err, "Could not delete WAL file. Data has been stored properly on a SSTable file.")
		}
	}

	s.StorageFile, s.walFile, err = createDbFiles(s.storageFolder, s.tempFolder)

	return
}

func deleteFile(f *os.File) (err error) {
	if err = os.Remove(f.Name()); err != nil {
		err = errors.Annotatef(err, "Could not remove storage file. Data is still available in the Write Ahead Log file but couldn't be persisted to disk. Maybe a permissions problem?")
	}

	return
}

func (s *SortedEntry) persistEntry(b []byte, i int, accBytes *int64) {
	e := s.E.Entries[i]

	insertIntoSlice(e.Data, b, *accBytes)

	temp := s.Get(e.Key)
	temp.Offset = *accBytes
	temp.Data = nil

	*accBytes += e.Length
}

func insertIntoSlice(o, d []byte, offset int64) {
	for i := 0; i < len(o); i++ {
		d[int64(i)+offset] = o[i]
	}
}

func getKey(s string) string {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Recovered from panic: %v\n", r)
			log.Errorf("Data: %s", s)
		}
	}()
	return s[0:strings.Index(s, " ")]
}
