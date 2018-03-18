package doom

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/thehivecorporation/log"
	"io"
	"os"
	"sort"
	"strings"
)

// New creates a new MemTable and its related WAL and SStable files on disk
func New(tempFolder, storageFolder string) (s *MemTable, err error) {
	s = &MemTable{
		Index:         make(map[string]*Entry),
		tempFolder:    tempFolder,
		storageFolder: storageFolder,
	}

	cleanEmptyFilesOnTempFolder(tempFolder)

	if s.StorageFile, s.walFile, err = createDbFiles(storageFolder, tempFolder); err != nil {
		log.WithError(err).Fatal("Error creating storage files")
	}

	s.writer = io.MultiWriter(s.walFile, s)

	if err := readWALFilesFromFolder(tempFolder, s); err != nil {
		log.WithError(err).Fatal("Could not read WAL file")
	}

	return
}

type MemTable struct {
	tempFolder, storageFolder string
	E                         []*Entry
	AccBytes                  int64
	Index                     map[string]*Entry
	StorageFile               *os.File
	walFile                   *os.File
	writer                    io.Writer
}

// Close closes the WAL and the sstable file
func (s *MemTable) Close() (err error) {
	if err = s.walFile.Close(); err != nil {
		log.WithError(err).Error("Error trying to close WAL file")
	}

	if err2 := s.StorageFile.Close(); err2 != nil {
		log.WithError(err2).Error("Error trying to close SSTable file")
		err = err2
	}

	return err
}

// Set stored a new key-value in the MemTable index
func (s *MemTable) Set(key string, value *Entry) {
	s.Index[key] = value
}

// Get returns a value taken from the MemTable
// TODO It should also check and merge the indices stored in the disk
func (s *MemTable) Get(key string) *Entry {
	return s.Index[key]
}

// Insert writes 'd' into the WAL and the MemTable
func (s *MemTable) Insert(d string) (err error) {
	if _, err = s.writer.Write([]byte(fmt.Sprintf("%s\n", d))); err != nil {
		err = errors.Annotate(err, "Error writing to pipe writer")
	}

	return
}

// Write is the io.Writer implementation that inserts the incoming bytes into the WAL
func (s *MemTable) Write(p []byte) (n int, err error) {
	e := Entry{
		Key:    getKey(string(p)),
		Length: int64(len(p)),
		Data:   p,
	}

	s.Add(e)
	s.Set(e.Key, &e)

	if SORT_ON_INSERTION {
		sort.Sort(s)
	}


	return int(e.Length), nil
}

// Len is part of the sort.Interface implementation
func (s MemTable) Len() int {
	return len(s.E)
}

// Less is part of the sort.Interface implementation
func (s MemTable) Less(i, j int) bool {
	return s.E[i].Key < s.E[j].Key
}

// Swap is part of the sort.Interface implementation
func (s MemTable) Swap(i, j int) {
	a := s.E[i]
	s.E[i] = s.E[j]
	s.E[j] = a
}

// Add a new record to the write ahead log
func (s *MemTable) Add(e Entry) *Entry {
	s.E = append(s.E, &e)
	s.AccBytes += e.Length

	return &e
}

// Persist writes the current write ahead log to disk in a new sstable file
func (s *MemTable) Persist() (err error) {
	defer s.Close()

	if !SORT_ON_INSERTION {
		sort.Sort(s)
	}

	var accBytes int64
	for i := 0; i < len(s.E); i++ {
		s.persistEntry(i, &accBytes)
	}

	return
}

func (s *MemTable) persistEntry(i int, accBytes *int64) (err error) {
	e := s.E[i]

	if _, err = s.StorageFile.Write(e.Data); err != nil {
		err = errors.Annotatef(err, "Error trying to persist data on sstable file. Deleting sstable file")

		err2 := deleteFile(s.StorageFile)
		if err2 != nil {
			err = errors.Annotatef(err, err2.Error())
		}

		return
	} else {
		err = deleteFile(s.walFile)
		if err != nil {
			err = errors.Annotatef(err, "Could not delete WAL file. Data has been stored properly on a SSTable file.")
		}
	}

	temp := s.Get(e.Key)
	temp.Offset = *accBytes
	temp.Data = nil

	*accBytes += e.Length

	return
}

func deleteFile(f *os.File) (err error) {
	if err = os.Remove(f.Name()); err != nil {
		err = errors.Annotatef(err, "Could not remove file. Data is still available in either the Write " +
			"Ahead Log or the SStable file. It just couldn't be deleted. Maybe a permissions problem?")
	}

	return
}

//getKey returns the possible key of a string entry
func getKey(s string) string {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Recovered from panic: %v\n", r)
			log.Errorf("Data: %s", s)
		}
	}()
	return s[0:strings.Index(s, " ")]
}
