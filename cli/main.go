package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sayden/doomdb"
	"github.com/thehivecorporation/log"
	"os"
	"strings"
	"github.com/juju/errors"
)

var tempFolder = "/tmp"
var storageFolder = "/tmp"

var memtable *doom.MemTable

type kv struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func main() {
	var err error
	if memtable, err = doom.New(tempFolder, storageFolder); err != nil {
		log.WithError(err).Fatal("Error creating DaDB")
	}

	r := gin.Default()

	r.PUT("/", func(c *gin.Context) {
		var e kv
		if err := c.BindJSON(&e); err != nil {
			log.WithError(err).Error("Could not bind entry")
			c.JSON(500, gin.H{"status": "error", "msg": err.Error()})
			return
		}

		if err = insert(e, memtable); err != nil {
			c.JSON(500, gin.H{"status": "error", "msg": err.Error()})
		}

	})

	storageFile := memtable.StorageFile.Name()

	r.POST("/", func(c *gin.Context) {
		if err := memtable.Persist(); err != nil {
			c.JSON(500, "Error persisting data on disk")
		}
	})

	r.GET("/", func(c *gin.Context) {
		findUsingIndex(storageFile)
		c.Status(200)
	})

	r.Run(":8080")
}

func insert(e kv, memtable *doom.MemTable) (err error) {
	if e.Key == "" || len(e.Value) == 0 {
		err := errors.New("Key or value not found")
		return
	}

	if err := memtable.Insert(fmt.Sprintf("%s %s", e.Key, e.Value)); err != nil {
		err = errors.Annotate(err, "Error inserting data")
	}

	return
}

func findUsingIndex(fn string) {
	log.Debugf("Opening file name %s", fn)

	f, err := os.Open(fn)
	if err != nil {
		log.WithError(err).Fatal("Error opening storage file")
	}
	defer f.Close()

	mario := memtable.Get("a_key")
	if mario == nil {
		log.Error("Key not found")
		return
	}

	b := make([]byte, mario.Length)
	f.ReadAt(b, mario.Offset)

	s := string(b)

	log.Infof("DATA->%s", s[strings.Index(s, " ")+1:])
}
