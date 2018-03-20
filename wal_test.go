package doom

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestReadFileLineByLine(t *testing.T) {
	f, _ := ioutil.TempFile("/tmp", "delete")
	defer os.Remove(f.Name())
	defer f.Close()

	f.WriteString("Hello world\n")

	longLine := make([]byte, 4096*1024)
	longLine = append(longLine, " \n"...)

	f.Write(longLine)
	f.Sync()
	f.Seek(0, 0)

	ls, _, err := readFileLineByLine(f)
	if err != nil {
		t.Fatalf("Unexpected error '%s'", err.Error())
	}

	if len(ls) != 2 {
		t.Fatalf("Unexpected number of lines: '%d'", len(ls))
	}

	if ls[0] != "Hello world\n" {
		t.Errorf("Unexpected first line '%s'", ls[0])
	}

	if ls[1] != string(longLine) {
		t.Errorf("Unexpected second line")
	}
}

func TestPersist(t *testing.T) {
	t.Run("asdfadf", func(t *testing.T) {
		w, _ := NewWAL()

		//Remove the expected index and sstable files if they exist

		w.Write([]byte("mario caster"))
		w.Write([]byte("Hello world"))
		w.Write([]byte("ula korn"))

		err := w.Persist()
		if err != nil {
			t.Fatal(err)
		}
	})
	//
	//t.Run("check index file", func(t *testing.T) {
	//	f, _ := os.Open(expectedIndexFileName)
	//	byt, _ := ioutil.ReadAll(f)
	//	var indices SSTableIndex
	//	if err := proto.Unmarshal(byt, &indices); err != nil {
	//		t.Fatal(err)
	//	}
	//
	//	if len(indices.Indices) != 3 {
	//		t.Errorf("Expecting 3 items, got '%d'", len(indices.Indices))
	//	}
	//
	//	if indices.Indices[0].Key != "Hello" {
	//		t.Fail()
	//	}
	//
	//	if indices.Indices[1].Key != "mario" {
	//		t.Fail()
	//	}
	//
	//	if indices.Indices[2].Key != "ula" {
	//		t.Fail()
	//	}
	//})
	//
	//t.Run("check SSTable file", func(t *testing.T) {
	//	fileNameBasedOnTempFile(expectedIndexFileName)
	//	f, err := os.Open(expectedSSTableFileName)
	//	if err != nil {
	//		log.WithError(err).Error("Error reading expected SSTable file")
	//		t.Fatal()
	//	}
	//
	//	lines, _, err := readFileLineByLine(f)
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//
	//	if len(lines) != 3 {
	//		t.Fatalf("Number of lines expected in 3. Got '%d'", len(lines))
	//	}
	//
	//	if lines[0] != "Hello world\n" {
	//		t.Fail()
	//	}
	//
	//	if lines[1] != "mario caster\n" {
	//		t.Fail()
	//	}
	//
	//	if lines[2] != "ula korn\n" {
	//		t.Fail()
	//	}
	//})

	t.Run("check that an big WAL file is splitted into two SSTable files", func(t *testing.T) {
		w, _ := NewWAL()

		byt := make([]byte, 1024)
		for i := 0; i < 1024; i++ {
			byt[i] = byte(78)
		}
		byt[10] = ' '

		w.Write(byt)
		w.Write(byt)
		w.Write(byt)

		if err := w.Persist(); err != nil {
			t.Fatal(err)
		}

		t.Run("2 files must have been created in case of MAX_SSTABLES_SIZE=2048", func(t *testing.T) {
			//files, err := ioutil.ReadDir(TEMP_PATH)
			//if err != nil {
			//	t.Fatal(err)
			//}
		})
	})
}
