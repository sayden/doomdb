package doom

import (
	"testing"
	"io/ioutil"
)

func TestReadFileLineByLine(t *testing.T) {
	f, _ := ioutil.TempFile("/tmp", "delete")

	f.WriteString("Hello world\n")

	longLine := make([]byte, 4096*1024)
	longLine = append(longLine, " \n"...)

	f.Write(longLine)
	f.Sync()
	f.Seek(0, 0)

	ls, err := readFileLineByLine(f)
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
