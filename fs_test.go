package doom

import (
	"testing"
	"os"
	"fmt"
)

func TestCreateIndexFile(t *testing.T){
	f, err := os.Create(fmt.Sprintf("/tmp/%s12345", WAL_PREFIX))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer os.Remove(f.Name())

	index, err := createIndexFile(f.Name(), "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	defer os.Remove(index.Name())

	if index.Name() != "/tmp/index12345" {
		t.Errorf("File name was '%s'", index.Name())
	}
}
