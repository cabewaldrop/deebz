package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeebz(t *testing.T) {
	db, _ := New("data.csv")
	defer func() {
		db.Close()
		os.Remove("data.csv")
	}()

	db.Put(&Item{
		Key:   "Old",
		Value: "Yeller",
	})

	db.Put(&Item{
		Key:   "Old",
		Value: "Yellerz",
	})

	val, err := db.Get("Old")
	if err != nil {
		t.Errorf("Error retrieving key: %v", err)
	}

	assert.Equal(t, "Yellerz", val)
}
