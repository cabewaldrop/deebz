package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

type Item struct {
	Key   string
	Value string
}

type ItemMeta struct {
	Position int64
	Size     int
}

type Store interface {
	Get(key string) (string, error)
	Put(item *Item) error
	Close()
}

type FileDB struct {
	mu       *sync.Mutex
	Idx      map[string]ItemMeta
	DataFile string
	File     *os.File
}

func (db FileDB) Put(item *Item) error {
	if item == nil {
		return errors.New("Cannot insert a null item")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Get current position of the file
	info, err := os.Stat(db.File.Name())
	if err != nil {
		return fmt.Errorf("Error reading datafile position: %w", err)
	}
	pos := info.Size()

	// Write the record
	row := fmt.Sprintf("%s,%v\n", item.Key, item.Value)
	_, err = db.File.Write([]byte(row))
	if err != nil {
		return fmt.Errorf("Error writing record: %s to file", row)
	}

	// Update the index
	db.Idx[item.Key] = ItemMeta{Position: pos + int64(len(item.Key)+1), Size: len(item.Value)}

	return nil
}

func (db FileDB) Get(key string) (string, error) {
	db.mu.Lock()
	meta, ok := db.Idx[key]
	db.mu.Unlock()

	if !ok {
		return "", errors.New("Key not found")
	}

	val := make([]byte, meta.Size)
	db.File.ReadAt(val, meta.Position)

	return string(val), nil
}

func (db FileDB) Close() {
	db.File.Close()
}

func New(dataFile string) (Store, error) {
	f, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("Error opening the data file for read: %w", err)
	}

	return FileDB{
		mu:       &sync.Mutex{},
		DataFile: dataFile,
		File:     f,
		Idx:      make(map[string]ItemMeta),
	}, nil
}
