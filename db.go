package deebz

import (
	"sync"

	"github.com/cabewaldrop/deebz/pkg/storage"
)

type DB struct {
	mu    sync.RWMutex
	store storage.Store
}

type Options struct {
	inMemory bool
}
