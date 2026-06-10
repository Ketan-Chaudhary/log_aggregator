package collector

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"
)

type BookmarkManager struct {
	mu       sync.Mutex
	filePath string
	offsets  map[string]int64
	dirty    bool
}

func NewBookmarkManager(filePath string) (*BookmarkManager, error) {
	bm := &BookmarkManager{
		filePath: filePath,
		offsets:  make(map[string]int64),
	}
	
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return bm, nil
		}
		return nil, err
	}
	defer file.Close()

	if err := json.NewDecoder(file).Decode(&bm.offsets); err != nil {
		return nil, err
	}
	return bm, nil
}

// StartPeriodicFlush flushes dirty bookmarks to disk every interval.
// It stops when the context is cancelled and does a final flush before returning.
func (bm *BookmarkManager) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if err := bm.Flush(); err != nil {
				log.Printf("Failed final bookmark flush: %v", err)
			}
			return
		case <-ticker.C:
			bm.mu.Lock()
			dirty := bm.dirty
			bm.mu.Unlock()
			if dirty {
				if err := bm.Flush(); err != nil {
					log.Printf("Failed periodic bookmark flush: %v", err)
				}
			}
		}
	}
}

func (bm *BookmarkManager) GetOffset(path string) int64 {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.offsets[path]
}

func (bm *BookmarkManager) SaveOffset(path string, offset int64) {
	bm.mu.Lock()
	bm.offsets[path] = offset
	bm.dirty = true
	bm.mu.Unlock()
}

func (bm *BookmarkManager) Flush() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	data, err := json.MarshalIndent(bm.offsets, "", "  ")
	if err != nil {
		return err
	}
	
	// Write to temporary file then rename for atomic write
	tempFile := bm.filePath + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}
	bm.dirty = false
	return os.Rename(tempFile, bm.filePath)
}
