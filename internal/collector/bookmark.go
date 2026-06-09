package collector

import (
	"encoding/json"
	"os"
	"sync"
)

type BookmarkManager struct {
	mu       sync.Mutex
	filePath string
	offsets  map[string]int64
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

func (bm *BookmarkManager) GetOffset(path string) int64 {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.offsets[path]
}

func (bm *BookmarkManager) SaveOffset(path string, offset int64) error {
	bm.mu.Lock()
	bm.offsets[path] = offset
	bm.mu.Unlock()
	
	// Flush to disk
	return bm.Flush()
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
	return os.Rename(tempFile, bm.filePath)
}
