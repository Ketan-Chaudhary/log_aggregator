package collector

import (
	"bufio"
	"os"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func CollectLogs(filepath string, out chan<- models.LogEntry) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()
	defer close(out)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		log := models.LogEntry{
			Message: line,
		}
		out <- log
	}
	return scanner.Err()
}

