package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/grafana/loki/cmd/logmetrics-generator/metrics"
	"github.com/grafana/loki/pkg/logql/syntax"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/model/labels"
)

type FileState struct {
	Path   string
	Offset int64
}

type Configuration struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	LogqlExpr string `json:"logqlExpr"`
}

var config Configuration

func main() {

	// Get the configuration
	config = getConfig()

	fmt.Println(config.LogqlExpr)
	metrics.GetMonitor().AddMetric(&metrics.Metric{
		Type:        metrics.Counter,
		Name:        config.Name,
		Description: "Number of debug logs",
		Labels:      []string{"provider"},
	})

	go func() {

		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":8080", nil))

	}()

	folder := "./cmd/logmetrics-generator/tmp-logs"
	existingFiles, err := getExistingFiles(folder)
	if err != nil {
		log.Fatal("Error getting existing files:", err)
	}

	fileStates := make(map[string]*FileState)

	for _, file := range existingFiles {
		fileStates[file] = &FileState{
			Path:   file,
			Offset: 0,
		}

		go tailFile(fileStates[file])

	}

	// to keep the goroutine running
	select {}
}

func getConfig() Configuration {

	// Open the JSON file
	file, err := os.Open("./cmd/logmetrics-generator/config.json")
	if err != nil {
		log.Fatal("Error opening file:", err)
	}
	defer file.Close()

	// Read the file contents
	fileData, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("Error reading file:", err)
	}

	// Parse the JSON
	var config Configuration
	err = json.Unmarshal(fileData, &config)
	if err != nil {
		log.Fatal("Error parsing JSON:", err)
	}

	return config
}

func getExistingFiles(folder string) ([]string, error) {
	files := make([]string, 0)

	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}

func tailFile(fileState *FileState) {
	fmt.Println("Tailing file:", fileState.Path)

	f, err := os.Open(fileState.Path)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if err := scanner.Err(); err != nil {
		log.Println("Error scanning file:", err)
		return
	}

	for {
		stat, err := f.Stat()
		if err != nil {
			log.Println("Error getting file stat:", err)
			return
		}

		if stat.Size() < fileState.Offset {
			// File has been truncated or rotated, reset the offset to the beginning
			fileState.Offset = 0
		}

		_, err = f.Seek(fileState.Offset, io.SeekStart)
		if err != nil {
			log.Println("Error seeking file:", err)
			return
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Println("New line:", line)

			exprQuery := config.LogqlExpr
			expr, err := syntax.ParseLogSelector(exprQuery, true)

			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			p, err := expr.Pipeline()
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			sp := p.ForStream(labels.EmptyLabels())
			_, _, matches := sp.ProcessString(0, line)
			if !matches {
				fmt.Println("No matches")
				continue
			}

			metrics.GetMonitor().GetMetric(config.Name).Inc([]string{"logmetrics-generator"})

			fmt.Println("Matches:", matches)
		}

		if err := scanner.Err(); err != nil {
			log.Println("Error scanning file:", err)
			return
		}

		fileState.Offset = stat.Size()

		time.Sleep(time.Second) // Adjust the interval as per your needs
	}
}
