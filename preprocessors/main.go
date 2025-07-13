package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// --- Configuration ---
const (
	// Corrected paths to remove duplication
	rawSalesPath    = "./../local_s3_bucket/sales/"
	rawMetadataPath = "./../local_s3_bucket/metadata/"
	rawExternalPath = "./../local_s3_bucket/external/"
)

// dbConnect establishes a connection to the PostgreSQL database.
func dbConnect() *sql.DB {
	connStr := "user=admin password=password dbname=sales_db sslmode=disable host=localhost port=5432"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	return db
}

// checkJobStatus checks if a file has already been processed successfully.
func checkJobStatus(db *sql.DB, fileName string) (bool, error) {
	var status string
	query := "SELECT status FROM etl_job_status WHERE file_name = $1"
	err := db.QueryRow(query, fileName).Scan(&status)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil // Not processed yet, which is fine
		}
		return false, fmt.Errorf("failed to query job status: %w", err)
	}
	return status == "SUCCESS", nil
}

// updateJobStatus updates the status of a file processing job using a standalone connection.
func updateJobStatus(db *sql.DB, fileName, status string) error {
	query := `
        INSERT INTO etl_job_status (file_name, status, last_updated)
        VALUES ($1, $2, NOW())
        ON CONFLICT (file_name) DO UPDATE
        SET status = $2, last_updated = NOW();
    `
	_, err := db.Exec(query, fileName, status)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}
	return nil
}

// updateJobStatusInTx updates the status within an existing transaction for atomicity.
func updateJobStatusInTx(tx *sql.Tx, fileName, status string) error {
	query := `
        INSERT INTO etl_job_status (file_name, status, last_updated)
        VALUES ($1, $2, NOW())
        ON CONFLICT (file_name) DO UPDATE
        SET status = $2, last_updated = NOW();
    `
	_, err := tx.Exec(query, fileName, status)
	return err
}

// processSalesData reads, transforms, and loads the daily sales CSV.
func processSalesData(db *sql.DB) error {
	log.Println("Starting sales data processing...")
	todayStr := time.Now().Format("2006-01-02")
	fileName := fmt.Sprintf("sales_%s.csv", todayStr)
	filePath := filepath.Join(rawSalesPath, fileName)

	isProcessed, err := checkJobStatus(db, fileName)
	if err != nil {
		return err
	}
	if isProcessed {
		log.Printf("File '%s' already processed successfully. Skipping.", fileName)
		return nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("could not open sales file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Read() // Skip header

	type HourlyAggregate struct {
		TotalSales    float64
		TotalQuantity int
	}
	aggregates := make(map[time.Time]map[string]HourlyAggregate)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading sales csv: %w", err)
		}

		if len(record) < 6 {
			log.Printf("Skipping row with insufficient columns: %v", record)
			continue
		}
		quantity, _ := strconv.Atoi(record[3])
		price, _ := strconv.ParseFloat(record[4], 64)
		if quantity <= 0 || price <= 0 {
			continue
		}

		const layout = "2006-01-02T15:04:05.999999999"
		timestamp, err := time.Parse(layout, record[5])
		if err != nil {
			log.Printf("Skipping row due to invalid timestamp format: %v, value: '%s'", err, record[5])
			continue
		}

		hour := timestamp.Truncate(time.Hour)
		categoryID := record[2]
		totalSalesValue := float64(quantity) * price

		if _, ok := aggregates[hour]; !ok {
			aggregates[hour] = make(map[string]HourlyAggregate)
		}
		agg := aggregates[hour][categoryID]
		agg.TotalSales += totalSalesValue
		agg.TotalQuantity += quantity
		aggregates[hour][categoryID] = agg
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO hourly_sales_by_category (time, category_id, total_sales, total_quantity) VALUES ($1, $2, $3, $4)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	for hour, catMap := range aggregates {
		for catID, data := range catMap {
			if _, err := stmt.Exec(hour, catID, data.TotalSales, data.TotalQuantity); err != nil {
				return fmt.Errorf("failed to execute statement: %w", err)
			}
		}
	}

	if err := updateJobStatusInTx(tx, fileName, "SUCCESS"); err != nil {
		return fmt.Errorf("failed to update job status within transaction: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Successfully processed and loaded '%s'.", fileName)
	return nil
}

// processSimpleCSV is a generic function to process a CSV file and load it into a database table.
// It uses a "DELETE ALL then INSERT" strategy within a single transaction.
func processSimpleCSV(db *sql.DB, filePath, fileName, tableName, insertQuery string, rowParser func([]string) ([]interface{}, error)) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("could not open file %s: %w", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Read() // Skip header

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error reading csv %s: %w", fileName, err)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for %s: %w", fileName, err)
	}
	defer tx.Rollback()

	// 1. Clear the table. This is a simple strategy for dimension tables.
	if _, err := tx.Exec(fmt.Sprintf("DELETE FROM %s", tableName)); err != nil {
		return fmt.Errorf("failed to clear table %s: %w", tableName, err)
	}

	// 2. Prepare the insert statement
	stmt, err := tx.Prepare(insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for %s: %w", tableName, err)
	}
	defer stmt.Close()

	// 3. Insert all records
	for _, record := range records {
		args, err := rowParser(record)
		if err != nil {
			log.Printf("Skipping invalid row in %s: %v, error: %v", fileName, record, err)
			continue
		}
		if _, err := stmt.Exec(args...); err != nil {
			return fmt.Errorf("failed to execute statement for %s with args %v: %w", tableName, args, err)
		}
	}

	// 4. Update job status atomically
	if err := updateJobStatusInTx(tx, fileName, "SUCCESS"); err != nil {
		return fmt.Errorf("failed to update job status for %s: %w", fileName, err)
	}

	// 5. Commit the transaction
	log.Printf("Successfully processed and loaded '%s' into table '%s'.", fileName, tableName)
	return tx.Commit()
}

// processMetadata loads product and category info.
func processMetadata(db *sql.DB) error {
	log.Println("Starting metadata processing...")

	// --- Process Categories ---
	catFileName := "categories.csv"
	catFilePath := filepath.Join(rawMetadataPath, catFileName)
	isCatProcessed, err := checkJobStatus(db, catFileName)
	if err != nil {
		return fmt.Errorf("error checking status for %s: %w", catFileName, err)
	}
	if !isCatProcessed {
		log.Printf("Processing %s...", catFileName)
		err := processSimpleCSV(
			db,
			catFilePath,
			catFileName,
			"categories",
			"INSERT INTO categories (category_id, category_name) VALUES ($1, $2)",
			func(record []string) ([]interface{}, error) {
				if len(record) < 2 {
					return nil, fmt.Errorf("invalid record, expected 2 columns: %v", record)
				}
				return []interface{}{record[0], record[1]}, nil
			},
		)
		if err != nil {
			updateJobStatus(db, catFileName, "FAILED")
			return fmt.Errorf("failed to process %s: %w", catFileName, err)
		}
	} else {
		log.Printf("File '%s' already processed. Skipping.", catFileName)
	}

	// --- Process Products ---
	prodFileName := "products.csv"
	prodFilePath := filepath.Join(rawMetadataPath, prodFileName)
	isProdProcessed, err := checkJobStatus(db, prodFileName)
	if err != nil {
		return fmt.Errorf("error checking status for %s: %w", prodFileName, err)
	}
	if !isProdProcessed {
		log.Printf("Processing %s...", prodFileName)
		// FIX: The INSERT statement and parser must match the CSV and table schema.
		// We will insert the description and leave category_id as NULL since it's not in the source CSV.
		err := processSimpleCSV(
			db,
			prodFilePath,
			prodFileName,
			"products",
			// The table has description and category_id, but we only provide 3 values from the CSV.
			"INSERT INTO products (product_id, product_name, description) VALUES ($1, $2, $3)",
			func(record []string) ([]interface{}, error) {
				if len(record) < 3 {
					return nil, fmt.Errorf("invalid record, expected 3 columns: %v", record)
				}
				// This now correctly maps product_id, product_name, and description.
				return []interface{}{record[0], record[1], record[2]}, nil
			},
		)
		if err != nil {
			updateJobStatus(db, prodFileName, "FAILED")
			return fmt.Errorf("failed to process %s: %w", prodFileName, err)
		}
	} else {
		log.Printf("File '%s' already processed. Skipping.", prodFileName)
	}

	return nil
}

// processExternalData loads promotions info.
func processExternalData(db *sql.DB) error {
	log.Println("Starting external data processing...")
	fileName := "promotions.csv"
	filePath := filepath.Join(rawExternalPath, fileName)

	isProcessed, err := checkJobStatus(db, fileName)
	if err != nil {
		return fmt.Errorf("error checking status for %s: %w", fileName, err)
	}
	if isProcessed {
		log.Printf("File '%s' already processed. Skipping.", fileName)
		return nil
	}

	log.Printf("Processing %s...", fileName)
	err = processSimpleCSV(
		db,
		filePath,
		fileName,
		"promotions",
		"INSERT INTO promotions (promotion_id, promotion_name, start_date, end_date, discount_percentage) VALUES ($1, $2, $3, $4, $5)",
		func(record []string) ([]interface{}, error) {
			if len(record) < 5 {
				return nil, fmt.Errorf("invalid record, expected 5 columns: %v", record)
			}

			// --- FIX: Use correct column indexes based on the CSV file ---
			// CSV: promotion_id[0], promotion_name[1], start_date[2], end_date[3], discount_percentage[4]

			startDate, err := time.Parse("2006-01-02", record[2])
			if err != nil {
				return nil, fmt.Errorf("invalid start_date format: %s", record[2])
			}
			endDate, err := time.Parse("2006-01-02", record[3])
			if err != nil {
				return nil, fmt.Errorf("invalid end_date format: %s", record[3])
			}
			discount, err := strconv.ParseFloat(record[4], 64)
			if err != nil {
				return nil, fmt.Errorf("invalid discount value: %s", record[4])
			}

			return []interface{}{record[0], record[1], startDate, endDate, discount}, nil
		},
	)

	if err != nil {
		updateJobStatus(db, fileName, "FAILED")
		return fmt.Errorf("failed to process %s: %w", fileName, err)
	}

	return nil
}

func main() {
	runType := flag.String("type", "all", "Type of data to process: all, sales, metadata, external")
	flag.Parse()

	db := dbConnect()
	defer db.Close()

	switch strings.ToLower(*runType) {
	case "all":
		log.Println("Running all processors concurrently...")
		var wg sync.WaitGroup
		// Create a channel to receive errors from goroutines.
		errChan := make(chan error, 3) // Buffer size matches number of goroutines

		processors := map[string]func(*sql.DB) error{
			"sales":    processSalesData,
			"metadata": processMetadata,
			"external": processExternalData,
		}

		for name, processFunc := range processors {
			wg.Add(1)
			go func(name string, pFunc func(*sql.DB) error) {
				defer wg.Done()
				if err := pFunc(db); err != nil {
					// Send a formatted error to the channel.
					errChan <- fmt.Errorf("processor '%s' failed: %w", name, err)
				}
			}(name, processFunc)
		}

		wg.Wait()
		close(errChan) // Close the channel after all goroutines are done.

		// Check if any errors were reported.
		var hasFailed bool
		for err := range errChan {
			log.Printf("ERROR: %v", err)
			hasFailed = true
		}

		if hasFailed {
			log.Println("One or more processors failed.")
		} else {
			log.Println("All processors finished successfully.")
		}

	case "sales":
		if err := processSalesData(db); err != nil {
			log.Fatalf("Error processing sales data: %v", err)
		}
	case "metadata":
		if err := processMetadata(db); err != nil {
			log.Fatalf("Error processing metadata: %v", err)
		}
	case "external":
		if err := processExternalData(db); err != nil {
			log.Fatalf("Error processing external data: %v", err)
		}
	default:
		log.Fatalf("Invalid type specified: %s. Use 'all', 'sales', 'metadata', or 'external'.", *runType)
	}
}
