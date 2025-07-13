package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
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

	"github.com/lib/pq"
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
		return fmt.Errorf("failed to update job status for %s: %w", fileName, err)
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

// processSingleSalesFile reads, transforms, and loads a single sales CSV file.
func processSingleSalesFile(db *sql.DB, fileName string) (err error) { // Named return for easier defer handling
	filePath := filepath.Join(rawSalesPath, fileName)
	log.Printf("[Worker] Starting to process file: %s", fileName)

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("could not open sales file %s: %w", fileName, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read() // Skip header
	if err != nil {
		return fmt.Errorf("could not read header from %s: %w", fileName, err)
	}

	type HourlyAggregate struct {
		TotalSales    float64
		TotalQuantity int
	}
	aggregates := make(map[time.Time]map[string]HourlyAggregate)

	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading csv %s: %w", fileName, err)
		}

		if len(record) < 6 {
			continue
		}
		quantity, _ := strconv.Atoi(record[3])
		price, _ := strconv.ParseFloat(record[4], 64)
		if quantity <= 0 || price <= 0 {
			continue
		}

		// *** PRIMARY FIX: Use a more flexible timestamp layout to match Python's isoformat() ***
		const layout = "2006-01-02T15:04:05.999999"
		timestamp, err := time.Parse(layout, record[5])
		if err != nil {
			// Fallback for formats without microseconds
			const fallbackLayout = "2006-01-02T15:04:05"
			timestamp, err = time.Parse(fallbackLayout, record[5])
			if err != nil {
				log.Printf("Skipping row due to invalid timestamp in %s: %s", fileName, record[5])
				continue
			}
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

	if len(aggregates) == 0 {
		log.Printf("[Worker] No valid data found in %s to process. Marking as SUCCESS.", fileName)
		return updateJobStatus(db, fileName, "SUCCESS")
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for %s: %w", fileName, err)
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	stmt, err := tx.Prepare("INSERT INTO hourly_sales_by_category (time, category_id, total_sales, total_quantity) VALUES ($1, $2, $3, $4)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement for %s: %w", fileName, err)
	}

	for hour, catMap := range aggregates {
		for catID, data := range catMap {
			if _, err := stmt.Exec(hour, catID, data.TotalSales, data.TotalQuantity); err != nil {
				return fmt.Errorf("failed to execute statement for %s: %w", fileName, err)
			}
		}
	}

	if err := updateJobStatusInTx(tx, fileName, "SUCCESS"); err != nil {
		return fmt.Errorf("failed to update job status for %s: %w", fileName, err)
	}

	log.Printf("[Worker] Successfully processed and loaded '%s'.", fileName)
	return nil
}

// processAllSalesFiles is the main orchestrator function.
func processAllSalesFiles(db *sql.DB) error {
	log.Println("--- Starting sales data processing cycle ---")

	dirEntries, err := os.ReadDir(rawSalesPath)
	if err != nil {
		return fmt.Errorf("failed to read sales directory: %w", err)
	}

	var fileNames []string
	for _, entry := range dirEntries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".csv") {
			fileNames = append(fileNames, entry.Name())
		}
	}

	if len(fileNames) == 0 {
		log.Println("No sales files found to process.")
		return nil
	}

	query := "SELECT file_name, status FROM etl_job_status WHERE file_name = ANY($1)"
	rows, err := db.Query(query, pq.Array(fileNames))
	if err != nil {
		return fmt.Errorf("failed to query job statuses: %w", err)
	}
	defer rows.Close()

	jobStatuses := make(map[string]string)
	for rows.Next() {
		var fileName, status string
		if err := rows.Scan(&fileName, &status); err != nil {
			return fmt.Errorf("failed to scan job status row: %w", err)
		}
		jobStatuses[fileName] = status
	}

	var filesToProcess []string
	for _, fileName := range fileNames {
		// *** IDEMPOTENCY FIX: Process any file that is NOT marked as SUCCESS ***
		if status, exists := jobStatuses[fileName]; !exists || status != "SUCCESS" {
			filesToProcess = append(filesToProcess, fileName)
		}
	}

	if len(filesToProcess) == 0 {
		log.Println("All sales files are already processed successfully. Nothing to do.")
		return nil
	}

	log.Printf("Found %d files to process: %v", len(filesToProcess), filesToProcess)

	var wg sync.WaitGroup
	for _, fileName := range filesToProcess {
		wg.Add(1)
		go func(fName string) {
			defer wg.Done()
			// Mark as PENDING before starting
			if err := updateJobStatus(db, fName, "PENDING"); err != nil {
				log.Printf("[Worker] ERROR failed to mark %s as PENDING: %v", fName, err)
				return
			}

			if err := processSingleSalesFile(db, fName); err != nil {
				log.Printf("[Worker] ERROR processing file %s: %v", fName, err)
				// On error, mark as FAILED
				updateJobStatus(db, fName, "FAILED")
			}
		}(fileName)
	}

	wg.Wait()
	log.Println("--- Sales data processing cycle finished ---")
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

// *** REFACTORED METADATA PROCESSING LOGIC ***
func processMetadata(db *sql.DB) (err error) {
	log.Println("Starting metadata processing...")
	jobName := "metadata_full_refresh"

	// Check status for the entire metadata job
	var status string
	err = db.QueryRow("SELECT status FROM etl_job_status WHERE file_name = $1", jobName).Scan(&status)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query metadata job status: %w", err)
	}
	// For simplicity, we re-run this job every time. In production, you might check for success.

	// Open source files
	catFile, err := os.Open(filepath.Join(rawMetadataPath, "categories.csv"))
	if err != nil {
		return fmt.Errorf("could not open categories file: %w", err)
	}
	defer catFile.Close()

	prodFile, err := os.Open(filepath.Join(rawMetadataPath, "products.csv"))
	if err != nil {
		return fmt.Errorf("could not open products file: %w", err)
	}
	defer prodFile.Close()

	// Read all data into memory first
	catReader := csv.NewReader(catFile)
	catReader.Read() // Skip header
	catRecords, err := catReader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read categories csv: %w", err)
	}

	prodReader := csv.NewReader(prodFile)
	prodReader.Read() // Skip header
	prodRecords, err := prodReader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read products csv: %w", err)
	}

	// Begin a single transaction for the entire operation
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin metadata transaction: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	// 1. Delete from products first to respect foreign key
	if _, err = tx.Exec("DELETE FROM products"); err != nil {
		return fmt.Errorf("failed to clear products table: %w", err)
	}
	log.Println("Cleared 'products' table.")

	// 2. Delete from categories
	if _, err = tx.Exec("DELETE FROM categories"); err != nil {
		return fmt.Errorf("failed to clear categories table: %w", err)
	}
	log.Println("Cleared 'categories' table.")

	// 3. Insert into categories
	catStmt, err := tx.Prepare("INSERT INTO categories (category_id, category_name) VALUES ($1, $2)")
	if err != nil {
		return fmt.Errorf("failed to prepare category statement: %w", err)
	}
	for _, record := range catRecords {
		if len(record) < 2 { continue }
		if _, err = catStmt.Exec(record[0], record[1]); err != nil {
			return fmt.Errorf("failed to insert category record %v: %w", record, err)
		}
	}
	log.Println("Loaded new data into 'categories' table.")

	// 4. Insert into products
	prodStmt, err := tx.Prepare("INSERT INTO products (product_id, product_name, description, category_id) VALUES ($1, $2, $3, $4)")
	if err != nil {
		return fmt.Errorf("failed to prepare product statement: %w", err)
	}
	for _, record := range prodRecords {
		if len(record) < 4 { continue }
		if _, err = prodStmt.Exec(record[0], record[1], record[2], record[3]); err != nil {
			return fmt.Errorf("failed to insert product record %v: %w", record, err)
		}
	}
	log.Println("Loaded new data into 'products' table.")

	// 5. Update the status for the logical job
	if err = updateJobStatusInTx(tx, jobName, "SUCCESS"); err != nil {
		return fmt.Errorf("failed to update metadata job status: %w", err)
	}

	log.Println("Metadata processing finished successfully.")
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
	// *** FIX: Update INSERT statement and parser to handle all 7 columns from the new promotions.csv ***
	err = processSimpleCSV(
		db,
		filePath,
		fileName,
		"promotions",
		"INSERT INTO promotions (promotion_id, promotion_name, start_date, end_date, discount_percentage, target_type, target_id) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		func(record []string) ([]interface{}, error) {
			if len(record) < 7 {
				return nil, fmt.Errorf("invalid record, expected 7 columns: %v", record)
			}

			// CSV: promotion_id[0], promotion_name[1], start_date[2], end_date[3], discount_percentage[4], target_type[5], target_id[6]
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

			// target_type and target_id are simple strings from the CSV
			targetType := record[5]
			targetID := record[6]

			return []interface{}{record[0], record[1], startDate, endDate, discount, targetType, targetID}, nil
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
			"sales":    processAllSalesFiles,
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
		if err := processAllSalesFiles(db); err != nil {
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
