package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/marcboeker/go-duckdb"
	"github.com/nats-io/nats.go"
)

type DuckDBStorage struct {
	js     nats.JetStreamContext
	obs    nats.ObjectStore
	bucket string
	dbName string
}

// NewDuckDBStorage creates a new storage handler for DuckDB files
func NewDuckDBStorage(nc *nats.Conn, bucket, dbName string) (*DuckDBStorage, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:      bucket,
		Description: "DuckDB database storage",
	})
	if err != nil {
		obs, err = js.ObjectStore(bucket)
		if err != nil {
			return nil, fmt.Errorf("failed to create/get object store: %w", err)
		}
	}

	return &DuckDBStorage{
		js:     js,
		obs:    obs,
		bucket: bucket,
		dbName: dbName,
	}, nil
}

// createSampleDatabase creates a sample DuckDB database with some data
func createSampleDatabase(dbPath string) error {
	// Ensure the directory exists
	err := os.MkdirAll(filepath.Dir(dbPath), 0755)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Connect to DuckDB
	connector, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	// Create a sample table and insert data
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			created_at TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert sample data
	_, err = db.Exec(`
		INSERT INTO users (id, name, created_at) VALUES 
		(1, 'Alice', CURRENT_TIMESTAMP),
		(2, 'Bob', CURRENT_TIMESTAMP),
		(3, 'Charlie', CURRENT_TIMESTAMP)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	return nil
}

// StoreDuckDB stores a DuckDB database file in NATS object store
func (d *DuckDBStorage) StoreDuckDB(dbFilePath string) error {
	file, err := os.Open(dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open database file: %w", err)
	}
	defer file.Close()

	_, err = d.obs.Put(&nats.ObjectMeta{
		Name:        d.dbName,
		Description: "DuckDB database file",
		Headers: nats.Header{
			"Content-Type": []string{"application/x-duckdb"},
			"Timestamp":    []string{time.Now().UTC().Format(time.RFC3339)},
		},
	}, file)

	if err != nil {
		return fmt.Errorf("failed to store database in NATS: %w", err)
	}

	return nil
}

// RetrieveDuckDB retrieves a DuckDB database file from NATS object store
func (d *DuckDBStorage) RetrieveDuckDB(outputPath string) error {
	obj, err := d.obs.Get(d.dbName)
	if err != nil {
		return fmt.Errorf("failed to retrieve database from NATS: %w", err)
	}
	defer obj.Close()

	// Ensure the directory exists
	err = os.MkdirAll(filepath.Dir(outputPath), 0755)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, obj)
	if err != nil {
		return fmt.Errorf("failed to write database to file: %w", err)
	}

	return nil
}

// GetInfo retrieves information about the stored database
func (d *DuckDBStorage) GetInfo() (*nats.ObjectInfo, error) {
	return d.obs.GetInfo(d.dbName)
}

// DeleteDatabase removes the database from the object store
func (d *DuckDBStorage) DeleteDatabase() error {
	return d.obs.Delete(d.dbName)
}

// VerifyDatabase checks if the retrieved database is accessible and contains the expected data
func VerifyDatabase(dbPath string) error {
	connector, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to query database: %w", err)
	}

	fmt.Printf("Database verified: found %d users\n", count)
	return nil
}

func main() {
	// Connect to NATS server
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		fmt.Printf("Failed to connect to NATS: %v\n", err)
		return
	}
	defer nc.Close()

	// Define paths
	dbPath := "./tmp/mydb.db"
	retrievedDbPath := "./tmp/mydb_retrieved.db"

	// Create a sample database
	fmt.Println("Creating sample database...")
	err = createSampleDatabase(dbPath)
	if err != nil {
		fmt.Printf("Failed to create sample database: %v\n", err)
		return
	}

	// Create storage handler
	storage, err := NewDuckDBStorage(nc, "DUCKDB", "mydb.db")
	if err != nil {
		fmt.Printf("Failed to create storage handler: %v\n", err)
		return
	}

	// Store database
	fmt.Println("Storing database in NATS...")
	err = storage.StoreDuckDB(dbPath)
	if err != nil {
		fmt.Printf("Failed to store database: %v\n", err)
		return
	}

	// Get info about stored database
	info, err := storage.GetInfo()
	if err != nil {
		fmt.Printf("Failed to get info: %v\n", err)
		return
	}
	fmt.Printf("Database stored: Size=%d bytes, ModTime=%v\n", info.Size, info.ModTime)

	// Retrieve database
	fmt.Println("Retrieving database from NATS...")
	err = storage.RetrieveDuckDB(retrievedDbPath)
	if err != nil {
		fmt.Printf("Failed to retrieve database: %v\n", err)
		return
	}

	// Verify the retrieved database
	fmt.Println("Verifying retrieved database...")
	err = VerifyDatabase(retrievedDbPath)
	if err != nil {
		fmt.Printf("Failed to verify database: %v\n", err)
		return
	}

	fmt.Println("Operation completed successfully!")
}
