package pgc

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/jackc/pgx"
)

const (
	DB_MAX_CONNECTIONS = 50

	// Default environmental var names for postgres connection string
	ENV_VAR_NAME_PG_DB       = "POSTGRES_DB"
	ENV_VAR_NAME_PG_HOST     = "POSTGRES_HOST"
	ENV_VAR_NAME_PG_MIG_PATH = "POSTGRES_MIGRATION_PATH"
	ENV_VAR_NAME_PG_PW       = "POSTGRES_PASSWORD"
	ENV_VAR_NAME_PG_PORT     = "POSTGRES_PORT"
	ENV_VAR_NAME_PG_SSL      = "POSTGRES_SSL_MODE"
	ENV_VAR_NAME_PG_USER     = "POSTGRES_USER"
	ENV_VAR_NAME_LOG_QUERIES = "PGC_LOG_QUERIES"
)

var pool *pgx.ConnPool
var cfg *Config

func init() {
	cfg = &Config{
		EnvVarNameDB:         ENV_VAR_NAME_PG_DB,
		EnvVarNameHost:       ENV_VAR_NAME_PG_HOST,
		EnvVarNameMigPath:    ENV_VAR_NAME_PG_MIG_PATH,
		EnvVarNamePW:         ENV_VAR_NAME_PG_PW,
		EnvVarNamePort:       ENV_VAR_NAME_PG_PORT,
		EnvVarNameSSL:        ENV_VAR_NAME_PG_SSL,
		EnvVarNameUser:       ENV_VAR_NAME_PG_USER,
		EnvVarNameLogQueries: ENV_VAR_NAME_LOG_QUERIES,
	}
}

func GetConfig() *Config {
	return cfg
}

/*
We'll allow consumers to change which OS environmental vars are used to load
postgres connection data

** Note we'll only auto set EnvVarNameLogQueries when calling InitFromEnv
*/
type Config struct {
	Initialized          bool
	LogQueries           bool
	EnvVarNameDB         string
	EnvVarNameHost       string
	EnvVarNameMigPath    string
	EnvVarNamePW         string
	EnvVarNamePort       string
	EnvVarNameSSL        string
	EnvVarNameUser       string
	EnvVarNameLogQueries string
}

// MustInit Initializes the Postgres connection pool or panics
func MustInit(dbName, dbHost, dbUserName, dbPassword string, useTLS bool, dbPort uint16) {
	err := Init(dbName, dbHost, dbUserName, dbPassword, useTLS, dbPort)
	if err != nil {
		panic(fmt.Sprintf("Failed to create a postgres db connection pool to db (%s) user (%s) host (%s), err: (%v)",
			dbName, dbUserName, dbHost, err))
	}
}

// Init Initializes the Postgres connection pool or returns an error
func Init(dbName, dbHost, dbUserName, dbPassword string, useTLS bool, dbPort uint16) error {
	if dbName == "" {
		panic("You must pass a dbName to pgc.Init*")
	}
	if pool != nil {
		panic("pgc connection pool has already been established")
	}
	pgxConfig := pgx.ConnConfig{
		Database: dbName,
		Host:     dbHost,
		User:     dbUserName,
		Password: dbPassword,
		Port:     dbPort,
		Dial:     Dial,
	}
	if useTLS {
		pgxConfig.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	var err error
	pool, err = pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig:     pgxConfig,
		MaxConnections: DB_MAX_CONNECTIONS,
	})
	if err != nil {
		return err
	}
	cfg.Initialized = true
	return nil
}

// Dial wraps standard diel func and tries to reconnect to the specified address on failure.
func Dial(network, addr string) (net.Conn, error) {
	netDial := (&net.Dialer{KeepAlive: 5 * time.Minute}).Dial
	con, err := netDial(network, addr)
	if err != nil {
		for i := 0; i < 2; i++ {
			time.Sleep(2 * time.Second)
			con, err = netDial(network, addr)
			if err == nil {
				break
			}
		}
	}
	return con, err
}

// Initialize PGC from environment Variables
// TODO split this into this and MustInitFromEnv like we did with Init
func InitFromEnv() {
	dbName := os.Getenv(cfg.EnvVarNameDB)
	dbHost := os.Getenv(cfg.EnvVarNameHost)
	dbUserName := os.Getenv(cfg.EnvVarNameUser)
	dbPassword := os.Getenv(cfg.EnvVarNamePW)
	sslMode := os.Getenv(cfg.EnvVarNameSSL)
	var useTLS bool
	if sslMode != "" && sslMode != "disable" {
		useTLS = true
	}
	dbPort := os.Getenv(cfg.EnvVarNamePort)
	var dbPortI int
	var err error
	if dbPort != "" {
		dbPortI, err = strconv.Atoi(dbPort)
		if err != nil {
			panic(fmt.Sprintf("Bad dbPort passed to pgc (%s) via env var (%s), expected a uint16",
				dbPort, cfg.EnvVarNamePort))
		}
	}
	shouldLogQueries := os.Getenv(cfg.EnvVarNameLogQueries)
	if shouldLogQueries == "true" {
		cfg.LogQueries = true
	}
	MustInit(dbName, dbHost, dbUserName, dbPassword, useTLS, uint16(dbPortI))
}

func getConn() *pgx.ConnPool {
	if pool == nil {
		panic("Please call pgc.Init(...) before using")
	}
	return pool
}

func MustPing() {
	var one int
	row := getConn().QueryRow("SELECT 1")

	if row == nil {
		panic("PGC couldnt connect to db, row was nil")
	}
	err := row.Scan(&one)
	if err != nil || one != 1 {
		panic("PGC couldnt connect to db, SELECT 1 wasnt 1")
	}
}

/*
These should only be used during local testing
*/
func CreateDB(namePrefix string) string {
	t := time.Now()
	dbName := fmt.Sprintf("%s_%s", namePrefix, t.Format("2006-01-02-15:04:05"))
	// Create the tmp db, make sure can connect to its
	out, err := exec.Command("createdb", dbName).CombinedOutput()

	if err != nil {
		panic(fmt.Sprintf("Failed to created temporary DB, Output: (%s) err (%s)\n", out, err))
	}

	fmt.Println("Created new tmp testing db", dbName)
	return dbName
}

func DropDB(dbName string) {
	out, err := exec.Command("dropdb", dbName).CombinedOutput()

	if err != nil {
		panic(fmt.Sprintf("Failed to drop temporary DB, Output: (%s) err (%s)\n", out, err))
	}
	fmt.Println("Dumped testing db", dbName)
}

// Can be used to close the pool if you want to drop the database (otherwise pg wont let you due to
// active connection). Test harness uses.
func ClosePool() {
	getConn().Close()
}
