package pgc

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cliqueinc/pgc/pgcq"
	"github.com/cliqueinc/pgc/util"
)

const (
	// Set *something* as our schema version table's primary key
	schemaID       = "pgc_default"
	actionInit     = "init"
	actionUpdate   = "update"
	actionStatus   = "status"
	actionRollback = "rollback"
	actionReset    = "reset"

	PGECTableDNE        = "42P01"
	PGECTableExists     = "42P07"
	PGECUniqueViolation = "23505"

	VersionTimeFormat = "2006-01-02:15:04:05"

	// DefaultVersion is used for keeping the default schema that we don't want to execute (but still can if needed)
	DefaultVersion = "0000-00-00:00:00:00"
)

var (
	// mh keeps all migrration data and handles migration operations.
	mh *MigrationHandler
)

// RegisterMigrationPath registers migration path for performing migration operations.
func RegisterMigrationPath(migrationPath string) error {
	if migrationPath == "" {
		return fmt.Errorf("invalid migration path: %s", migrationPath)
	}
	mh = &MigrationHandler{
		MigrationPath: migrationPath,
	}

	files, err := ioutil.ReadDir(migrationPath)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if !strings.HasSuffix(f.Name(), ".sql") || strings.HasSuffix(f.Name(), "_down.sql") {
			continue
		}

		versionName := strings.TrimRight(f.Name(), ".sql")
		if _, err := time.Parse(VersionTimeFormat, versionName); err != nil && versionName != DefaultVersion {
			return fmt.Errorf("unrecognized version (%s) format: %v", versionName, err)
		}
		var upSQL, downSQL string

		data, err := ioutil.ReadFile(migrationPath + "/" + f.Name())
		if err != nil {
			return fmt.Errorf("fail read migration %s file: %v", versionName, err)
		}
		upSQL = string(data)
		downFileName := migrationPath + "/" + versionName + "_down.sql"
		if _, err := os.Stat(downFileName); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("cannot open version (%s) down sql: %v", versionName, err)
			}
		} else {
			data, err := ioutil.ReadFile(downFileName)
			if err != nil {
				return fmt.Errorf("fail read migration %s down file: %v", versionName, err)
			}
			downSQL = string(data)
		}
		mh.RegisterMigration(versionName, upSQL, downSQL)
	}

	return nil
}

// migrationHandler handles all db migrations.
type MigrationHandler struct {
	migrations map[string]migration

	// migration paht is used to determine absolute path to each migration so pgc cmd tool may be called from everywhere.
	// If not set, relative path is used to find migration file.
	MigrationPath string
}

type migration struct {
	upSQL   string
	downSQL string

	// isDefault indicates whether this migration is the default schema.
	isDefault bool
}

// RegisterMigration registers migration to process during migration update.
func (h *MigrationHandler) RegisterMigration(name string, upSQL, downSQL string) {
	if h.migrations == nil {
		h.migrations = make(map[string]migration)
	}
	if _, ok := h.migrations[name]; ok {
		panic(fmt.Sprintf("migration (%s) has already been registered", name))
	}
	h.migrations[name] = migration{upSQL: upSQL, downSQL: downSQL, isDefault: name == DefaultVersion}
}

// MigrationVersions returns slice of sorted migration versions.
func (h *MigrationHandler) MigrationVersions() []string {
	versions := make([]string, 0, len(h.migrations))
	for v := range h.migrations {
		versions = append(versions, v)
	}
	sort.Strings(versions)

	return versions
}

// SchemaMigration tracks all performed migrations.
type SchemaMigration struct {
	Version string `pgc:"pk"`
	Created time.Time
}

func (pgcsv SchemaMigration) TableName() string { return "pgc_schema_migration" }

// MigrationLog tracks all migration activity.
type MigrationLog struct {
	Action  string
	Created time.Time
	ID      string
	Message string
	Success bool
	Version string
}

func (pgcsv MigrationLog) TableName() string { return "pgc_migration_log" }

// If this wasn't successful, manually set that fact after calling this
func NewLog(action, message string, version string) *MigrationLog {
	return &MigrationLog{
		ID:      util.NewGuid(),
		Created: time.Now().UTC(),
		Action:  action,
		Message: message,
		Version: version,
		Success: true,
	}
}

/*
Set up pgc schema migration on a new pg database without pgc_schema_migration
OR pgc_migration_log tables. We'll fail if either exists.

Passing execDefault=true will run the default schema 000-00-00:00:00:00.sql
*/
func InitSchema(execDefault bool) error {
	if mh == nil {
		return errors.New("call pgc.RegisterMigrationPath befoere performing any migration operation")
	}

	created, errNum := createTableIfNotExists(&SchemaMigration{})
	if errNum != nil {
		return errors.New("unable to init schema versioning table, cant create")
	}
	schemaVersionLog := NewLog(actionInit, "Creating pgc schema version and log tables", "")
	created, errNum = createTableIfNotExists(schemaVersionLog)
	if errNum != nil {
		return errors.New("unable to init schema versioning log table, cant create")
	}

	var existing []SchemaMigration
	MustSelect(&existing, pgcq.Order("version", pgcq.ASC))

	for _, migration := range existing {
		if _, ok := mh.migrations[migration.Version]; !ok {
			fmt.Printf("Warning: couldn't find schema version (%s)\n", migration.Version)
		}
	}

	// Table created, insert initial log row
	MustInsert(schemaVersionLog)
	// At this point the schema versioning tables should be ready to go.
	// Typically you would call UpdateSchema after this
	if created { // Can this ever be false at this point?
		fmt.Println("Schema versioning is now initialized. Run `$ pgccmd status` for info")
		if execDefault { // If you have a default schema, you might want to run it when no migration
			// table exist yet like in the case with a new local install or new server environment
			return ExecuteMigration("default") // Return any associated error
		}
	}
	return nil
}

func createTableIfNotExists(model interface{}) (bool, error) {
	if err := CreateTable(model); err != nil {
		if IsTableExistsError(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// UpdateSchema updates schema in case if new migrations appear.
// if execDefault param is set to true, default schema will be executed if exists.
func UpdateSchema(execDefault bool) error {
	if mh == nil {
		return errors.New("call pgc.RegisterMigrationPath befoere performing any migration operation")
	}

	var existing []SchemaMigration
	MustSelect(&existing, pgcq.Order("version", pgcq.ASC))

	existingMap := make(map[string]struct{})
	for _, item := range existing {
		existingMap[item.Version] = struct{}{}
	}

	installedMigrations := make([]string, 0, len(mh.migrations))
	for _, version := range mh.MigrationVersions() {
		m, ok := mh.migrations[version]
		if !ok {
			continue
		}
		if _, ok := existingMap[version]; ok {
			continue
		}

		if m.upSQL == "" {
			return fmt.Errorf("migration (%s): up function not defined", version)
		}

		if m.isDefault && !execDefault {
			continue
		}

		migrationFunc := func(a *MigrationAdapter) error {
			if err := a.Exec(m.upSQL); err != nil {
				return err
			}
			if err := a.Insert(&SchemaMigration{Version: version, Created: time.Now().UTC()}); err != nil {
				return err
			}

			return nil
		}

		if err := execInTx(version, migrationFunc); err != nil {
			return fmt.Errorf("fail update to version (%s): %v", version, err)
		}

		installedMigrations = append(installedMigrations, version)
	}

	if len(installedMigrations) == 0 {
		fmt.Printf("Schema is up to date\n")
		return nil
	}

	fmt.Printf("*** Migration(s) (%s) have been installed ***\n", strings.Join(installedMigrations, ", "))
	return nil
}

// execInTx executes migration under transaction, panics on transaction errors.
func execInTx(version string, migrationFunc func(a *MigrationAdapter) error) error {
	tx, err := Begin()
	if err != nil {
		return fmt.Errorf("fail start transaction: %v", err)
	}

	migrationAdapter := &MigrationAdapter{crudAdapter: &crudAdapter{con: tx.con}}
	if err := migrationFunc(migrationAdapter); err != nil {
		if err := tx.Rollback(); err != nil {
			panic(fmt.Sprintf("migration (%s): failed to rollback transaction: %v\n", version, err))
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		panic(fmt.Sprintf("migration (%s): failed to commit transaction: %v", version, err))
	}

	return nil
}

// ExecuteMigration executes particular migration.
func ExecuteMigration(version string) error {
	if mh == nil {
		return errors.New("call pgc.RegisterMigrationPath before performing any migration operation")
	}
	if version == "default" {
		version = DefaultVersion
	}

	m, ok := mh.migrations[version]
	if !ok {
		return fmt.Errorf("migration (%s) not found", version)
	}

	if m.upSQL == "" {
		return fmt.Errorf("migration (%s): up sql not defined", version)
	}

	migrationFunc := func(a *MigrationAdapter) error {
		if err := a.Exec(m.upSQL); err != nil {
			return err
		}
		schemaMigration := &SchemaMigration{}
		found, err := Get(schemaMigration, pgcq.Equal("version", version))
		if err != nil {
			return fmt.Errorf("fail get schema migration (%s): %v", version, err)
		}
		if !found {
			if err := a.Insert(&SchemaMigration{Version: version, Created: time.Now().UTC()}); err != nil {
				return err
			}
		}

		return nil
	}

	if err := execInTx(version, migrationFunc); err != nil {
		return fmt.Errorf("fail execute migration (%s): %v", version, err)
	}

	return nil
}

// InitMigration generates new migration file.
func InitMigration(isDefault bool) error {
	newVersion := time.Now().UTC().Format(VersionTimeFormat)
	if isDefault {
		newVersion = DefaultVersion
	}

	f, err := os.Create(getMigrationPath() + newVersion + ".sql")
	if err != nil {
		return fmt.Errorf("fail create migration sql file: %v", err)
	}
	sqlTmpl := `-- paste here migration sql code`
	if _, err := f.WriteString(sqlTmpl); err != nil {
		f.Close()
		return fmt.Errorf("fail init sql file: %v", err)
	}
	f.Close()

	f, err = os.Create(getMigrationPath() + newVersion + "_down.sql")
	if err != nil {
		return fmt.Errorf("fail create down migration sql file: %v", err)
	}
	sqlTmpl = `-- paste here migration rollback sql code`
	if _, err := f.WriteString(sqlTmpl); err != nil {
		f.Close()
		os.Remove(getMigrationPath() + newVersion + ".sql")
		return fmt.Errorf("fail init sql file: %v", err)
	}
	f.Close()

	return nil
}

// RollbackLatest rollbacks latest migration.
func RollbackLatest() error {
	if mh == nil {
		return errors.New("call pgc.RegisterMigrationPath befoere performing any migration operation")
	}
	latestVersion := &SchemaMigration{}
	found, err := Get(latestVersion, pgcq.Order("version", pgcq.DESC))
	if err != nil {
		return fmt.Errorf("fail get latest schema version: %v", err)
	}
	if !found {
		fmt.Printf("Nothing to rollback\n")
		return nil
	}
	m, found := mh.migrations[latestVersion.Version]
	if !found {
		return fmt.Errorf("migration (%s) not found", latestVersion.Version)
	}
	if err := rollback(latestVersion.Version, m); err != nil {
		return err
	}

	previousMigration := &SchemaMigration{}
	MustGet(previousMigration, pgcq.Order("version", pgcq.DESC)) // if not found, Version will be empty string, which is fine
	sLog := NewLog(
		actionRollback,
		fmt.Sprintf("Rolled back from \"%s\" to \"%s\"", latestVersion.Version, previousMigration.Version),
		latestVersion.Version,
	)
	MustInsert(sLog)

	fmt.Printf("Rolled back from \"%s\" to \"%s\"\n", latestVersion.Version, previousMigration.Version)

	return nil
}

// Rollback rollbacks particular migration.
func Rollback(version string) error {
	if mh == nil {
		return errors.New("call pgc.RegisterMigrationPath befoere performing any migration operation")
	}
	if version == "default" {
		version = DefaultVersion
	}
	schemaMigration, found := mh.migrations[version]
	if !found {
		return fmt.Errorf("migration (%s) not found", version)
	}
	if err := rollback(version, schemaMigration); err != nil {
		return err
	}

	sLog := NewLog(
		actionRollback,
		fmt.Sprintf("Rolled back migration \"%s\"", version),
		version,
	)
	MustInsert(sLog)

	fmt.Printf("Rolled back migration \"%s\"\n", version)

	return nil
}

// reset resets all migration data.
func reset() error {
	_, err := DeleteRows(&SchemaMigration{}, pgcq.All())
	if err != nil {
		return err
	}

	sLog := NewLog(actionReset, "Reset all data", "")
	MustInsert(sLog)

	fmt.Print("Migation data has been reseted\n")
	return nil
}

// rollback rollbacks particular migration.
func rollback(version string, m migration) error {
	if mh == nil {
		return errors.New("call pgc.RegisterMigrationPath befoere performing any migration operation")
	}

	if m.downSQL == "" {
		return fmt.Errorf("migration (%s): down sql not found", version)
	}

	migrationFunc := func(a *MigrationAdapter) error {
		if err := a.Exec(m.downSQL); err != nil {
			return err
		}
		if _, err := a.DeleteRows(&SchemaMigration{}, pgcq.Equal("version", version)); err != nil {
			return err
		}

		return nil
	}

	if err := execInTx(version, migrationFunc); err != nil {
		return fmt.Errorf("fail rollback version (%s): %v", version, err)
	}

	return nil
}

// TODO print the version and latest X logs in a less retarded way
func PrintVersionStatus() error {
	logs := []MigrationLog{}
	if err := Select(&logs, pgcq.Order("created", pgcq.DESC), pgcq.Limit(10)); err != nil {
		return fmt.Errorf("can't get latest logs: %v", err)
	}

	latestMigrations := []SchemaMigration{}
	if err := Select(&latestMigrations, pgcq.Order("version", pgcq.DESC), pgcq.Limit(10)); err != nil {
		return fmt.Errorf("can't get latest migrations: %v", err)
	}

	fmt.Println("--------------------------------------------------")
	fmt.Println("Last 10 schema change logs")
	fmt.Println("--------------------------------------------------")
	// common.PrettyPrintJson(logs)

	fmt.Println("--------------------------------------------------")
	fmt.Println("Latest migrations info")
	fmt.Println("--------------------------------------------------")
	if len(latestMigrations) != 0 {
		for i := range latestMigrations {
			fmt.Println(latestMigrations[len(latestMigrations)-i-1].Version)
		}
		fmt.Println("--------------------------------------------------")
	} else {
		fmt.Printf("No migrations so far\n")
		fmt.Println("--------------------------------------------------")
	}

	return nil
}

func getMigrationPath() string {
	if mh != nil && mh.MigrationPath != "" {
		return filepath.Clean(mh.MigrationPath) + "/"
	}

	return "./"
}
