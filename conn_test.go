package pgc

import (
	"flag"
	"os"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

/*
Configure the running of the main to create and config a temp/test db
*/
func TestMain(m *testing.M) {
	flag.Parse()
	envDBName := os.Getenv("POSTGRES_DB")

	var shouldDropDB bool
	if !strings.HasPrefix(envDBName, "cyclops_tmp") {
		// Create and new a tmp db
		envDBName = CreateDB("cyclops_tmp")
		shouldDropDB = true

	}
	// Will default to the current os username, no pass, no tls
	Init(envDBName, "localhost", "", "", false, 0)

	// Set up spew to not use string reps (helpful for buggin')
	spew.Config.DisableMethods = true

	exitVal := m.Run()
	ClosePool()
	if shouldDropDB {
		DropDB(envDBName)
	}
	os.Exit(exitVal)
}

// Just to get some coverage on our init process beyond TestMain above
func TestInitFromEnvBadPort(t *testing.T) {
	myConfig := GetConfig()

	badPort := "2lk3j4lk234j"

	os.Setenv(myConfig.EnvVarNamePort, badPort)
	func() { // Call an inline function to test and deal with the panic
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("TestInitFromEnvBadPort should have panicked on bad port (%s)",
					badPort)
			}
		}()
		InitFromEnv()
	}()
}

func TestInitMissingDBName(t *testing.T) {
	func() { // Call an inline function to test and deal with the panic
		defer func() {
			if r := recover(); r == nil {
				t.Error("TestInitMissingDBName should have panicked on missing db name")
			}
		}()
		Init("", "localhost", "", "", false, 0)
	}()
}

// Not going to bother with a negative test here since it will
// require a way to destory the connection pool, ug
func TestMustPing(t *testing.T) {
	MustPing()
}
