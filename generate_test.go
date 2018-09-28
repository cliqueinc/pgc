package pgc_test

import (
	"fmt"
	"time"

	"github.com/cliqueinc/pgc"
)

// Example email struct used for generating sql, model, test
type Email struct {
	ID              string
	EmailID         string
	RejectionReason string
	Sent            time.Time
	Created         time.Time
	Updated         time.Time
}

func ExampleGenerateModel() {
	fmt.Println(pgc.GenerateSchema(&Email{}))
	fmt.Println(pgc.GenerateModel(&Email{}, "email"))
	fmt.Println(pgc.GenerateModelTest(&Email{}, "email"))
}
