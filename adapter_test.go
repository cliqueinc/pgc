package pgc_test

import (
	"testing"
	"time"

	"github.com/cliqueinc/pgc"
	"github.com/cliqueinc/pgc/util"
)

func TestAdapter(t *testing.T) {
	t.Log("start adapter")
	type fakeProfile struct {
		Bio     string
		PicUrl  string
		Meta    map[string]interface{}
		SubTime time.Time
	}

	type fakeAddress struct {
		Street string
		State  string
		City   string
	}

	type weird struct {
		Name            string
		OptionalProfile *fakeProfile
	}

	type fakeTransactionUser struct {
		ID        string
		FirstName string
		LastName  string
		Emails    []string
		Addresses []fakeAddress
		Profile   fakeProfile
		Meta      map[string]interface{}
		Created   time.Time
	}

	fu := &fakeTransactionUser{
		ID:        util.RandomString(30),
		FirstName: "Jym",
		LastName:  "Luast",
		Emails:    []string{"but@butts.com", "second@whowhooo.com"},
		Addresses: []fakeAddress{
			{"750 N. San Vicente", "CA", "LA"},
			{"8800 Sunset BLVD", "CA", "LA"},
		},
		Profile: fakeProfile{"Some Lame bio", "http://picsrus.com/23ds34", map[string]interface{}{
			"facebookid": "123k1l2j34l13", "twitterid": "sdklj324lkj23"}, time.Now()},
		Meta: map[string]interface{}{
			"favcolor": "brown", "fakeage": 42,
		},
	}
	fu2 := &fakeTransactionUser{
		ID:        util.RandomString(30),
		FirstName: "John",
		LastName:  "Snow",
		Emails:    []string{"but@butts.com", "second@whowhooo.com"},
		Addresses: []fakeAddress{
			{"750 N. San Vicente", "CA", "LA"},
			{"8800 Sunset BLVD", "CA", "LA"},
		},
		Profile: fakeProfile{"Some Lame bio", "http://picsrus.com/23ds34", map[string]interface{}{
			"facebookid": "123k1l2j34l13", "twitterid": "sdklj324lkj23"}, time.Now()},
		Meta: map[string]interface{}{
			"favcolor": "brown", "fakeage": 42,
		},
	}

	t.Run("CRUD", func(t *testing.T) {
		type adapterUser struct {
			ID   string
			Name string
		}

		u := &adapterUser{
			ID:   util.RandomString(30),
			Name: util.RandomString(10),
		}

		a := pgc.NewAdapter()
		pgc.MustCreateTable(u)
		if err := a.Insert(u); err != nil {
			t.Fatalf("failed to insert row: %s", err)
		}
	})

	if err := pgc.CreateTable(fu); err != nil {
		t.Fatalf("failed to create schema: %s", err)
	}

	t.Run("Rollback", func(t *testing.T) {
		a, err := pgc.Begin()
		if err != nil {
			t.Fatalf("cannot begin transaction: %s", err)
		}

		err = a.Insert(fu)
		if err != nil {
			t.Fatalf("failed to insert row: %s", err)
		}
		err = a.Insert(fu2)
		if err != nil {
			t.Fatalf("failed to insert row: %s", err)
		}

		fu.FirstName = util.RandomString(30)
		err = a.Update(fu)
		if err != nil {
			t.Fatalf("failed to update row: %s", err)
		}

		fu3 := &fakeTransactionUser{ID: fu.ID}
		found, err := a.Get(fu3)
		if err != nil {
			t.Fatalf("failed to get row: %s", err)
		}
		if !found {
			t.Fatalf("user %s not found", fu3.ID)
		}

		if fu3.FirstName != fu.FirstName {
			t.Errorf("FirstName wasnt changed after update, expected (%s) was (%s)",
				fu3.FirstName, fu.FirstName)
		}

		err = a.Delete(fu2)
		if err != nil {
			t.Fatalf("failed to delete row: %s", err)
		}

		err = a.Rollback()
		if err != nil {
			t.Fatalf("failed to rollback transaction: %s", err)
		}

		fu3 = &fakeTransactionUser{ID: fu.ID}
		found, err = pgc.Get(fu3)
		if found {
			t.Error("transaction changes should have been discarded")
		}
	})

	t.Run("Commit", func(t *testing.T) {
		a, err := pgc.Begin()
		if err != nil {
			t.Fatalf("cannot begin transaction: %s", err)
		}

		err = a.Insert(fu)
		if err != nil {
			t.Fatalf("failed to insert row: %s", err)
		}

		err = a.Commit()
		if err != nil {
			t.Fatalf("failed to commit transaction: %s", err)
		}

		fu3 := &fakeTransactionUser{ID: fu.ID}
		_, err = pgc.Get(fu3)
		if err != nil || fu3.FirstName != fu.FirstName {
			t.Error("transaction changes should have been preserved")
		}
	})
}

type User struct {
	ID   string
	Name string
}

func ExampleAdapter() {
	a := pgc.NewAdapter()

	u := &User{
		ID:   util.RandomString(30),
		Name: util.RandomString(10),
	}

	// create db table
	if err := pgc.CreateTable(u); err != nil {
		panic(err)
	}
	// common CRUD
	if err := a.Insert(u); err != nil {
		panic(err)
	}
	u.Name = util.RandomString(10)
	if err := a.Update(u); err != nil {
		panic(err)
	}
	if _, err := a.Get(u); err != nil {
		panic(err)
	}
	if err := a.Delete(u); err != nil {
		panic(err)
	}

	// start transaction ops
	tx, err := a.Begin()
	if err != nil {
		panic(err)
	}

	if err := tx.Insert(u); err != nil {
		panic(err)
	}
	if err := tx.Rollback(); err != nil {
		panic(err)
	}
}

func ExampleBegin() {
	u := &User{
		ID:   util.RandomString(30),
		Name: util.RandomString(10),
	}

	// start transaction ops
	tx, err := pgc.Begin()
	if err != nil {
		panic(err)
	}

	if err := tx.Insert(u); err != nil {
		panic(err)
	}
	// or tx.Commit()
	if err := tx.Rollback(); err != nil {
		panic(err)
	}
}
