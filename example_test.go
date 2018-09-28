package pgc_test

import (
	"time"

	"github.com/cliqueinc/pgc"
	"github.com/cliqueinc/pgc/pgcq"
)

func ExampleSelect() {
	type user struct {
		Name string
	}

	var users []user
	pgc.Select(
		&users,
		pgcq.Equal("id", 123),
		pgcq.OR(
			pgcq.Equal("id", 123),
			pgcq.LessThan("name", 123),
			pgcq.AND(
				pgcq.NotEqual("id", 123),
				pgcq.GreaterOrEqual("name", 123),
			),
		),
		pgcq.Limit(10),
		pgcq.Order("id", pgcq.DESC),
	)

	type fakeBlog struct {
		Name         string
		Descr        string
		ID           string
		PublishStart time.Time
	}

	var fetchedBlogs []fakeBlog
	pgc.Select(
		&fetchedBlogs,
		pgcq.OR(
			pgcq.Equal("name", "blog4"),
			pgcq.AND(
				pgcq.Equal("descr", "descr3"),
				pgcq.IN("id", "111", "222"),
			),
		),
	)
}
