# PGC v2 Specification

## Quick Start

PGC is being used in production by Clique for almost one year. It is a
product that we are actively supporting and enhancing

PGC is a *specific* postgres library designed to make using structs with
postgres tables very easy. It is designed for heavy use of jsonb.

Although technically an ORM, our aim is to keep this library light and simple.
A main concern is supporting distributed, high-traffic systems. This means
avoiding things like nasty, nested queries that some ORMs do.

## Using Code Generation To Get Started

You can use the following example script to generate a simple service
for use with PGC. You should put this code into a main.go and run it
using `go run main.go`

```golang
package main

import (
    "fmt"
    "github.com/cliqueinc/pgc"
)

type UserProfile struct {
    FirstName string
    LastName string
    Tags []string
}

func main() {
	fmt.Println(pgc.CreateS(&UserProfile{}))
	// The second param is "short name" for use in object methods
	fmt.Println(pgc.GenerateModel(&UserProfile{}, "uprof"))
	fmt.Println(pgc.GenerateModelTest(&UserProfile{}, "uprof"))
}

```

Normally when you are ready to create a new data model, you can first
design and create your struct, then pop it into an example file like this
then build out the service or supporting code based on the generated code.

* Note you will need to copy the struct into the example service file.

## Naming

All sql queries usually have 2 methods: first starts with `Must` (MustInsert) and the second just the naming of a method (Insert), where `MustInsert` panics in case of an error, and `Insert` returns an error. First advantage is readability, it's a common naming style in go (like `MustExec` in template lib), and newcomer usually awares that `MustInsert` may panic, while `Insert` returns just an error.

pgx.CmdTag, which tells additional info about sql result, is usually handled inside of a method. For instance, for the case when row not found for a Get method, 2 variables returned:

```golang
found, err := pgc.Get(&user)
if err != nil {
  panic(err)
}
if !found { // user not exist
  pgc.MustInsert(&user)
}
```

Sometimes helpers may be used to fetch additional info in case of an error:

```golang
err = pgc.Insert(f2)
if pgc.IsUniqueViolationError(err) {
  return errors.New("user with the same email already exists") // supposing email is unique for each row
}
```

## Struct Tags

- `pgc`

  - `pgc:"pk"` detects whether a field is a primary key. If not such tag set, pgc will set `ID` field as primary.

  - `pgc:"-"` tells pgc to skip this field from all pg operations.

  <strong>Gotchas:</strong>
    - It is strongly encouraged for your models to have ONLY one of the following (to define the primary key):
        - A `ID string` field OR
        - A ```UserID string `pgc:"pk"` ``` field where UserID can be whatever
        - Having both or none will cause an error
    - Custom time.Time fields are not supported. Your models must use time.Time directly.
        - If you custom times for json output see [this](http://choly.ca/post/go-json-marshalling/)
    - PGType will be quite limited initially. Basic ints, floats, timestamp sans tz (always utc),
    only text (no need to use varchar with modern pg), lots of jsonb. We will use the most
    correct data type [names](https://www.postgresql.org/docs/9.5/static/datatype.html):

- `pgc_name`
By default pgc converts struct name (usually CamelCased) into underscored name. But sometimes we have such field names like
`RedirectURL`, which may be converted in not a proper way, so all one needs to do is to add `pgc_name` tag:

  ```golang
  type blog struct {
    ID string
    RedirectURL string `pgc_name:"redirect_url"`
  }
  ```

## Adapter

pgc methods may be called directly from pgc (like `pgc.MustInsert(&user)`), or from Adapter, which can be created by function:

```golang
a := pgc.NewAdapter()
a.MustInsert(&u)
```

pgc global methods just use default adapter instance.

## Insert

There are 2 methods: Insert(structPtrs ...interface{}) and MustInsert(structPtrs ...interface{}). Multiple items from the same struct may be inserted
per one sql query. Attempt to call insert without models or more than `1000` items, or insert models that are different structs will cause an error.

```golang
u1 := &User{
  ID:   common.RandomString(30),
  Name: "John",
}
u2 := &User{
  ID:   common.RandomString(30),
  Name: "Forest",
}

pgc.MustInsert(u1, u2)
```

## Select

The idea is that we usually use the same patterns for building raw queries, such as limit, ordering, IN construction, where, etc. The purpose of method is to simplify quering, which can make using pgc more fun.

For example, in order to query by IN with limit 5 ordering by `created` column, one'd need to type:
```golang
err := pgc.Select(
  &users,
  pgc.IN("id", 111, 222, 333),
  pgc.Limit(5),
  pgc.Order("created", pgcq.DESC),
)
```

Or in case of more complicated conditions:
```golang
pgc.MustSelect(
  &blogs,
  pgcq.OR(
    pgcq.Equal("name", "blog4"),
    pgcq.AND(
      pgcq.Equal("descr", "descr3"),
      pgcq.IN("id", blog1ID, blog2ID),
    ),
  ),
)
```

The method is implemented using functional options pattern, which is super lightweight and is easy extendable for adding common constructions.

One other pros is that one doesn't need to keep in mind field ordering (like $1, $2 etc), method deals with it by itself, which allows dynamic adding of options.

Example:
```golang
opts := []pgcq.Option{
  pgcq.Order("id", pgcq.ASC),
  pgcq.Limit(50),
  pgcq.Offset(100),
}

if excludeEmail {
  opts = append(opts, pgcq.NotEqual("email", "some@email"))
}

pgc.MustSelect(&users, opts...)
```

### <strong>Default limit</strong>
If no limit specified for select, the default limit will be added (`1000`). If you <strong>really need</strong> to fetch all rows, you need to
add pgcq.All() option:

```golang
pgc.MustSelect(&blogs, pgcq.Order("updated", pgcq.DESC), pgcq.All())
```

## Select specific columns

In case one needs to fetch only custom columns (foe example table have a column html_content, which is too expensive to load each time), they can simply use `pgcq.Columns` query option:
```golang
var users []User
pgc.MustSelect(&users, pgcq.Columns("id", "name", "salary"), pgcq.Limit(2), pgcq.GreaterThan("salary", 500))

for _, u := range users {
  fmt.Printf("user (%s) has salary (%d) (not secret anymore!)\n", u.Name, u.Salary))
}
```

## Get

Get is almost the same as select except it returns exactly 1 row and returns flag whether row exists and an error if some has occured.

```golang
user := &user{ID: "111"}
found, err := pgc.Get(&user) // by default gets by primary key
if err != nil {
  return fmt.Error("db error: %v", err)
}
if !found {
  return errors.New("user not found")
}

user2 := &user{}
found := pgc.MustGet(&user2, pgcq.Equal("email", "user2@mail.com")) // this one will fetch by email
if !found {
  return errors.New("user not found by email")
}

user3 := &user{ID: "333"}
// if only few fields needed form db
found := pgc.MustGet(&user3, pgcq.Columns("company_id")) // by default gets by primary key
if !found {
  return errors.New("user not found")
}
fmt.Println(user3.CompanyID)
```

## Update

Update updates struct by primary key

```golang
if err := pgc.Update(&user); err != nil {
  return fmt.Errorf("fail update user: %v", err)
}
```

## UpdateRows

It is also posible to update multiple rows at once:

```golang
num, err := pgc.UpdateRows(&user{}, pgc.Map{"scores": 50, "is_active": false}, pgcq.Equal("company_id", "555"), pgcq.NotEqual("is_active", false))
if err != nil {
  return fmt.Errorf("db error: %v", err)
}
```

- `&user{}`, first argument, is a struct that represents table for update, pgc greps metadata from it.
- `pgc.Map` is an alias for `map[string]interface{}`, represents updated data
- `pgcq.Equal("company_id", "555")`, `pgcq.NotEqual("is_active", false)` - optional query options for updating rows.

The sample above will produce something like:
```sql
UPDATE "user" SET "scores"=50, "is_active"=fase WHERE "company_id"="555" AND "is_active" != false;
```

### <b>Update all rows</b>

By default, if you try to call `pgc.UpdateRows` without any query option, it will produce an error: `query options cannot be empty`

In case when you <b>really need</b> to update all rows (e.g. migration script), you need to pass `pgcq.All()` option.
It is done to avoid unintentional update of all rows:

```golang
num, err := pgc.UpdateRows(&user{}, pgc.Map{"is_active": false}, pgcq.All())
if err != nil {
  return err
}
fmt.Println(num)
```

## Delete

Delete deletes struct by primary key

```golang
if err := pgc.Delete(&user); err != nil {
  return fmt.Errorf("fail delete user: %v", err)
}
```

## DeleteRows

It is also posible to delete multiple rows at once:

```golang
num, err := pgc.DeleteRows(&user{}, pgcq.Equal("company_id", "555"), pgcq.NotEqual("is_active", false))
if err != nil {
  return fmt.Errorf("db error: %v", err)
}
```

- `&user{}`, first argument, is a struct that represents table, pgc greps metadata from it.
- `pgcq.Equal("company_id", "555")`,  `pgcq.NotEqual("is_active", false)` - optional query options for deleting rows.

The sample above will produce something like:
```sql
DELETE FROM "user" WHERE "company_id"="555";
```

### <b>Delete all rows</b>

By default, if you try to call `pgc.DeleteRows` without any query option, it will produce an error: `query options cannot be empty`

In case when you <b>really need</b> to delete all rows (e.g. migration script), you need to pass `pgcq.All()` option.
It is done to avoid unintentional deleting of all rows:

```golang
num, err := pgc.DeleteRows(&user{}, pgcq.All())
if err != nil {
  return err
}
fmt.Println(num)
```

## Count

In order to get count of all rows by query, just call something like a sample below:

```golang
count := pgc.MustCount(&user{}, pgcq.LessThan("score", 1000))
fmt.Printf("found %d rows\n", count)
```

## Advanced

## SelectCustomData

In case one needs to perform some aggregation query or a query that returns columns not the same as columns in table, there is
`SelectCustomData` method.

Assuming we have a struct `order`:

```golang
type Order struct {
  ID           string
  UserID       string
  Price        int
  Created time.Time
}
```

And we want to get total amount of money each user spent in our shop, but less than, say, `1000`.
So we want to get something like `["user_id", "total_price"]`

For such specific query one needs to create struct that can represent query result:

```golang
type aggregated struct {
  UserID       string
  TotalPrice   int `pgc_name:"SUM(price) as total_price"`
}
```

With `pgc_name` tag you can specify aggregation if needed, like in a sample above, and than call the method:

```golang
var aggregatedRows []aggregated

pgc.MustSelectCustomData(
	&order{},
	&aggregatedRows,
	pgcq.GroupBy("user_id"),
	pgcq.Order("total_price", pgcq.DESC),
	pgcq.LessThan("SUM(price)", 1000),
)

for _, row := range aggregatedRows {
  fmt.Printf("user %s has bought products with total cost (%d)\n", row.UserID, row.TotalPrice)
}
```

Arguments:
- `&order{}` - model to fetch table metadata from
- `&aggregatedRows` - slice of rows with expected result
- `pgcq.GroupBy("user_id")`, `pgcq.Order("total_price", pgcq.DESC)`,	`pgcq.LessThan("SUM(price)", 1000)` - optional query options.

This call will product query like:
```sql
SELECT "user_id", SUM(price) as "total_price" from "user" GROUP BY "user_id" HAVING SUM(price) < 1000 ORDER BY "total_price" DESC;
```

## Installation

This assumes you are using govendor. See the cyclops readme for more info on govendor.

- govendor add github.com/cliqueinc/cws-mono/pgc
- govendor add github.com/cliqueinc/cws-mono/pgc/pgccmd
- cd vendor/github.com/cliqueinc/cws-mono/pgc/pgccmd && go install -v

At this point you will have pgc installed and should be able to run the command pgccmd. If you can't,
make sure you have $GOPATH/bin in your path.

## Generate sum Codez!

The easiest way to understand how to use the code generator is to view examples/generate_print.go

Then run it via `go run examples/generate_print.go`

It will print out the sql create table statement and a stub model and test.

You can use the following two step process to create everything from scratch:

Run the command `pgccmd gen init StructName shortStructName` where StructName is something like "User" and
shortStructName could be "user" and is the lowercase self/this reference for class methods
(will make more sense in a second).

`pgccmd gen init User user`

This will print a main go program. Place this code in a go file called main.go. You should then spend some
time and fill out the struct and look over this code. Once the struct looks good, run:

`go run main.go`

This will print a create table statement, sample struct methods, and a simple test stub. The create table should
go in a new up.sql in a new schema migration folder (see instructions below). From the user example, you would
put the struct and methods in a model_user.go file and the test in a model_user_test.go file.

Delete your main.go file now and start building out your model/service!


## Versioning (schema migrations)

In order to use the schema migration capabilities, you need to follow next steps:

### 1. Install pgccmd tool

```bash
go install github.com/cliqueinc/cws-mono/pgc.v2/pgccmd
```
So now you can operate migrations.

### 2. Init migration
pgccmd inits postgres connection from default env vars, and migration path also taken from `POSTGRES_MIGRATION_PATH` variable.

In order to init migration:
```bash
pgccmd migration
```

This command creates 2 migration files (like `2017-09-19:15:08:52.sql` and `2017-09-19:15:08:52_down.sql`), up and rollback sql commands, the second one
is optional and can be safely deleted.

If you are running migration by your app, you need to register the migration path before pgc init:
```golang
import (
	"github.com/cliqueinc/pgc"
)

pgc.RegisterMigrationPath(cfg.AppPath+"deployments/mono-files/pg-revisions")
```

So the migration handler knows where to take migration from.
Now pgc nows where to take migrations from, and you are able to call pgc.InitSchema(), or pgc.UpdateSchema():
```
if err := pgc.InitSchema(); err != nil {
  panic(fmt.Sprintf("Fail init schema: %v\n", err))
}
```

### <strong>pgc commands</strong>

- #### pgccmd init

  Creates required pgc schema tables.
  `pgc init` also ensures all executed migrations exist in corresponding files.

- #### pgccmd up

  Checks is there any migration that needs to be executed, and executes them in ascending order.

- #### pgccmd migration [default]

  Generates new migration file, see `Migration file internals` for more info. If the next argument is `default`, the migration 0000-00-00:00:00:00.sql
  will be generated. It won't be added to migration log and won't be executed unless you explicitly call `exec default`. It is made in order to have
  an ability to keep existing schema, which needs to be executed only once, and most likely, only in local environment.

- #### pgccmd status

  Prints latest migration logs and most recent migrations.

- #### pgccmd exec [version]

  Executes specific migration and markes it as applied if needed. Type `pgccmd exec default`, if you want to execute the default migration.

- #### pgccmd rollback [version]

  Rollbacks specific version. If no version specified, just rollbacks the latest one.

- #### pgccmd gen

  Run the command pgccmd gen init StructName shortStructName where StructName is something like "User" and shortStructName could be "user" and is the lowercase self/this reference for class methods (will make more sense in a second).

  pgccmd gen init User user

  This will print a main go program. Place this code in a go file called main.go. You should then spend some time and fill out the struct and look over this code. Once the struct looks good, run:

  go run main.go

  This will print a create table statement, sample struct methods, and a simple test stub. The create table should go in a new up.sql in a new schema migration folder (see instructions below). From the user example, you would put the struct and methods in a model_user.go file and the test in a model_user_test.go file.

  Delete your main.go file now and start building out your model/service!


## Testing Notes

- You can simply run `go test -v ./...` inside this directory to run all the tests.
- See conn_test.go for the main test function and details
- Due to the way we configure the tmp/test db, running an individual test should be accomplished
  via `go test -v -run=TestName`
- Given the heavy use of structs, please have each test define its
  own structs inside the test function. Otherwise maintenance will become
  a real nightmare.

## Understanding Reflection

In order to improve this library a fairly in depth knowledge of golang's reflect
package is required. I would start with the following resources below, then try adding
a few tests around the parsing and Select code.

Resources (in order of importance)
- [Laws of Reflection by Rob Pike](https://blog.golang.org/laws-of-reflection)
    - Links to other posts
- Go book chapter ?
