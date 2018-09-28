package pgc_test

import (
	"strings"
	"testing"
	"time"

	"github.com/cliqueinc/pgc/util"
	pgc "github.com/cliqueinc/pgc"
	"github.com/cliqueinc/pgc/pgcq"
)

// Please see testing guidelines in the readme

func TestFullCRUD(t *testing.T) {
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

	type fakeUser struct {
		ID        string
		FirstName string
		LastName  string
		Emails    []string
		Addresses []fakeAddress
		Profile   fakeProfile
		Meta      map[string]interface{}
		Created   time.Time
	}

	fu := &fakeUser{
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
	pgc.MustCreateTable(fu)
	pgc.MustInsert(fu)

	fu.FirstName = util.RandomString(30)
	pgc.MustUpdate(fu)

	fu2 := &fakeUser{ID: fu.ID}
	pgc.MustGet(fu2)

	if fu2.FirstName != fu.FirstName {
		t.Errorf("FirstName wasnt changed after update, expected (%s) was (%s)",
			fu2.FirstName, fu.FirstName)
	}
}

type selectTest struct {
	ID      string
	Name    string
	Color   string
	Height  int
	Created time.Time
}

func TestSelect(t *testing.T) {
	s := &selectTest{}
	pgc.MustCreateTable(s)

	s.ID = util.RandomString(10)
	s.Name = util.RandomString(10)
	s.Color = util.RandomString(10)
	s.Height = util.RandomInt(1, 5000000)
	s.Created, _ = time.Parse("2006-01-02", "2006-01-02")
	pgc.MustInsert(s)

	var sl []selectTest
	pgc.MustSelect(&sl)
	if len(sl) != 1 {
		t.Errorf("expected %d items, %d received", 1, len(sl))
	}

	defer func() {
		if err := recover(); err != nil {
			t.Errorf("SelectAllWhere failed: %v", err)
		}
	}()
	var res []selectTest
	pgc.SelectAllWhere(&res, "where id = $1", s.ID)
	if len(res) != 1 {
		t.Fatalf("slice len expected to be 1, actual slice: %v", res)
	}
}

func TestMustSelectPanicNoSlicePtr(t *testing.T) {
	// Panics because you should be passing pointer to slice not slice
	var sl []selectTest
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("TestSelectAllWherePanicNoSlicePtr should have panicked")
			}
		}()
		pgc.MustSelect(sl)
	}()
}

func TestSelectColumns(t *testing.T) {
	s := &selectTest{}

	s.ID = util.RandomString(10)
	s.Name = util.RandomString(10)
	s.Color = util.RandomString(10)
	s.Height = util.RandomInt(1, 5000000)
	s.Created, _ = time.Parse("2006-01-02", "2006-01-02")
	pgc.MustInsert(s)

	t.Run("unknown column", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("panic expected in case some column isn't recognized")
			}
		}()

		var sl []selectTest
		err := pgc.Select(&sl, pgcq.Columns("some column", "id"), pgcq.Limit(2))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
	t.Run("custom columns", func(t *testing.T) {
		var sl []selectTest
		err := pgc.Select(&sl, pgcq.Columns("id", "name"), pgcq.Limit(2))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(sl) == 0 {
			t.Fatalf("empty result, at least 1 item expected")
		}
		el := sl[0]
		if el.ID == "" || el.Name == "" {
			t.Errorf("specified columns (id,name) are empty")
		}
		if el.Color != "" || el.Height != 0 {
			t.Errorf("all columns except (id,name) should be empty, actual result: %+v", el)
		}
	})
}

func TestGet(t *testing.T) {
	type fakeGet struct {
		ID     string
		UserID string
		Name   string
	}
	f1 := &fakeGet{ID: util.RandomString(25), UserID: util.RandomString(25), Name: "BoB"}
	f2 := &fakeGet{ID: util.RandomString(25), UserID: util.RandomString(25), Name: "John"}
	pgc.MustCreateTable(f1)
	pgc.MustInsert(f1, f2)

	f1Get := &fakeGet{ID: f1.ID}
	found := pgc.MustGet(f1Get)
	if !found {
		t.Fatalf("struct not found")
	}
	if f1Get.Name != f1.Name {
		t.Fatalf("Get expected to return user (%s), actual: (%s)", f1.Name, f1Get.Name)
	}

	f2Get := &fakeGet{}
	found = pgc.MustGet(f2Get, pgcq.Columns("name"), pgcq.Equal("user_id", f2.UserID))
	if !found {
		t.Fatalf("struct not found")
	}
	if f2Get.Name != f2.Name {
		t.Fatalf("Get expected to return user (%s), actual: (%s)", f2.Name, f2Get.Name)
	}
	if f2Get.UserID != "" {
		t.Fatalf("pgc fetched not only name column!")
	}

	f3Get := &fakeGet{}
	found = pgc.MustGet(f3Get, pgcq.Equal("user_id", "unknown"))
	if found {
		t.Fatalf("unexpected user found: (%s)", f3Get.Name)
	}
}

func TestMustInsert(t *testing.T) {
	type fakeInsert1 struct {
		ID   string
		Name string
	}
	f1 := &fakeInsert1{ID: util.RandomString(25), Name: "BoB"}
	pgc.MustCreateTable(f1)
	pgc.MustInsert(f1)
	f1Get := &fakeInsert1{ID: f1.ID}
	pgc.MustGet(f1Get)
	if f1Get.Name != f1.Name {
		t.Fatalf("TestInsertWPanic couldnt get back struct (%#v)", f1) // Dies
	}
	f2 := &fakeInsert1{ID: f1.ID, Name: "JiM"}

	func() { // Define a function then call it to fully encapsulate the error process
		defer func() {
			if err := recover(); err == nil {
				t.Errorf("TestInsertWPanic should have panic on duped struct (%#v)", f1)
			}
		}()
		// Should panic here
		pgc.MustInsert(f2)
	}()
}

func TestInsert(t *testing.T) {
	type fakeInsert2 struct {
		ID   string
		Name string
	}
	f1 := &fakeInsert2{ID: util.RandomString(25), Name: "BoBErr"}
	pgc.MustCreateTable(f1)
	err := pgc.Insert(f1)
	if err != nil {
		t.Fatalf("InsertErr's error on (%#v) should have been nil", f1)
	}
	f1Get := &fakeInsert2{ID: f1.ID}
	pgc.MustGet(f1Get)
	if f1Get.ID != f1.ID || f1Get.Name != f1.Name {
		t.Fatalf("TestInsertWErr couldnt get back struct (%#v)", f1) // Dies
	}
	f2 := &fakeInsert2{ID: f1.ID, Name: "JiMErr"}

	err = pgc.Insert(f2)
	if err == nil {
		t.Fatalf("InsertErr's error on (%#v) should NOT have been nil", f2)
	}

	// Let's also check the string code and message
	err = pgc.Insert(f2)
	// Code: (string) (len=5) "23505",
	// Message: (string) (len=66) "duplicate key value violates unique constraint \"fake_insert2_pkey\"",
	if !pgc.IsUniqueViolationError(err) {
		t.Errorf("TestInsertWErr InsertErrS expected code for unique violation (%s) was (%s)",
			pgc.PGECUniqueViolation, err)
	}
	if !strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
		t.Errorf("TestInsertWErr InsertErrS expected message for unique violation was (%s), actual: (%v)",
			pgc.PGECUniqueViolation, err)
	}

	t.Run("insert more that limit allows", func(t *testing.T) {
		items := make([]interface{}, 0, pgc.LimitInsert+1)
		for i := 0; i < pgc.LimitInsert+1; i++ {
			items = append(items, &fakeInsert2{ID: util.RandomString(5)})
		}

		if err := pgc.Insert(items...); err == nil {
			t.Fatalf("shouldn't allow insertion more than (%d) items", pgc.LimitInsert)
		}
	})
}

func TestUpdate(t *testing.T) {
	type fakeUpdate struct {
		ID        string
		CompanyID string
		Name      string
		Scores    int
		IsActive  bool
	}
	companyID1 := util.RandomString(20)
	companyID2 := util.RandomString(23)

	f1 := &fakeUpdate{ID: util.RandomString(25), CompanyID: companyID1, Name: "Bob", IsActive: true}
	f2 := &fakeUpdate{ID: util.RandomString(25), CompanyID: companyID2, Name: "John", IsActive: false}
	f3 := &fakeUpdate{ID: util.RandomString(25), CompanyID: companyID2, Name: "James", IsActive: true}
	pgc.MustCreateTable(f1)
	pgc.MustInsert(f1, f2, f3)

	t.Run("update by PK", func(t *testing.T) {
		f1.Scores = 200
		err := pgc.Update(f1)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
	t.Run("update rows by column", func(t *testing.T) {
		num, err := pgc.UpdateRows(&fakeUpdate{}, pgc.Map{"scores": 50, "is_active": false}, pgcq.Equal("company_id", companyID2))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if num != 2 {
			t.Fatalf("2 items should be updated, actual num items: %d", num)
		}
		pgc.MustGet(f2)
		if f2.Scores != 50 {
			t.Errorf("f2 wasn't updated")
		}
		pgc.MustGet(f3)
		if f3.Scores != 50 {
			t.Errorf("f3 scores value wasn't updated")
		}
		if f3.IsActive {
			t.Errorf("f3 is_active value wasn't updated")
		}
		pgc.MustGet(f1)
		if f1.Scores == 50 {
			t.Errorf("f1 shouldn't be updated")
		}
	})
	t.Run("update rows no query", func(t *testing.T) {
		num, err := pgc.UpdateRows(&fakeUpdate{}, pgc.Map{"is_active": false})
		if err == nil {
			t.Fatalf("error expected if no options specified, rows affected: %d", num)
		}
	})
	t.Run("force update all rows", func(t *testing.T) {
		num, err := pgc.UpdateRows(&fakeUpdate{}, pgc.Map{"is_active": false}, pgcq.All())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if num != 3 {
			t.Fatalf("not all rows were updated, actual rows num: (%d)", num)
		}
		pgc.MustGet(f1)
		if f1.IsActive {
			t.Errorf("f1 wasn't updated")
		}
	})
}

func TestDelete(t *testing.T) {
	type fakeDelete struct {
		ID        string
		CompanyID string
		Name      string
		Scores    int
		IsActive  bool
	}
	companyID1 := util.RandomString(20)
	companyID2 := util.RandomString(23)

	f1 := &fakeDelete{ID: util.RandomString(25), CompanyID: companyID1, Name: "Bob", IsActive: true}
	f2 := &fakeDelete{ID: util.RandomString(25), CompanyID: companyID2, Name: "John", IsActive: false}
	f3 := &fakeDelete{ID: util.RandomString(25), CompanyID: companyID2, Name: "James", IsActive: true}
	f4 := &fakeDelete{ID: util.RandomString(25), CompanyID: companyID2, Name: "James2", IsActive: true}
	f5 := &fakeDelete{ID: util.RandomString(25), CompanyID: util.RandomString(23), Name: "Forest", IsActive: false}
	pgc.MustCreateTable(f1)
	pgc.MustInsert(f1, f2, f3, f4, f5)

	t.Run("delete by PK", func(t *testing.T) {
		f1.Scores = 200
		err := pgc.Delete(f4)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
	t.Run("delete rows", func(t *testing.T) {
		num, err := pgc.DeleteRows(&fakeDelete{}, pgcq.Equal("company_id", companyID2))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if num != 2 {
			t.Fatalf("2 items should be updated, actual num items: %d", num)
		}
		if found := pgc.MustGet(f2); found {
			t.Fatalf("f2 wasn't deleted")
		}
		if found := pgc.MustGet(f3); found {
			t.Fatalf("f3 wasn't deleted")
		}
	})
	t.Run("delete rows no query", func(t *testing.T) {
		num, err := pgc.DeleteRows(&fakeDelete{})
		if err == nil {
			t.Fatalf("error expected if no options specified, rows affected: %d", num)
		}
	})
	t.Run("force delete all rows", func(t *testing.T) {
		num, err := pgc.DeleteRows(&fakeDelete{}, pgcq.All())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if num != 2 {
			t.Fatalf("not all rows were updated, actual rows num: (%d)", num)
		}
		if found := pgc.MustGet(f1); found {
			t.Errorf("f1 wasn't deleted")
		}
	})
}

func TestCount(t *testing.T) {
	type countUser struct {
		ID   string
		Name string
	}

	pgc.MustCreateTable(&countUser{})
	if count := pgc.MustCount(&countUser{}); count != 0 {
		t.Fatalf("no users inserted, received count: %d", count)
	}

	u1 := &countUser{
		ID:   util.RandomString(30),
		Name: util.RandomString(10),
	}
	u2 := &countUser{
		ID:   util.RandomString(30),
		Name: util.RandomString(10),
	}

	pgc.MustInsert(u1, u2)

	if count := pgc.MustCount(&countUser{}); count != 2 {
		t.Fatalf("%d users should have been inserted, received count: %d", 2, count)
	}

	if count := pgc.MustCount(&countUser{}, pgcq.Equal("id", u1.ID)); count != 1 {
		t.Fatalf("%d user expected, received count: %d", 1, count)
	}
}

func TestSelectWithOpts(t *testing.T) {
	type fakeBlog struct {
		Name         string
		Descr        string
		ID           string
		PublishStart time.Time
	}

	blogs := []fakeBlog{
		{
			Name:         "blog1",
			Descr:        "descr1",
			ID:           util.RandomString(30),
			PublishStart: time.Now(),
		},
		{
			Name:         "blog2",
			Descr:        "descr2",
			ID:           util.RandomString(30),
			PublishStart: time.Now(),
		},
		{
			Name:         "blog3",
			Descr:        "descr3",
			ID:           util.RandomString(30),
			PublishStart: time.Now(),
		},
		{
			Name:         "blog4",
			Descr:        "descr3",
			ID:           util.RandomString(30),
			PublishStart: time.Now(),
		},
	}

	pgc.MustCreateTable(&fakeBlog{})
	for _, b := range blogs {
		pgc.MustInsert(&b)
	}

	t.Run("select by ID", func(t *testing.T) {
		var fetchedBlogs []fakeBlog
		pgc.Select(
			&fetchedBlogs,
			pgcq.Equal("id", blogs[0].ID),
		)
		if len(fetchedBlogs) == 0 {
			t.Fatal("no items found in select")
		}
		if len(fetchedBlogs) != 1 || fetchedBlogs[0].Name != blogs[0].Name {
			t.Errorf("select failed. Expected blog: (%v), actual: (%v)", blogs[0], fetchedBlogs[0])
		}
	})
	t.Run("select IN", func(t *testing.T) {
		var fetchedBlogs []fakeBlog
		pgc.Select(
			&fetchedBlogs,
			pgcq.IN("id", blogs[0].ID, blogs[1].ID, blogs[2].ID),
			pgcq.Limit(2),
		)
		if len(fetchedBlogs) != 2 {
			t.Fatalf("expected %d items, %d given", 2, len(fetchedBlogs))
		}
		if fetchedBlogs[0].Name != blogs[0].Name || fetchedBlogs[1].Name != blogs[1].Name {
			t.Errorf("select failed. Expected blogs: (%v), actual: (%v)", blogs[:2], fetchedBlogs)
		}
	})
	t.Run("limit offset", func(t *testing.T) {
		var fetchedBlogs []fakeBlog
		pgc.Select(
			&fetchedBlogs,
			pgcq.IN("id", blogs[0].ID, blogs[1].ID, blogs[2].ID),
			pgcq.Limit(2),
			pgcq.Offset(1),
		)
		if len(fetchedBlogs) != 2 {
			t.Fatalf("expected %d items, %d given", 2, len(fetchedBlogs))
		}
		if fetchedBlogs[0].Name != blogs[1].Name || fetchedBlogs[1].Name != blogs[2].Name {
			t.Errorf("select failed. Expected blogs: (%v), actual: (%v)", blogs[1:3], fetchedBlogs)
		}
	})
	t.Run("nested conditions", func(t *testing.T) {
		var fetchedBlogs []fakeBlog
		pgc.Select(
			&fetchedBlogs,
			pgcq.OR(
				pgcq.Equal("name", "blog4"),
				pgcq.AND(
					pgcq.Equal("descr", "descr3"),
					pgcq.IN("id", blogs[1].ID, blogs[2].ID),
				),
			),
		)
		if len(fetchedBlogs) != 2 {
			t.Fatalf("expected %d items, %d given", 2, len(fetchedBlogs))
		}
		if fetchedBlogs[0].Name != blogs[2].Name || fetchedBlogs[1].Name != blogs[3].Name {
			t.Errorf("select failed. Expected blogs: (%v), actual: (%v)", blogs[2:], fetchedBlogs)
		}
	})
	t.Run("custom orders", func(t *testing.T) {
		var fetchedBlogs []fakeBlog
		err := pgc.Select(
			&fetchedBlogs,
			pgcq.Order("descr", pgcq.DESC),
			pgcq.Order("name", pgcq.ASC),
			pgcq.Limit(3),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fetchedBlogs) != 3 {
			t.Fatalf("expected %d items, %d given", 3, len(fetchedBlogs))
		}
		if fetchedBlogs[0].ID != blogs[2].ID || fetchedBlogs[1].ID != blogs[3].ID || fetchedBlogs[2].ID != blogs[1].ID {
			t.Errorf("select failed. Expected blogs: (%v), actual: (%v)", []fakeBlog{blogs[2], blogs[3], blogs[1]}, fetchedBlogs)
		}
	})
	t.Run("like", func(t *testing.T) {
		b1 := &fakeBlog{
			Name:         "aa_name1",
			Descr:        "descr1",
			ID:           util.RandomString(30),
			PublishStart: time.Now(),
		}
		b2 := &fakeBlog{
			Name:         "aa_name2",
			Descr:        "descr1",
			ID:           util.RandomString(30),
			PublishStart: time.Now(),
		}
		b3 := &fakeBlog{
			Name:         "bb_name",
			Descr:        "descr1",
			ID:           util.RandomString(30),
			PublishStart: time.Now(),
		}
		pgc.Insert(b1, b2, b3)

		var fetchedBlogs []fakeBlog
		err := pgc.Select(
			&fetchedBlogs,
			pgcq.Like("name", "aa_%"),
			pgcq.Order("name", pgcq.ASC),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fetchedBlogs) != 2 {
			t.Fatalf("expected %d items, %d given", 2, len(fetchedBlogs))
		}
		if fetchedBlogs[0].ID != b1.ID || fetchedBlogs[1].ID != b2.ID {
			t.Errorf("select failed. Expected blogs: (%v), actual: (%v)", []fakeBlog{*b1, *b2}, fetchedBlogs)
		}
	})
	t.Run("raw query", func(t *testing.T) {
		t.Run("raw select", func(t *testing.T) {
			var fetchedBlogs []fakeBlog
			pgc.Select(
				&fetchedBlogs,
				pgcq.OR(
					pgcq.IN("id", blogs[0].ID, blogs[1].ID),
					pgcq.Raw("name = ?", "blog4"),
				),
			)
			if len(fetchedBlogs) != 3 {
				t.Fatalf("expected %d items, %d given", 3, len(fetchedBlogs))
			}
			if fetchedBlogs[0].Name != blogs[0].Name || fetchedBlogs[1].Name != blogs[1].Name {
				t.Errorf("select failed. Expected blogs: (%v), actual: (%v)", blogs[:2], fetchedBlogs)
			}
		})
		t.Run("only raw query", func(t *testing.T) {
			type JobTest struct {
				ID           string
				Name         string
				Frequency    int
				LastExecuted time.Time
			}
			j1 := &JobTest{
				ID:           util.NewGuid(),
				Name:         "name-111",
				Frequency:    1,
				LastExecuted: time.Now().Add(-2 * time.Minute).UTC(),
			}
			j2 := &JobTest{
				ID:           util.NewGuid(),
				Name:         "name-222",
				Frequency:    3,
				LastExecuted: time.Now().Add(-1 * time.Minute).UTC(),
			}
			j3 := &JobTest{
				ID:           util.NewGuid(),
				Name:         "name-333",
				Frequency:    2,
				LastExecuted: time.Now().UTC(),
			}
			j4 := &JobTest{
				ID:           util.NewGuid(),
				Name:         "name-444",
				Frequency:    1,
				LastExecuted: time.Now().Add(-5 * time.Minute).UTC(),
			}
			pgc.MustCreateTable(&JobTest{})

			err := pgc.Insert(j1, j2, j3, j4)
			if err != nil {
				t.Fatalf("fail insert jobs: %v", err)
			}

			rawQuery := "last_executed <= (NOW() AT TIME ZONE 'UTC') - (? * INTERVAL '1 minute'*frequency)"
			var items []JobTest
			err = pgc.Select(
				&items,
				pgcq.Raw(rawQuery, 1),
				pgcq.Order("name", pgcq.ASC),
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(items) != 2 {
				t.Fatalf("expected items num: %d, got %d", 2, len(items))
			}
			if items[0].ID != j1.ID {
				t.Fatalf("expected item #1 ID: %s, actual item: %v", j1.ID, items[0])
			}
			if items[1].ID != j4.ID {
				t.Fatalf("expected item #2 ID: %s, actual item: %v", j4.ID, items[1])
			}
		})
	})
	t.Run("ensure pointer data is correct", func(t *testing.T) {
		type nestedData struct {
			Name, Title string
		}

		type pointerData struct {
			ID     string
			Name   string
			Map    map[string]string
			Slice  []int
			Nested nestedData
		}
		pgc.MustCreateTable(&pointerData{})

		i1 := &pointerData{
			ID:     "111",
			Name:   "name 1",
			Map:    map[string]string{"1": "1"},
			Slice:  []int{1, 2, 3},
			Nested: nestedData{"11", "11"},
		}
		i2 := &pointerData{
			ID:     "222",
			Name:   "name 2",
			Map:    map[string]string{"1": "2"},
			Slice:  []int{4, 5, 6},
			Nested: nestedData{"22", "22"},
		}
		pgc.MustInsert(i1, i2)

		var res []pointerData
		err := pgc.Select(&res)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res) != 2 {
			t.Fatalf("expected %d items, %d given", 2, len(res))
		}
		res1, res2 := res[0], res[1]
		if res1.ID != i1.ID || res1.Name != i1.Name || res1.Map == nil || res1.Map["1"] != i1.Map["1"] || len(res1.Slice) == 0 || res1.Slice[0] != i1.Slice[0] {
			t.Errorf("result data is not the same as inserted: expected (%v), actual (%v)", i1, res1)
		}
		if res1.Nested.Name != i1.Nested.Name {
			t.Errorf("result nested data is not the same as inserted: expected (%v), actual (%v)", i1, res1)
		}
		if res2.ID != i2.ID || res2.Name != i2.Name || res2.Map == nil || res2.Map["1"] != i2.Map["1"] || len(res2.Slice) == 0 || res2.Slice[0] != i2.Slice[0] {
			t.Errorf("result data is not the same as inserted: expected (%v), actual (%v)", i2, res2)
		}
		if res2.Nested.Name != i2.Nested.Name {
			t.Errorf("result nested data is not the same as inserted: expected (%v), actual (%v)", i2, res2)
		}
	})
}

func TestSelectCustomData(t *testing.T) {
	type rowData struct {
		UserID       string
		Price        int
		ID           string
		PublishStart time.Time
	}

	rows := []rowData{
		{
			UserID:       "user1",
			ID:           util.RandomString(30),
			Price:        100,
			PublishStart: time.Now(),
		},
		{
			UserID:       "user1",
			ID:           util.RandomString(30),
			Price:        200,
			PublishStart: time.Now(),
		},
		{
			UserID:       "user2",
			ID:           util.RandomString(30),
			Price:        150,
			PublishStart: time.Now(),
		},
		{
			UserID:       "user3",
			ID:           util.RandomString(30),
			Price:        10,
			PublishStart: time.Now(),
		},
		{
			UserID:       "user3",
			ID:           util.RandomString(30),
			Price:        700,
			PublishStart: time.Now(),
		},
		{
			UserID:       "user4",
			ID:           util.RandomString(30),
			Price:        8000,
			PublishStart: time.Now(),
		},
	}

	type customData struct {
		UserID     string
		TotalPrice int `pgc_name:"SUM(price) as total_price"`
	}

	pgc.MustCreateTable(&rowData{})
	for _, r := range rows {
		pgc.MustInsert(&r)
	}

	var fetchedRows []customData
	pgc.SelectCustomData(
		&rowData{},
		&fetchedRows,
		pgcq.GroupBy("user_id"),
		pgcq.Order("total_price", pgcq.DESC),
		pgcq.Having(
			pgcq.LessThan("SUM(price)", 1000),
		),
	)
	if len(fetchedRows) != 3 {
		t.Fatalf("expected %d items, %d given", 3, len(fetchedRows))
	}
	if userID := fetchedRows[0].UserID; userID != "user3" {
		t.Fatalf("user3 expected to have the biggest price, actual user id: (%s)", userID)
	}
	expectedPrice := rows[3].Price + rows[4].Price
	if totalPrice := fetchedRows[0].TotalPrice; totalPrice != expectedPrice {
		t.Fatalf("aggregation failed: expected price (%d), actual price (%d)", expectedPrice, totalPrice)
	}
}

func TestJoin(t *testing.T) {
	type subscriptionJoin struct {
		ID     string
		UserID string
		URL    string
	}
	type orderJoin struct {
		ID     string
		UserID string
		Total  int
	}
	type socialJoin struct {
		UserID    string `pgc:"pk"`
		FBID      string `pgc_name:"fb_id"`
		TwitterID string
	}

	type userJoin struct {
		ID            string
		Name          string
		Subscriptions []subscriptionJoin `pgc:"join"`
		Orders        []orderJoin        `pgc:"join"`
		Social        *socialJoin        `pgc:"join"`
		PublishStart  time.Time
	}

	user1 := userJoin{ID: "u111", Name: "user1", PublishStart: time.Now()}
	user2 := userJoin{ID: "u222", Name: "user2", PublishStart: time.Now()}
	user3 := userJoin{ID: "u333", Name: "user3", PublishStart: time.Now()}

	sub1 := subscriptionJoin{ID: "s111", UserID: user1.ID, URL: "https://whowhatwhear.com/url1"}
	sub2 := subscriptionJoin{ID: "s222", UserID: user1.ID, URL: "https://whowhatwhear.com/article"}
	sub3 := subscriptionJoin{ID: "s333", UserID: user1.ID, URL: "url2"}
	sub4 := subscriptionJoin{ID: "s444", UserID: user3.ID, URL: "https://whowhatwhear.com/another-article"}

	o1 := &orderJoin{ID: "o111", UserID: user1.ID, Total: 111}
	o2 := &orderJoin{ID: "o222", UserID: user1.ID, Total: 500}
	o3 := &orderJoin{ID: "o333", UserID: user3.ID, Total: 200}
	o4 := &orderJoin{ID: "o444", UserID: user2.ID, Total: 700}

	s1 := &socialJoin{UserID: user1.ID, FBID: "1-1", TwitterID: "2-2"}
	s2 := &socialJoin{UserID: user2.ID, FBID: "2-2", TwitterID: "3-3"}

	userRows := []userJoin{user1, user2, user3}
	subRows := []subscriptionJoin{sub1, sub2, sub3, sub4}

	pgc.MustCreateTable(&userJoin{})
	pgc.MustCreateTable(&subscriptionJoin{})
	pgc.MustCreateTable(&orderJoin{})
	pgc.MustCreateTable(&socialJoin{})
	for _, r := range userRows {
		pgc.MustInsert(&r)
	}
	for _, r := range subRows {
		pgc.MustInsert(&r)
	}
	pgc.MustInsert(o1, o2, o3, o4)
	pgc.MustInsert(s1, s2)

	const userJoinSubscription = "user_join.id = subscription_join.user_id"
	const userJoinOrder = "user_join.id = order_join.user_id"
	const userJoinSocial = "user_join.id = social_join.user_id"

	t.Run("one join", func(t *testing.T) {
		var fetchedRows []userJoin
		err := pgc.Select(
			&fetchedRows,
			pgcq.Columns("id", "name"),
			pgcq.Join(&subscriptionJoin{}, userJoinSubscription, "url"),
			pgcq.Order("name", pgcq.ASC),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fetchedRows) != 3 {
			t.Fatalf("expected %d items, %d given", 3, len(fetchedRows))
		}
		if len(fetchedRows[0].Subscriptions) != 3 {
			t.Fatalf("row 1 expected to have 3 subscriptions: actual: (%v)", fetchedRows[0].Subscriptions)
		}
		if fetchedRows[0].Subscriptions[0].ID != "s111" {
			t.Errorf("1st subscription of 1st user expected to be (%v), actual: (%v)", sub1, fetchedRows[0].Subscriptions[0])
		}
		if len(fetchedRows[1].Subscriptions) != 0 {
			t.Errorf("row 1 expected to have 0 subscription: actual: (%v)", fetchedRows[1])
		}
		if len(fetchedRows[2].Subscriptions) != 1 {
			t.Errorf("row 1 expected to have 1 subscription: actual: (%v)", fetchedRows[2])
		}
	})
	t.Run("multiple join", func(t *testing.T) {
		var fetchedRows []userJoin
		err := pgc.Select(
			&fetchedRows,
			pgcq.Columns("id", "name"),
			pgcq.Join(&subscriptionJoin{}, userJoinSubscription, "url"),
			pgcq.Join(&orderJoin{}, userJoinOrder, "total"),
			pgcq.Like("url", "https://whowhatwhear.com/%"),
			pgcq.GreaterOrEqual("total", 500),
			pgcq.Order("name", pgcq.ASC),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fetchedRows) != 1 {
			t.Fatalf("expected %d items, %d given", 1, len(fetchedRows))
		}
		if len(fetchedRows[0].Subscriptions) != 2 {
			t.Fatalf("row 1 expected to have 2 subscriptions: actual: (%v)", fetchedRows[0].Subscriptions)
		}
		if len(fetchedRows[0].Orders) != 1 {
			t.Fatalf("row 1 expected to have 1 orders: actual: (%v)", fetchedRows[0].Orders)
		}
	})
	t.Run("one to one join", func(t *testing.T) {
		var fetchedRows []userJoin
		err := pgc.Select(
			&fetchedRows,
			pgcq.Columns("id", "name"),
			pgcq.Join(&socialJoin{}, userJoinSocial),
			pgcq.Join(&orderJoin{}, userJoinOrder, "total"),
			pgcq.GreaterOrEqual("total", 200),
			pgcq.Order("name", pgcq.ASC),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fetchedRows) != 3 {
			t.Fatalf("expected %d items, %d given", 2, len(fetchedRows))
		}
		if len(fetchedRows[0].Orders) != 1 {
			t.Fatalf("row 1 expected to have 1 orders: actual: (%v)", fetchedRows[0].Orders)
		}
		if len(fetchedRows[1].Orders) != 1 {
			t.Fatalf("row 1 expected to have 1 orders: actual: (%v)", fetchedRows[1].Orders)
		}
		if fetchedRows[0].Social.TwitterID != s1.TwitterID {
			t.Errorf("row 1 expected twitter ID %s: actual: (%s)", s1.TwitterID, fetchedRows[0].Social.TwitterID)
		}
		if fetchedRows[0].Social.FBID != s1.FBID {
			t.Errorf("row 1 expected fb ID %s: actual: (%s)", s1.FBID, fetchedRows[0].Social.FBID)
		}
		if fetchedRows[1].Social.TwitterID != s2.TwitterID {
			t.Errorf("row 2 expected twitter ID %s: actual: (%s)", s2.TwitterID, fetchedRows[1].Social.TwitterID)
		}
		if fetchedRows[1].Social.FBID != s2.FBID {
			t.Errorf("row 2 expected fb ID %s: actual: (%s)", s2.FBID, fetchedRows[1].Social.FBID)
		}
		if fetchedRows[2].Social != nil {
			t.Errorf("row 3 expected to have empty social, actual: (%v)", fetchedRows[2].Social)
		}
	})

	t.Run("many to many join", func(t *testing.T) {
		type lessonJoin struct {
			ID   string
			Name string
		}

		type studentJoin struct {
			ID      string
			Name    string
			Lessons []lessonJoin `pgc:"join"`
		}

		type studentLessonJoin struct {
			ID        string
			StudentID string
			LessonID  string
			PGC       struct{} `pgc:"many_to_many"`
		}

		pgc.MustCreateTable(&lessonJoin{})
		pgc.MustCreateTable(&studentJoin{})
		pgc.MustCreateTable(&studentLessonJoin{})

		l1 := &lessonJoin{ID: "111", Name: "lesson1"}
		l2 := &lessonJoin{ID: "222", Name: "lesson2"}
		l3 := &lessonJoin{ID: "333", Name: "lesson3"}
		l4 := &lessonJoin{ID: "444", Name: "lesson4"}
		pgc.MustInsert(l1, l2, l3, l4)

		s1 := &studentJoin{ID: "111", Name: "student1"}
		s2 := &studentJoin{ID: "222", Name: "student2"}
		s3 := &studentJoin{ID: "333", Name: "student3"}
		s4 := &studentJoin{ID: "444", Name: "student4"}
		pgc.MustInsert(s1, s2, s3, s4)

		sl1 := &studentLessonJoin{ID: "1", StudentID: "111", LessonID: "111"}
		sl2 := &studentLessonJoin{ID: "2", StudentID: "111", LessonID: "222"}
		sl3 := &studentLessonJoin{ID: "3", StudentID: "222", LessonID: "333"}
		sl4 := &studentLessonJoin{ID: "4", StudentID: "333", LessonID: "444"}
		pgc.MustInsert(sl1, sl2, sl3, sl4)

		const studentJoinStudentLesson = "student_join.id = student_lesson_join.student_id"
		const lessonJoinStudentLesson = "lesson_join.id = student_lesson_join.lesson_id"

		var fetchedRows []studentJoin
		err := pgc.Select(
			&fetchedRows,
			pgcq.Join(&studentLessonJoin{}, studentJoinStudentLesson),
			pgcq.Join(&lessonJoin{}, lessonJoinStudentLesson),
			pgcq.NotEqual(pgc.GetColumnName(&lessonJoin{}, "name"), "lesson1"),
			pgcq.Order("name", pgcq.ASC),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fetchedRows) != 3 {
			t.Fatalf("expected %d items, %d given", 3, len(fetchedRows))
		}
		if len(fetchedRows[0].Lessons) != 1 {
			t.Fatalf("row 1 expected to have 2 lessons: actual: (%v)", fetchedRows[0].Lessons)
		}
		if fetchedRows[0].Lessons[0].Name != l2.Name {
			t.Fatalf("row 1 lesson expected to be (%s), actual: (%s)", fetchedRows[0].Lessons[0].Name, l2.Name)
		}
		if len(fetchedRows[1].Lessons) != 1 {
			t.Fatalf("row 2 expected to have 1 lessons: actual: (%v)", fetchedRows[1].Lessons)
		}
		if fetchedRows[1].Lessons[0].Name != l3.Name {
			t.Fatalf("row 2 lesson expected to be (%s), actual: (%s)", fetchedRows[1].Lessons[0].Name, l3.Name)
		}
		if len(fetchedRows[2].Lessons) != 1 {
			t.Fatalf("row 3 expected to have 1 lessons: actual: (%v)", fetchedRows[2].Lessons)
		}
		if fetchedRows[2].Lessons[0].Name != l4.Name {
			t.Fatalf("row 3 lesson expected to be (%s), actual: (%s)", fetchedRows[2].Lessons[0].Name, l4.Name)
		}
	})
	t.Run("join on get", func(t *testing.T) {
		var u userJoin
		found, err := pgc.Get(
			&u,
			pgcq.Equal(pgc.GetColumnName(&userJoin{}, "id"), user1.ID),
			pgcq.Join(&socialJoin{}, userJoinSocial),
			pgcq.Join(&orderJoin{}, userJoinOrder, "total"),
			pgcq.GreaterOrEqual("total", 200),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Fatalf("user %s not found", u.ID)
		}
		if len(u.Orders) != 1 {
			t.Fatalf("row 1 expected to have 1 orders: actual: (%v)", u.Orders)
		}
		if u.Social.TwitterID != s1.TwitterID {
			t.Errorf("row 1 expected twitter ID %s: actual: (%s)", s1.TwitterID, u.Social.TwitterID)
		}
		if u.Social.FBID != s1.FBID {
			t.Errorf("row 1 expected fb ID %s: actual: (%s)", s1.FBID, u.Social.FBID)
		}

		var notFoundUser userJoin
		found, err = pgc.Get(
			&notFoundUser,
			pgcq.Equal(pgc.GetColumnName(&userJoin{}, "id"), util.NewGuid()),
			pgcq.Join(&socialJoin{}, userJoinSocial),
			pgcq.Join(&orderJoin{}, userJoinOrder, "total"),
			pgcq.GreaterOrEqual("total", 200),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if found {
			t.Errorf("expect to not found user, actual user: %v", notFoundUser)
		}
	})
}
