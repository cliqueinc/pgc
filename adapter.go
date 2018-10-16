package pgc

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/cliqueinc/pgc/pgcq"
	"github.com/jackc/pgx"
)

// Limits for db ops.
const (
	LimitInsert = 1000
)

// Map is a short representation of map[string]interface{}, used in adapter ops.
type Map map[string]interface{}

// TxAdapter handles basic operations with postgres under transaction.
type TxAdapter struct {
	*crudAdapter
}

// Commit commits transaction.
func (a *TxAdapter) Commit() error {
	return a.con.(*pgx.Tx).Commit()
}

// Rollback performs transaction rollback.
func (a *TxAdapter) Rollback() error {
	return a.con.(*pgx.Tx).Rollback()
}

// MigrationAdapter handles operations with postgres under transaction.
type MigrationAdapter struct {
	*crudAdapter
}

// ExecFile executes sql file.
func (a *MigrationAdapter) ExecFile(fileName string) error {
	filePath := getMigrationPath() + filepath.Base(fileName)
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("cannot open sql file (%s): %v", fileName, err)
	}
	defer f.Close()

	sqlData, err := ioutil.ReadAll(f)
	if err != nil {
		return fmt.Errorf("cannot read sql file (%s): %v", fileName, err)
	}

	return a.Exec(string(sqlData))
}

// Exec executes raw query.
func (a *MigrationAdapter) Exec(sql string, args ...interface{}) error {
	cmdTag, err := a.con.Exec(sql, args...)
	if err != nil {
		return fmt.Errorf("error: (%v), cmdTag (%s)", err, cmdTag)
	}
	return nil
}

// MustCreateTable ensures table is created from struct.
func (a *MigrationAdapter) MustCreateTable(structPtr interface{}) {
	err := a.CreateTable(structPtr)
	if err != nil {
		panic(err)
	}
}

// CreateTable creates table from struct.
func (a *MigrationAdapter) CreateTable(structPtr interface{}) error {
	createTableSQL := GenerateSchema(structPtr)
	if cfg.LogQueries {
		fmt.Println(createTableSQL)
	}
	// TODO decide what to do with cmdTag aka rows created (first param)
	_, err := a.con.Exec(createTableSQL)
	return err
}

// Adapter handles basic operations with postgres.
type Adapter struct {
	*mustAdapter
}

// crudAdapter handles basic operations with db.
type crudAdapter struct {
	con connection
}

// mustAdapter allows panicing during pgc operations.
type mustAdapter struct {
	*crudAdapter
}

type connection interface {
	Exec(sql string, arguments ...interface{}) (commandTag pgx.CommandTag, err error)
	Query(sql string, args ...interface{}) (*pgx.Rows, error)
	QueryRow(sql string, args ...interface{}) *pgx.Row
}

// Begin begins new transaction.
func (a *Adapter) Begin() (*TxAdapter, error) {
	con, err := getConn().Begin()
	if err != nil {
		return nil, err
	}

	return &TxAdapter{crudAdapter: &crudAdapter{con: con}}, nil
}

// MustInsert ensures structs are inserted without errors, panics othervise.
// Limit of items to insert at once is 1000 items.
func (a *mustAdapter) MustInsert(structPtrs ...interface{}) {
	err := a.Insert(structPtrs...)
	if err != nil {
		panic(err)
	}
}

// Insert inserts one or more struct into db. If no options specied, struct will be updated by primary key.
// Limit of items to insert at once is 1000 items.
func (a *crudAdapter) Insert(structPtrs ...interface{}) error {
	if len(structPtrs) == 0 {
		return errors.New("nothing to insert")
	}
	if len(structPtrs) > LimitInsert {
		return fmt.Errorf("insertion of more than (%d) items not allowed", LimitInsert)
	}

	var (
		model *model
		args  []interface{}
	)
	items := make([]interface{}, 0, len(structPtrs))
	for i, structPtr := range structPtrs {
		mod := parseModel(structPtr, true)
		rowModel := reflect.ValueOf(structPtr)
		items = append(items, mod)
		if i == 0 {
			model = mod
			args = make([]interface{}, 0, len(mod.Fields)*len(structPtrs))
		}

		if i != 0 && mod.TableName != model.TableName {
			return errors.New("cannot insert items from different tables")
		}
		args = append(args, mod.getVals(rowModel, mod.Fields)...)
	}
	tmplData := map[string]interface{}{
		"model": model,
		"Items": items,
	}
	insertSQL := renderTemplate(tmplData, insertTemplate)
	if cfg.LogQueries {
		fmt.Println(insertSQL)
	}

	tag, err := a.con.Exec(insertSQL, args...)
	if err != nil {
		return fmt.Errorf("insert error: %v, cmdTag: %s", err, tag)
	}

	return nil
}

// MustUpdate ensures struct will be updated without errors, panics othervise.
func (a *mustAdapter) MustUpdate(structPtr interface{}) {
	err := a.Update(structPtr)
	/*
		Add later:
		if commandTag.RowsAffected() != 1 {
			return errors.New("No row found to delete")
		}
	*/
	if err != nil {
		panic(err)
	}
}

// Update updates struct by primary key.
func (a *crudAdapter) Update(structPtr interface{}) error {
	mod := parseModel(structPtr, true)
	fieldsNoPK := mod.GetFieldsNoPK(nil)

	rowModel := reflect.ValueOf(structPtr)
	args := append(mod.getVals(rowModel, fieldsNoPK), mod.getPK(rowModel))
	updateSQL := renderTemplate(Map{"mod": mod, "fields": fieldsNoPK}, fmt.Sprintf("%s WHERE \"{{.mod.PKName}}\" = $%d;", updateTemplate, len(args)))
	if cfg.LogQueries {
		fmt.Println(updateSQL)
	}
	tag, err := a.con.Exec(updateSQL, args...)
	if err != nil {
		return fmt.Errorf("update error: %v, cmdTag: %s", err, tag)
	}

	return nil
}

// MustUpdateRows ensures rows are updated without errors, panics othervise. Returns number of affected rows.
// In case when you really need to update all rows (e.g. migration script), you need to pass pgc.QueryAll() option.
// It is done to avoid unintentional update of all rows.
func (a *mustAdapter) MustUpdateRows(structPtr interface{}, dataMap Map, opts ...pgcq.Option) int64 {
	rowsNum, err := a.UpdateRows(structPtr, dataMap, opts...)
	if err != nil {
		panic(err)
	}

	return rowsNum
}

// UpdateRows updates rows with specified map data by query, returns number of affected rows.
// In case when you really need to update all rows (e.g. migration script), you need to pass pgc.QueryAll() option.
// It is done to avoid unintentional update of all rows.
func (a *crudAdapter) UpdateRows(structPtr interface{}, dataMap Map, opts ...pgcq.Option) (int64, error) {
	if len(dataMap) == 0 {
		return 0, errors.New("columns for update cannot be empty")
	}
	if len(opts) == 0 {
		return 0, errors.New("query options cannot be empty")
	}

	columns := make([]string, 0, len(dataMap))
	for col := range dataMap {
		columns = append(columns, col)
	}

	mod := parseModel(structPtr, true)
	fieldsNoPK := mod.GetFieldsNoPK(columns)
	args := make([]interface{}, 0, len(dataMap))
	for _, f := range fieldsNoPK {
		val, ok := dataMap[f.PGName]
		if !ok {
			continue
		}

		args = append(args, val)
	}

	stmt, err := pgcq.Build(opts, pgcq.OpUpdate, args...)
	if err != nil {
		return 0, err
	}
	if !stmt.IsQueryAll && !strings.Contains(stmt.Query, "WHERE") {
		return 0, errors.New("query options cannot be empty")
	}

	updateTpl := updateTemplate + " " + stmt.Query + ";"
	updateSQL := renderTemplate(Map{"mod": mod, "fields": fieldsNoPK}, updateTpl)
	if cfg.LogQueries {
		fmt.Println(updateSQL)
	}

	tag, err := a.con.Exec(updateSQL, stmt.Args...)
	if err != nil {
		return 0, fmt.Errorf("update error: %v, cmdTag: %s", err, tag)
	}

	return tag.RowsAffected(), nil
}

// MustSelect ensures select will not produce any error, panics othervise.
func (a *mustAdapter) MustSelect(structPtr interface{}, opts ...pgcq.Option) {
	err := a.Select(structPtr, opts...)
	if err != nil {
		panic(err)
	}
}

// Select performs select using query options. If no options specified, all rows will be returned.
// destSlicePtr parameter expects pointer to a slice
func (a *crudAdapter) Select(destSlicePtr interface{}, opts ...pgcq.Option) error {
	stmt, err := pgcq.Build(opts, pgcq.OpSelect)
	if err != nil {
		return err
	}

	mod, sliceValElement, sliceTypeElement, err := parseDestSlice(destSlicePtr)
	if err != nil {
		return err
	}
	fields := mod.getFields(stmt.Columns)
	joinMods, joinFields, err := processJoins(mod, stmt.Joins)
	if err != nil {
		return err
	}

	finalSQL := renderTemplate(Map{"mod": mod, "fields": fields, "joins": stmt.Joins, "joinFields": joinFields, "joinMods": joinMods}, selectBaseTemplate) + " " + stmt.Query + ";"
	if cfg.LogQueries {
		fmt.Println(finalSQL)
	}

	return rawSelect(finalSQL, stmt.Columns, joinMods, joinFields, true, sliceValElement, sliceTypeElement, a.con, stmt.Args...)
}

// MustSelectCustomData ensures select will not produce any error, panics othervise.
func (a *mustAdapter) MustSelectCustomData(model interface{}, structPtr interface{}, opts ...pgcq.Option) {
	err := a.SelectCustomData(model, structPtr, opts...)
	if err != nil {
		panic(err)
	}
}

// SelectCustomData selects custom columns like aggregated one or any specified by struct form destSlicePtr.
// Use pgc_name tage to specify custom column name, e.g. `COUNT(*) as count`. If no options specified, all rows will be returned.
// destSlicePtr parameter expects pointer to a slice
func (a *crudAdapter) SelectCustomData(model interface{}, destSlicePtr interface{}, opts ...pgcq.Option) error {
	// use table of a given model
	originModel := parseModel(model, true)

	mod, sliceValElement, sliceTypeElement, err := parseDestSlice(destSlicePtr)
	if err != nil {
		return err
	}
	stmt, err := pgcq.Build(opts, pgcq.OpSelect)
	if err != nil {
		return err
	}
	mod.TableName = originModel.TableName
	customFields := make([]*field, 0, len(mod.Fields))
	for _, f := range mod.getFields(stmt.Columns) {
		field := *f
		field.TableName = mod.TableName
		customFields = append(customFields, &field)
	}

	finalSQL := renderTemplate(Map{"mod": mod, "fields": customFields}, selectBaseTemplate)

	finalSQL += " " + stmt.Query + ";"
	if cfg.LogQueries {
		fmt.Println(finalSQL)
	}

	return rawSelect(finalSQL, stmt.Columns, nil, nil, false, sliceValElement, sliceTypeElement, a.con, stmt.Args...)
}

func parseDestSlice(destSlicePtr interface{}) (*model, reflect.Value, reflect.Type, error) {
	var (
		defaultVal  reflect.Value
		defaultType reflect.Type
	)

	rt := reflect.TypeOf(destSlicePtr)
	if rt.Kind() != reflect.Ptr {
		return nil, defaultVal, defaultType, errors.New("please pass a pointer to slice of structs for SelectAllWhere.destSlicePtr")
	}
	rv := reflect.ValueOf(destSlicePtr)

	// This is slice itself (not a pointer to) but is in essence still a pointer
	// to elements (hence you will call sliceElement.Elem())
	sliceValElement := rv.Elem()
	sliceTypeElement := rt.Elem().Elem()

	if rt.Kind() != reflect.Ptr || sliceValElement.Kind() != reflect.Slice ||
		sliceTypeElement.Kind() != reflect.Struct {
		return nil, defaultVal, defaultType, errors.New("please pass a pointer to slice of structs for SelectAllWhere.destSlicePtr")
	}

	// Create a new instance of the slice type for model parsing to render
	// the template to create the sql!
	newThang := reflect.New(sliceTypeElement)
	mod := parseModel(newThang.Interface(), false)

	return mod, sliceValElement, sliceTypeElement, nil
}

// MustGet returns whether or not it found the item and panic on errors.
func (a *mustAdapter) MustGet(structPtr interface{}, opts ...pgcq.Option) bool {
	found, err := a.Get(structPtr, opts...)
	if err != nil {
		panic(fmt.Sprintf("Failed to Get item (%v)", err))
	}
	return found
}

// Get gets struct by primary key or by specified options.
func (a *crudAdapter) Get(structPtr interface{}, opts ...pgcq.Option) (found bool, err error) {
	getTpl := selectBaseTemplate
	var (
		query   = "WHERE \"{{.mod.PKName}}\" = $1"
		args    []interface{}
		columns []string
		stmt    pgcq.Query
	)
	if len(opts) != 0 {
		s, err := pgcq.Build(opts, pgcq.OpSelect)
		if err != nil {
			return false, err
		}
		stmt = *s
		query = stmt.Query
		args = stmt.Args
		columns = stmt.Columns
	}
	mod := parseModel(structPtr, true)
	fields := mod.getFields(columns)
	rowModel := reflect.ValueOf(structPtr)
	if len(opts) == 0 {
		args = []interface{}{mod.getPK(rowModel)}
	}
	if stmt.Joins != nil {
		joinMods, joinFields, err := processJoins(mod, stmt.Joins)
		if err != nil {
			return false, err
		}
		finalSQL := renderTemplate(Map{"mod": mod, "fields": fields, "joins": stmt.Joins, "joinFields": joinFields, "joinMods": joinMods}, selectBaseTemplate) + " " + stmt.Query + ";"
		if cfg.LogQueries {
			fmt.Println(finalSQL)
		}
		sliceValElement := reflect.New(reflect.SliceOf(mod.ReflectType.Elem()))
		if err := rawSelect(finalSQL, stmt.Columns, joinMods, joinFields, true, sliceValElement.Elem(), mod.ReflectType.Elem(), a.con, stmt.Args...); err != nil {
			return false, err
		}
		if sliceValElement.Elem().Len() == 0 {
			return false, nil
		}

		rowModel.Elem().Set(sliceValElement.Elem().Index(0))
		return true, nil
	}

	getTpl += " " + query + ";"
	getSQL := renderTemplate(Map{"mod": mod, "fields": fields}, getTpl)
	if cfg.LogQueries {
		fmt.Println(getSQL)
	}

	row := a.con.QueryRow(getSQL, args...)

	valAddrs := make([]interface{}, 0, len(fields))
	for i := range fields {
		valAddrs = append(valAddrs, rowModel.Elem().Field(fields[i].FieldPos).Addr().Interface())
	}

	err = row.Scan(valAddrs...)
	if err != nil {
		if err.Error() != pgx.ErrNoRows.Error() {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

// MustDelete ensures struct will be deleted without errors, panics othervise.
func (a *mustAdapter) MustDelete(structPtr interface{}) {
	if err := a.Delete(structPtr); err != nil {
		panic(err)
	}
}

// Delete deletes struct by primary key or by specified options.
func (a *crudAdapter) Delete(structPtr interface{}) error {
	mod := parseModel(structPtr, true)
	rowModel := reflect.ValueOf(structPtr)
	pkVal := mod.getPK(rowModel)
	if pkVal == "" {
		return fmt.Errorf("pgc cant delete from table (%s), ID/PK not set", mod.TableName)
	}
	deleteSQL := renderTemplate(mod, deleteTemplate+" WHERE \"{{.PKName}}\" = $1")
	if cfg.LogQueries {
		fmt.Println(deleteSQL)
	}

	cmdTag, err := a.con.Exec(deleteSQL, pkVal)
	if err != nil {
		return fmt.Errorf("delete error: (%v), cmdTag (%v)", err, cmdTag)
	}

	return nil
}

// MustDeleteRows ensures rows are deleted without errors, panics othervise. Returns number of affected rows.
// In case when you really need to update all rows (e.g. migration script), you need to pass pgc.QueryAll() option.
// It is done to avoid unintentional update of all rows.
func (a *mustAdapter) MustDeleteRows(structPtr interface{}, opts ...pgcq.Option) int64 {
	num, err := a.DeleteRows(structPtr, opts...)
	if err != nil {
		panic(err)
	}

	return num
}

// DeleteRows deletes rows by specified options. Returns number of affected rows.
// In case when you really need to update all rows (e.g. migration script), you need to pass pgc.QueryAll() option.
// It is done to avoid unintentional update of all rows.
func (a *crudAdapter) DeleteRows(structPtr interface{}, opts ...pgcq.Option) (int64, error) {
	mod := parseModel(structPtr, true)
	stmt, err := pgcq.Build(opts, pgcq.OpDelete)
	if err != nil {
		return 0, err
	}
	if !stmt.IsQueryAll && !strings.Contains(stmt.Query, "WHERE") {
		return 0, errors.New("query options cannot be empty")
	}

	deleteTpl := deleteTemplate + " " + stmt.Query + ";"
	deleteSQL := renderTemplate(mod, deleteTpl)
	if cfg.LogQueries {
		fmt.Println(deleteSQL)
	}

	cmdTag, err := a.con.Exec(deleteSQL, stmt.Args...)
	if err != nil {
		return 0, fmt.Errorf("delete error: (%v), cmdTag (%v)", err, cmdTag)
	}

	return cmdTag.RowsAffected(), nil
}

// MustCount gets rows count, panics in case of an error.
func (a *mustAdapter) MustCount(model interface{}, opts ...pgcq.Option) int {
	count, err := a.Count(model, opts...)
	if err != nil {
		panic(err)
	}

	return count
}

// Count gets rows count by query.
func (a *crudAdapter) Count(model interface{}, opts ...pgcq.Option) (int, error) {
	type rowsCount struct {
		Count int `pgc_name:"COUNT(*) as count"`
	}

	var rows []rowsCount
	if err := a.SelectCustomData(model, &rows, opts...); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	return rows[0].Count, nil
}

func processJoins(mod *model, joinConfigs []pgcq.JoinConfig) ([]*model, [][]*field, error) {
	if len(joinConfigs) == 0 {
		return nil, nil, nil
	}

	joins := make([]*model, 0, len(joinConfigs))
	joinFields := make([][]*field, 0, len(joinConfigs))
	for i := range joinConfigs {
		joinMod := parseModel(joinConfigs[i].StructPtr, true)
		if _, ok := mod.Joins[joinMod.ReflectType.Elem().Name()]; !ok && !joinMod.NoFields {
			return nil, nil, fmt.Errorf("unknown join relation %s, fields to be joined should be marked with tag pgc:\"join\"", joinMod.ReflectType.String())
		}
		joinConfigs[i].TableName = joinMod.TableName
		if joinMod.NoFields {
			continue
		}
		joins = append(joins, joinMod)
		joinFields = append(joinFields, joinMod.getFields(joinConfigs[i].Columns))
	}

	return joins, joinFields, nil
}
