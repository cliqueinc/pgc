package pgc

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cliqueinc/pgc/pgcq"
	"github.com/jackc/pgx"
)

// defaultAdapter allows to perform pgc operations without creating adapter instance.
var defaultAdapter *Adapter

func getDefault() *Adapter {
	if defaultAdapter == nil {
		defaultAdapter = NewAdapter()
	}

	return defaultAdapter
}

// NewAdapter creates new adapter instance.
func NewAdapter() *Adapter {
	return &Adapter{&mustAdapter{&crudAdapter{con: getConn()}}}
}

// Begin begins new transaction.
func Begin() (*TxAdapter, error) {
	return getDefault().Begin()
}

// MustInsert ensures struct will be inserted without errors, panics othervise.
// Limit of items to insert at once is 1000 items.
func MustInsert(structPtrs ...interface{}) {
	getDefault().MustInsert(structPtrs...)
}

// Insert inserts struct into db. If no options specied, struct will be updated by primary key.
// Limit of items to insert at once is 1000 items.
func Insert(structPtrs ...interface{}) error {
	return getDefault().Insert(structPtrs...)
}

// MustUpdate ensures struct will be updated without errors, panics othervise.
func MustUpdate(structPtr interface{}) {
	getDefault().MustUpdate(structPtr)
}

// Update updates struct by primary key.
func Update(structPtr interface{}) error {
	return getDefault().Update(structPtr)
}

// MustUpdateRows ensures rows are updated without errors, panics othervise. Returns number of affected rows.
// In case when you really need to update all rows (e.g. migration script), you need to pass pgc.QueryAll() option.
// It is done to avoid unintentional update of all rows.
func MustUpdateRows(structPtr interface{}, colsMap Map, opts ...pgcq.Option) int64 {
	return getDefault().MustUpdateRows(structPtr, colsMap, opts...)
}

// UpdateRows updates rows with specified map data by query, returns number of affected rows.
// In case when you really need to update all rows (e.g. migration script), you need to pass pgc.QueryAll() option.
// It is done to avoid unintentional update of all rows.
func UpdateRows(structPtr interface{}, colsMap Map, opts ...pgcq.Option) (int64, error) {
	return getDefault().UpdateRows(structPtr, colsMap, opts...)
}

// MustSelect ensures select will not produce any error, panics othervise.
func MustSelect(destSlicePtr interface{}, opts ...pgcq.Option) {
	getDefault().MustSelect(destSlicePtr, opts...)
}

// Select performs select using query options. If not options specified, all rows will be returned.
// destSlicePtr parameter expects pointer to a slice
func Select(destSlicePtr interface{}, opts ...pgcq.Option) error {
	return getDefault().Select(destSlicePtr, opts...)
}

// MustSelectCustomData ensures select will not produce any error, panics othervise.
func MustSelectCustomData(model interface{}, destSlicePtr interface{}, opts ...pgcq.Option) {
	getDefault().MustSelectCustomData(model, destSlicePtr, opts...)
}

// SelectCustomData selects custom columns like aggregated one or any specified by struct form destSlicePtr.
// Use pgc_name tage to specify custom column name, e.g. `COUNT(*) as count`. If no options specified, all rows will be returned.
// destSlicePtr parameter expects pointer to a slice
func SelectCustomData(model interface{}, destSlicePtr interface{}, opts ...pgcq.Option) error {
	return getDefault().SelectCustomData(model, destSlicePtr, opts...)
}

// MustGet returns whether or not it found the item and panic on errors.
func MustGet(structPtr interface{}, opts ...pgcq.Option) bool {
	return getDefault().MustGet(structPtr, opts...)
}

// Get gets struct by primary key or by specified options.
func Get(structPtr interface{}, opts ...pgcq.Option) (found bool, err error) {
	return getDefault().Get(structPtr, opts...)
}

// MustDelete ensures struct will be deleted without errors, panics othervise.
func MustDelete(structPtr interface{}) {
	getDefault().MustDelete(structPtr)
}

// Delete deletes struct by primary key or by specified options.
func Delete(structPtr interface{}) error {
	return getDefault().Delete(structPtr)
}

// MustDeleteRows ensures rows are deleted without errors, panics othervise. Returns number of affected rows.
func MustDeleteRows(structPtr interface{}, opts ...pgcq.Option) int64 {
	return getDefault().MustDeleteRows(structPtr, opts...)
}

// DeleteRows deletes rows by specified options. Returns number of affected rows.
func DeleteRows(structPtr interface{}, opts ...pgcq.Option) (int64, error) {
	return getDefault().DeleteRows(structPtr, opts...)
}

// MustCount gets rows count, panics in case of an error.
func MustCount(model interface{}, opts ...pgcq.Option) int {
	return getDefault().MustCount(model, opts...)
}

// Count gets rows count by query.
func Count(model interface{}, opts ...pgcq.Option) (int, error) {
	return getDefault().Count(model, opts...)
}

// MustCreateTable ensures table is created from struct.
func MustCreateTable(structPtr interface{}) {
	a := &MigrationAdapter{crudAdapter: getDefault().crudAdapter}
	a.MustCreateTable(structPtr)
}

// CreateTable creates table from struct.
func CreateTable(structPtr interface{}) error {
	a := &MigrationAdapter{crudAdapter: getDefault().crudAdapter}
	return a.CreateTable(structPtr)
}

// IsUniqueViolationError checks whether an error is unique constraint violation error.
func IsUniqueViolationError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "SQLSTATE "+PGECUniqueViolation)
}

// IsTableExistsError checks whether an error is table already exists error.
func IsTableExistsError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), PGECTableExists)
}

// SelectAllWhere performs raw select and panics in case of errors.
func SelectAllWhere(destSlicePtr interface{}, sqlWhereStmt string, args ...interface{}) {
	if sqlWhereStmt != "" && strings.HasPrefix(strings.ToLower(sqlWhereStmt), "select") {
		panic("pgc: If provided, SelectAllWhere.sqlWhereStmt must not start with select")
	}

	mod, sliceValElement, sliceTypeElement, err := parseDestSlice(destSlicePtr)
	if err != nil {
		panic(err.Error())
	}

	finalSQL := renderTemplate(Map{"mod": mod, "fields": mod.Fields, "joins": nil}, selectBaseTemplate) + " " + sqlWhereStmt + ";"
	if cfg.LogQueries {
		fmt.Println(finalSQL)
	}
	err = rawSelect(finalSQL, nil, nil, nil, true, sliceValElement, sliceTypeElement, getDefault().con, args...)
	if err != nil {
		panic(err.Error())
	}
}

func Query(stmt string, args ...interface{}) (*pgx.Rows, error) {
	con := getDefault().con
	rows, err := con.Query(stmt, args...)

	return rows, err
}

func rawSelect(sqlStmt string, columns []string, joinMods []*model, joinFields [][]*field, requirePK bool, sliceValElement reflect.Value,
	sliceTypeElement reflect.Type, con connection, args ...interface{}) error {

	if cfg.LogQueries {
		fmt.Println(sqlStmt, args)
	}

	rows, err := con.Query(sqlStmt, args...)
	if err != nil {
		return err
	}

	var prevRow struct {
		modPK            string
		model            reflect.Value
		parsedJoinModels map[string][]string
	}
	var (
		mod       *model
		modFields []*field
		modPK     string
		valAddrs  []interface{}
		rowModel  reflect.Value
		rowJoins  []reflect.Value
	)

	defer rows.Close()
	for rows.Next() {
		if mod == nil {
			mod = parseModel(reflect.New(sliceTypeElement).Interface(), requirePK)
			modFields = mod.getFields(columns)
			valAddrs = make([]interface{}, 0, len(modFields))
		} else {
			valAddrs = valAddrs[:0]
			rowJoins = rowJoins[:0]
		}
		rowModel = reflect.New(mod.ReflectType.Elem())
		for i := range joinMods {
			rowJoins = append(rowJoins, reflect.New(joinMods[i].ReflectType.Elem()))
		}
		for i := range modFields {
			valAddrs = append(valAddrs, rowModel.Elem().Field(modFields[i].FieldPos).Addr().Interface())
		}
		for i := range joinMods {
			for ind := range joinFields[i] {
				valAddrs = append(valAddrs, rowJoins[i].Elem().Field(joinFields[i][ind].FieldPos).Addr().Interface())
			}
		}

		err = rows.Scan(valAddrs...)
		if err != nil {
			panic(err)
		}

		if mod.PKPos != -1 {
			modPK = mod.getPK(rowModel)
		}
		// rowIsTheSame is used for checks whether the next row represents the same
		// model, and if yes it means that the difference is in the different join model,
		// which happens in one to many relation.
		rowIsTheSame := modPK != "" && modPK == prevRow.modPK
		if len(joinMods) != 0 {
			for i := range joinMods {
				joinName := joinMods[i].ReflectType.Elem().Name()
				joinPos, ok := mod.Joins[joinName]
				if !ok {
					panic(fmt.Sprintf("unknown join %s", joinMods[i].ReflectType.Elem().Name()))
				}
				modJoin := rowModel.Elem().Field(joinPos)

				var joinPKVal string
				if joinMods[i].PKPos != -1 {
					joinPKVal = joinMods[i].getPK(rowJoins[i])
				}

				// during join select we replace possible joined null values with default values,
				// as pgx don't want to parse null into string), so we just check whether
				// joined primary key is empty, which means that this row don't have anything joined.
				if joinPKVal == "" {
					continue
				}

				// if case its one to one join
				if modJoin.Kind() != reflect.Slice {
					if modJoin.Kind() == reflect.Ptr {
						modJoin.Set(rowJoins[i])
					} else {
						modJoin.Set(rowJoins[i].Elem())
					}
					continue
				}

				// in case one-to-many join we want to ensure that we haven't already added this
				// join to our model, thats why we keep added models in prevRow.parsedJoinModels map.
				var modelAlreadySet bool
				if prevRow.parsedJoinModels != nil {
					if parsedModels, ok := prevRow.parsedJoinModels[joinName]; ok {
						for _, mID := range parsedModels {
							if mID == joinPKVal {
								modelAlreadySet = true
								break
							}
						}
					}
				} else {
					prevRow.parsedJoinModels = make(map[string][]string)
				}
				if modelAlreadySet {
					continue
				}
				prevRow.parsedJoinModels[joinName] = append(prevRow.parsedJoinModels[joinName], joinPKVal)

				// set current join model to our real model.
				if rowIsTheSame {
					prevVal := prevRow.model.Elem().Field(joinPos)
					prevVal.Set(reflect.Append(prevVal, rowJoins[i].Elem()))
				} else {
					slice := reflect.MakeSlice(reflect.SliceOf(joinMods[i].ReflectType.Elem()), 0, 1)
					rowModel.Elem().Field(joinPos).Set(reflect.Append(slice, rowJoins[i].Elem()))
				}
			}
		}
		if !rowIsTheSame {
			prevRow.model = rowModel
			prevRow.modPK = modPK
		}

		if !rowIsTheSame {
			// if our model is scanned first time, just append it to other models
			sliceValElement.Set(reflect.Append(sliceValElement, rowModel.Elem()))
		} else {
			// if this model was already scanned, but some joins were added to it,
			// we just want to update the latest slice element with newest changes.
			sliceValElement.Index(sliceValElement.Len() - 1).Set(prevRow.model.Elem())
		}
	}
	err = rows.Err()

	if err != nil {
		return err
	}

	return nil
}
