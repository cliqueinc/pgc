package pgc

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
)

const (
	pgt_pk_string   = "text PRIMARY KEY"
	pgt_date_time   = "timestamp without time zone NOT NULL"
	pgt_jsonb_dict  = "jsonb DEFAULT '{}'::jsonb NOT NULL"
	pgt_jsonb_array = "jsonb DEFAULT '[]'::jsonb NOT NULL"
	pgt_small_int   = "smallint DEFAULT 0 NOT NULL"
	pgt_big_int     = "bigint DEFAULT 0 NOT NULL"
	pgt_integer     = "integer DEFAULT 0 NOT NULL"
	pgt_boolean     = "boolean DEFAULT false NOT NULL"
	pgt_float64     = "double precision DEFAULT 0 NOT NULL"
	pgt_float       = "real DEFAULT 0 NOT NULL"
	pgt_text        = "text DEFAULT ''::text NOT NULL"
)

func init() {
	cachedModelMap.Init()
}

type model struct {
	Struct     interface{}
	StructName string

	// Used to generate the model definition
	ShortName string
	TableName string

	Fields []*field

	ReflectType reflect.Type

	// Explicitly store the PK name for where clauses
	// Note PKName will always be id for now
	PKName string
	// PKPos is a position of a primary key.
	PKPos int

	// used if we don't want to fetch model's fields
	NoFields bool

	// Joins maps joined table name to joined field position.
	Joins map[string]int
}

type modelSyncMap struct {
	modelMap map[string]*model
	mux      sync.Mutex
}

func (mm *modelSyncMap) Get(name string) (*model, bool) {
	val, ok := mm.modelMap[name]
	return val, ok
}

func (mm *modelSyncMap) Init() {
	mm.mux.Lock()
	mm.modelMap = make(map[string]*model)
	mm.mux.Unlock()
}

func (mm *modelSyncMap) Set(name string, m *model) {
	mm.mux.Lock()
	mm.modelMap[name] = m
	mm.mux.Unlock()
}

var cachedModelMap modelSyncMap

// In cases like UPDATE we need to get the list of fields sans ID since you can't update PK.
// Must be exported since the templates call this.
func (mod *model) GetFieldsNoPK(columns []string) []*field {
	fields := mod.getFields(columns)
	filteredFields := make([]*field, 0, len(fields))
	for _, f := range fields {
		if f.PGName == mod.PKName {
			continue
		}
		filteredFields = append(filteredFields, f)
	}

	return filteredFields
}

func (mod *model) getFields(columns []string) []*field {
	if len(columns) == 0 {
		return mod.Fields
	}
	fields := make([]*field, 0, len(columns))
	for i := range mod.Fields {
		if i == mod.PKPos {
			fields = append(fields, mod.Fields[i])
			continue
		}

		var includeColumn bool
		for _, col := range columns {
			if col == mod.Fields[i].PGName {
				includeColumn = true
				break
			}
		}

		if includeColumn {
			fields = append(fields, mod.Fields[i])
		}
	}
	if len(columns) != len(fields) { // some column not exists in db
	ColumnsLoop:
		for _, col := range columns {
			for _, f := range fields {
				if f.PGName == col {
					continue ColumnsLoop
				}
			}
			panic(fmt.Sprintf("unrecognized column (%s)", col))
		}
	}

	return fields
}

// Get a slice of the vals for interfacing with pgx
func (pm *model) getVals(rowModel reflect.Value, fields []*field) []interface{} {
	vals := make([]interface{}, 0, len(fields))
	for _, f := range fields {
		vals = append(vals, reflect.Indirect(rowModel).Field(f.FieldPos).Interface())
	}
	return vals
}

/*
GoType are the reflect types so you can do == reflect.Ptr or whatever

PGType will be quite limited initially. Basic ints, floats, only text (no need to use varchar with modern pg), lots of jsonb
Will also include the constraints for pk and all not null initially.

*/
type field struct {
	TableName    string
	GoName       string
	PGName       string
	PGType       string
	ReflectType  reflect.Type
	ReflectValue reflect.Value
	ReflectKind  reflect.Kind

	// FieldPos is a position of a field in our struct.
	FieldPos int

	pgNameQuoted       string
	pgNameQuotedSelect string
	joinedPGName       string
}

func (f *field) PGNameQuoted() string {
	if f.pgNameQuoted != "" {
		return f.pgNameQuoted
	}
	trimString := func(str string) string {
		return strings.Replace(strings.Replace(str, ";", "", -1), "\"", "", -1)
	}
	escapeString := func(str string) string {
		return "\"" + trimString(str) + "\""
	}
	f.pgNameQuoted = escapeString(f.PGName)

	return f.pgNameQuoted
}

func (f *field) PGNameQuotedSelect() string {
	if f.pgNameQuotedSelect != "" {
		return f.pgNameQuotedSelect
	}
	trimString := func(str string) string {
		return strings.Replace(strings.Replace(str, ";", "", -1), "\"", "", -1)
	}
	escapeString := func(str string) string {
		return "\"" + trimString(str) + "\""
	}

	pgName := f.PGName
	parts := strings.Split(strings.ToLower(pgName), " as ")
	if len(parts) > 1 {
		if len(parts) == 2 {
			pgName = trimString(parts[0]) + " as " + escapeString(parts[1])
		} else {
			panic(fmt.Sprintf("invalid column name (%s)", pgName))
		}
	} else {
		pgName = "\"" + f.TableName + "\"." + escapeString(pgName)
	}

	f.pgNameQuotedSelect = pgName
	return f.pgNameQuotedSelect
}

func (f *field) JoinedPGName() string {
	if f.joinedPGName != "" {
		return f.joinedPGName
	}
	trimString := func(str string) string {
		return strings.Replace(strings.Replace(str, ";", "", -1), "\"", "", -1)
	}
	escapeString := func(str string) string {
		return "\"" + trimString(str) + "\""
	}

	pgName := "\"" + f.TableName + "\"." + escapeString(f.PGName)
	var defaultVal string
	switch f.ReflectKind {
	case reflect.Int8, reflect.Int16, reflect.Int, reflect.Int32, reflect.Uint, reflect.Uint32, reflect.Uint8,
		reflect.Int64, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.Uint16:
		defaultVal = "0"
	case reflect.String:
		defaultVal = "''"
	case reflect.Struct:
		defaultVal = "'{}'::jsonb"
		if f.ReflectType.Name() == "Time" {
			defaultVal = "CURRENT_TIMESTAMP"
		}
	case reflect.Array, reflect.Slice:
		defaultVal = "'[]'::jsonb"
		if f.ReflectType.Elem().Kind() == reflect.String {
			defaultVal = "array[]::text[]"
		} else if f.ReflectType.Elem().Kind() == reflect.Int {
			defaultVal = "array[]::int[]"
		}
	case reflect.Bool:
		defaultVal = "false"
	}

	// in case join return null raw replace null with column default value.
	f.joinedPGName = "COALESCE(" + pgName + ", " + defaultVal + ")"
	return f.joinedPGName
}

func (fi *field) isPrimaryKey() bool {
	if strings.Contains(strings.ToLower(fi.PGType), "primary key") {
		return true
	}
	return false
}

func (mod *model) setTableName(rowModel reflect.Value) {
	tableNameMethod := rowModel.MethodByName("TableName")

	if tableNameMethod.IsValid() {
		vals := tableNameMethod.Call([]reflect.Value{})
		mod.TableName = vals[0].String()
	} else {
		if mod.StructName == "" { // Just in case...
			panic("Cannot call setTableName until StructName is set")
		}
		mod.TableName = parseName(mod.StructName)
	}
}

func (mod *model) getPK(rowModel reflect.Value) string {
	if mod.PKPos == -1 {
		panic(fmt.Sprintf("Missing primary key for table (%s)", mod.TableName))
	}
	return reflect.Indirect(rowModel).Field(mod.PKPos).String()
}

func parseModel(mm interface{}, requirePK bool) *model {
	modType := reflect.TypeOf(mm)
	typeName := modType.String()
	if mod, ok := cachedModelMap.Get(typeName); ok {
		return mod
	}

	mod := &model{
		Struct:      mm,
		ReflectType: modType,
	}
	modKind := modType.Kind()
	rowModel := reflect.ValueOf(mm)

	if modKind != reflect.Ptr || rowModel.Elem().Kind() != reflect.Struct {
		panic("Please pass a struct pointer to parseModel")
	}
	elem := rowModel.Elem()
	elemType := elem.Type()
	mod.StructName = elemType.Name()

	mod.setTableName(rowModel)

	fieldLen := elem.NumField()
	mod.Fields = make([]*field, 0, fieldLen)
	for i := 0; i < fieldLen; i++ {
		fieldName := elemType.Field(i).Name

		// Get the pgc struct tag for this field
		tagValue := strings.TrimSpace(elemType.Field(i).Tag.Get("pgc"))
		if tagValue == "-" {
			continue
		}
		fieldType := elemType.Field(i).Type
		fieldKind := fieldType.Kind()

		if tagValue == "join" {
			if mod.Joins == nil {
				mod.Joins = make(map[string]int)
			}
			var joinType string
			elType := fieldType
			if fieldKind == reflect.Slice {
				elType = fieldType.Elem()
			}
			if fieldKind == reflect.Ptr {
				joinType = elType.Elem().Name()
			} else {
				joinType = elType.Name()
			}
			mod.Joins[joinType] = i
			continue
		}
		// reserved field name
		if fieldName == "PGC" {
			if tagValue == "many_to_many" {
				mod.NoFields = true
			}
			continue
		}

		var pgName string
		if tagName := strings.TrimSpace(elemType.Field(i).Tag.Get("pgc_name")); tagName != "" {
			pgName = tagName
		} else {
			pgName = parseName(fieldName)
		}

		// Support PK and - struct tags for now
		newField := &field{
			TableName:   mod.TableName,
			GoName:      fieldName,
			PGName:      pgName,
			ReflectKind: fieldKind,
			ReflectType: fieldType,
			FieldPos:    i,
		}

		newField.setPGType(mod, tagValue)
		if newField.PGName == mod.PKName {
			mod.PKPos = i
		}

		mod.Fields = append(mod.Fields, newField)
	}
	// TODO we really need to do more inspection of the model to make sure there isn't
	// more than one PK and/or warn about ID field in addition to PK
	if requirePK && mod.PKName == "" {
		panic(fmt.Sprintf("Missing primary key for table (%s)", mod.TableName))
	}
	cachedModelMap.Set(typeName, mod)

	return mod
}

func (fi *field) setPGType(mod *model, tagVal string) {

	switch tagVal {
	case "pk":
		fi.PGType = pgt_pk_string
		mod.PKName = fi.PGName
		return
	case "dt": // Custom time.Time. Use the dt struct tag for custom
		// times since the below time.Time type assertion will fail
		fi.PGType = pgt_date_time
		return
	case "": // Do nothing special
	default:
		panic("Invalid pgc tag " + tagVal)
	}

	if fi.PGName == "id" && mod.PKName == "" {
		fi.PGType = pgt_pk_string
		mod.PKName = "id"
	} else if fi.ReflectType.Name() == "Time" {
		// Slight hack to see if this is a time object, otherwise we'll use the long below switch
		fi.PGType = pgt_date_time
	} else {
		fi.PGType = getPGBaseType(fi.ReflectKind)
	}
}

func getPGBaseType(goType reflect.Kind) string {
	switch goType {
	// All supported jsonb types. TODO test ptr, might have to dive
	case reflect.Array, reflect.Slice:
		return pgt_jsonb_array

	case reflect.Map, reflect.Struct:
		return pgt_jsonb_dict
	// Small int
	case reflect.Int8, reflect.Int16:
		return pgt_small_int
	// Note postgres does not support uint, so we'll just use int
	case reflect.Int, reflect.Int32, reflect.Uint, reflect.Uint32, reflect.Uint8,
		reflect.Uint16:
		return pgt_integer
	case reflect.Int64, reflect.Uint64:
		return pgt_big_int
	case reflect.Float32:
		return pgt_float
	case reflect.Float64:
		return pgt_float64
	case reflect.String:
		return pgt_text
	case reflect.Bool:
		return pgt_boolean
	}
	panic(fmt.Sprintf("Unsupported type %d", goType))
}

func parseName(name string) string {
	buf := bytes.NewBuffer(make([]byte, 0, 2*len(name)))

	var (
		upperCount               int
		writeUnderscode, isUpper bool
		runes                    = []rune(name)
	)
	for i := range runes {
		isUpper = unicode.IsUpper(runes[i])
		writeUnderscode = i != 0 && upperCount == 0 && isUpper
		if writeUnderscode {
			buf.WriteByte('_')
		}
		if !isUpper {
			// in case there are capitalized letters before camelcase, lile
			// JSONString, so we want to split this into json_string
			if upperCount > 1 {
				for j := 0; j < upperCount-1; j++ {
					buf.WriteRune(unicode.ToLower(runes[i-upperCount+j]))
				}
				buf.WriteByte('_')
			}
			// in case previous letter is capital, write it before current lower letter
			if upperCount > 0 {
				buf.WriteRune(unicode.ToLower(runes[i-1]))
			}
			buf.WriteRune(runes[i])
			upperCount = 0
		} else {
			upperCount++
		}
	}
	// if the last part of string is capitalized, like MyStringJSON, write last part to buffer
	if isUpper {
		for j := 0; j < upperCount; j++ {
			buf.WriteRune(unicode.ToLower(runes[len(runes)-upperCount+j]))
		}
	}

	return buf.String()
}
