package db

import (
	"database/sql"
	"log"
	"reflect"
	"regexp"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	mysqlIntReg       *regexp.Regexp
	mysqlBigIntReg    *regexp.Regexp
	mysqlStrReg       *regexp.Regexp
	mysqlFloatReg     *regexp.Regexp
	mysqlByteReg      *regexp.Regexp
	mysqlTimeStampReg *regexp.Regexp
	mysqlNoJsReg      *regexp.Regexp
	mysqlNoDbReg      *regexp.Regexp
	mysqlTcpSocketReg *regexp.Regexp

	fpOrderByCleanReg *regexp.Regexp
	fpLimitCleanReg   *regexp.Regexp
	fpGroupByCleanReg *regexp.Regexp
	fpFieldsCleanReg  *regexp.Regexp
	fpWhereCleanReg   *regexp.Regexp
)

func init() {
	mysqlIntReg = regexp.MustCompile("int\\(")
	mysqlBigIntReg = regexp.MustCompile("bigint")
	mysqlStrReg = regexp.MustCompile("text|char|enum|set")
	mysqlFloatReg = regexp.MustCompile("float|double|decimal")
	mysqlByteReg = regexp.MustCompile("binary")
	mysqlTimeStampReg = regexp.MustCompile("timestamp")
	mysqlTcpSocketReg = regexp.MustCompile("[^:]:[0-9]")

	mysqlNoJsReg = regexp.MustCompile("nojson")
	mysqlNoDbReg = regexp.MustCompile("--deleted--")

	fpOrderByCleanReg = regexp.MustCompile("[^a-zA-Z0-9,()_.='` \n\r\t]")
	fpLimitCleanReg = regexp.MustCompile("[^0-9,]")
	fpGroupByCleanReg = regexp.MustCompile("[^a-zA-Z0-9,()_` \n\r\t]")
	fpFieldsCleanReg = regexp.MustCompile("[^a-zA-Z0-9_,`*]")
	fpWhereCleanReg = regexp.MustCompile("[^a-zA-Z0-9,()_.='` ?<>\n\r\t*+-]")
}

// Объект конекта к базе
type InitConnect struct {
	Login    string
	Password string
	Socket   string
	DBName   string
	Charset  string
}

// Родительский объект
type Parent struct {
	Fields     []Field
	DbTable    string
	PKey       string
	SKeys      []string
	MapAddFunc func(map[string]interface{})
	Tx         *sqlx.Tx
	Existed    bool
	sync.RWMutex
}

// Объект значения строки
type Field struct {
	Name           string
	Type           string
	Value          interface{}
	IsJson         bool
	IsDb           bool
	Null           bool
	_special_value string
	_to_commit     bool
}

// Преобразование значения времени в строку понятную mysql
func (f *Field) FormatTime() string {
	t, ok := f.Value.(time.Time)
	if !ok {
		return ""
	}

	return t.Format(GetMysqlTimeFormat())
}

// Проверяем не надо ли значение заменить на NULL
func (f *Field) CheckNullValue(v interface{}) {
	// Если NULL не подходит
	if !f.Null {
		return
	}

	var null bool
	switch reflect.TypeOf(v).String() {
	case "int64":
		if v.(int64) == 0 {
			null = true
		}
	case "int":
		if v.(int) == 0 {
			null = true
		}
	case "float64":
		if v.(float64) == 0 {
			null = true
		}
	case "string":
		if v.(string) == "" {
			null = true
		}
	}

	// Если новое значение nil и старое nil, то и комитить ничего ненадо
	if f.Value == nil && null {
		f._to_commit = false
		return
	}

	// Если это нуль значение - в базу пишем NULL
	if null {
		f._special_value = "[[=NULL]]"
	}
}

// Объект для запроса группы интерфейсов
type ForeachItemInterfaceObj struct {
	NewFunc     func(*InitObj) (interface{}, error)
	ForeachFunc func(interface{}, *ForeachParam, ...interface{}) (interface{}, *sql.Rows, error)
	ParseFunc   func(interface{}, *sql.Rows) (interface{}, error)
	TxFunc      func(interface{}) *sqlx.Tx
	Param       *ForeachParam
	Vals        []interface{}
}

// Объект для запроса группы объектов
type ForeachParam struct {
	OrderBy     string
	Limit       string
	GroupBy     string
	Where       string
	Fields      string
	CondEntries []string
	ForUpdate   bool
}

func (fp *ForeachParam) Clean() {
	if fp == nil {
		return
	}

	ob := fpOrderByCleanReg.ReplaceAllString(fp.OrderBy, "")
	lm := fpLimitCleanReg.ReplaceAllString(fp.Limit, "")
	gb := fpGroupByCleanReg.ReplaceAllString(fp.GroupBy, "")
	fi := fpFieldsCleanReg.ReplaceAllString(fp.Fields, "")
	wh := fpWhereCleanReg.ReplaceAllString(fp.Where, "")

	if ob != fp.OrderBy {
		log.Println("[info]", "order by", ob, fp.OrderBy)
	}
	fp.OrderBy = ob

	if lm != fp.Limit {
		log.Println("[info]", "limit", lm, fp.Limit)
	}
	fp.Limit = lm

	if gb != fp.GroupBy {
		log.Println("[info]", "group by", gb, fp.GroupBy)
	}
	fp.GroupBy = gb

	if fi != fp.Fields {
		log.Println("[info]", "fields", fi, fp.Fields)
	}
	fp.Fields = fi

	if wh != fp.Where {
		log.Println("[info]", "where", wh, fp.Where)
	}
	fp.Where = wh

	for i, v := range fp.CondEntries {
		wh2 := fpWhereCleanReg.ReplaceAllString(v, "")
		if wh2 != v {
			log.Println("[info]", "CondEntries", wh2, v)
		}

		fp.CondEntries[i] = wh2
	}
}

// Объект для инициализации значения из базы
type InitObj struct {
	PK        string
	SKN       string
	SKV       string
	Fields    string
	ForUpdate bool
	Tx        *sqlx.Tx
	Empty     bool
}

// Объект типа строки в mysql
type mysqlType struct {
	Name    string
	Type    string
	Key     string
	Comment string
	Null    string
}

func (mt mysqlType) GetType() (ans string) {
	if mysqlBigIntReg.MatchString(mt.Type) {
		ans = "int64"
	} else if mysqlIntReg.MatchString(mt.Type) {
		ans = "int"
	} else if mysqlStrReg.MatchString(mt.Type) {
		ans = "string"
	} else if mysqlFloatReg.MatchString(mt.Type) {
		ans = "float64"
	} else if mysqlByteReg.MatchString(mt.Type) {
		ans = "[]uint8"
	} else if mysqlTimeStampReg.MatchString(mt.Type) {
		ans = "time.Time"
	}

	return
}

func (mt mysqlType) IsJson() (ok bool) {
	if !mysqlNoJsReg.MatchString(mt.Comment) && !mysqlNoDbReg.MatchString(mt.Comment) {
		ok = true
	}

	return
}

func (mt mysqlType) IsDb() (ok bool) {
	if !mysqlNoDbReg.MatchString(mt.Comment) {
		ok = true
	}

	return
}

func (mt mysqlType) CanNull() (ok bool) {
	if mt.Null == "NO" {
		return
	}

	ok = true
	return
}
