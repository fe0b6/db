package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	Dbh *sqlx.DB
)

// Подключение к базе
func Connect(o InitConnect) {
	var err error
	socktype := "unix"

	// Кодировка по умолчанию
	if o.Charset == "" {
		o.Charset = "utf8mb4"
	}

	// Если это конект по tcp
	if mysqlTcpSocketReg.MatchString(o.Socket) {
		socktype = "tcp"
	}

	Dbh, err = sqlx.Open("mysql", fmt.Sprintf("%s:%s@%s(%s)/%s?charset=%s", o.Login,
		o.Password, socktype, o.Socket, o.DBName, o.Charset))
	if err != nil {
		log.Fatalln("[fatal]", err)
		return
	}
}

// MustBegin starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
func MustBegin() *sqlx.Tx {
	tx, err := Dbh.Beginx()
	if err != nil {
		panic(err)
	}
	return tx
}

// Формируем структуру объекта
func (p *Parent) CreateFields() (err error) {
	rows, err := Dbh.Query(`SHOW FULL COLUMNS FROM ` + p.DbTable)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Println("[error]", err)
		return
	}

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Println("[error]", err)
		return
	}

	// Хэш для запоминания уникальных ключей
	skeys := make(map[string]bool)

	p.Fields = []Field{}
	for rows.Next() {
		// Make a slice for the values
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Println("[error]", err)
			return
		}

		mt := mysqlType{}
		for i, col := range values {
			switch columns[i] {
			case "Field":
				mt.Name = string(col)
			case "Type":
				mt.Type = string(col)
			case "Comment":
				mt.Comment = string(col)
			case "Key":
				mt.Key = string(col)
			case "Null":
				mt.Null = string(col)
			}
		}

		p.Fields = append(p.Fields, Field{
			Name:   mt.Name,
			Type:   mt.GetType(),
			IsDb:   mt.IsDb(),
			IsJson: mt.IsJson(),
			Null:   mt.CanNull(),
		})

		// Отмечаем что ключ уникальный
		if mt.Key == "UNI" {
			skeys[mt.Name] = true
		}
	}

	// Проверяем что вторичные ключи уникальны
	for _, k := range p.SKeys {
		_, ok := skeys[k]
		if !ok {
			log.Fatal("[fatal]", "secondary key not uni: ", k)
		}
	}

	return
}

// Инициализация по первичному ключу
func (p *Parent) GetFromDB(o *InitObj) (err error) {
	if o == nil {
		return
	}

	// Есть ли транзакция?
	if o.Tx != nil {
		p.Tx = o.Tx
	} else if o.ForUpdate {
		p.Tx = MustBegin()
	}

	// Если это пустая инициализация - то заканчиваем
	if o.Empty {
		return
	}

	// Если не указаны поля для выборки - подставляем все
	if o.Fields == "" {
		o.Fields = p.GetFiledsString()
	}

	// Собираем запрос
	var sqlrq, val string
	if o.PK != "" {
		sqlrq = fmt.Sprintf(`SELECT %s FROM %s WHERE %s=?`, o.Fields,
			p.GetTableName(), p.PKey)
		val = o.PK
	} else if o.SKN != "" && o.SKV != "" {
		for _, n := range p.SKeys {
			if n == o.SKN {
				sqlrq = fmt.Sprintf(`SELECT %s FROM %s WHERE %s=?`, o.Fields,
					p.GetTableName(), n)
				val = o.SKV
				break
			}
		}
	} else { // Если нет ключей - возвращаем ошибку как будто ничего не нашли
		err = errors.New("sql: no rows in result set")
		return
	}

	// Если нет ключей для поиска
	if sqlrq == "" {
		return
	}

	// Если надо залочить
	if o.ForUpdate {
		sqlrq += " FOR UPDATE"
	}

	var rows *sql.Rows
	if p.Tx != nil {
		rows, err = p.Tx.Query(sqlrq, val)
	} else {
		rows, err = Dbh.Query(sqlrq, val)
	}
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return p.ParseDbFields(rows)
}

// Парсинг параметров полученных из базы
func (p *Parent) ParseDbFields(rows *sql.Rows) (err error) {
	// Отмечаем что это существующая запись
	p.Existed = true

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Println("[error]", err)
		return
	}

	if !rows.Next() {
		err = errors.New("sql: no rows in result set")
		return
	}

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	err = rows.Scan(scanArgs...)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	for i, col := range values {
		if col == nil {
			continue
		}

		for k := range p.Fields {
			if p.Fields[k].IsDb && p.Fields[k].Name == columns[i] {
				switch p.Fields[k].Type {
				case "int":
					var v int64
					v, err = strconv.ParseInt(string(col), 10, 32)
					if err != nil {
						log.Println("[error]", err, p.DbTable, p.Fields[k].Name, string(col))
						return
					}
					p.Fields[k].Value = int(v)
				case "int64":
					var v int64
					v, err = strconv.ParseInt(string(col), 10, 64)
					if err != nil {
						log.Println("[error]", err, p.DbTable, p.Fields[k].Name, string(col))
						return
					}
					p.Fields[k].Value = v
				case "float64":
					var v float64
					v, err = strconv.ParseFloat(string(col), 64)
					if err != nil {
						log.Println("[error]", err, p.DbTable, p.Fields[k].Name, string(col))
						return
					}
					p.Fields[k].Value = v
				case "string":
					p.Fields[k].Value = string(col)
				case "[]uint8":
					p.Fields[k].Value = []byte(col)
				case "time.Time":
					var t time.Time
					if string(col) != "0000-00-00 00:00:00" {
						t, err = time.Parse("2006-01-02 15:04:05", string(col))
						if err != nil {
							t, err = time.Parse("2006-01-02", string(col))
							if err != nil {
								log.Println("[error]", err, p.DbTable, p.Fields[k].Name, string(col))
								return
							}
						}
					}
					p.Fields[k].Value = t
				}

				break
			}
		}
	}

	return
}

func (p *Parent) CommitTx(txcommit bool) (err error) {
	sqlstr := []string{}
	params := []interface{}{}
	var pkv interface{}

	for _, f := range p.Fields {
		if f._to_commit && f.IsDb {
			// Если надо установить особое значение
			if f._special_value != "" {
				switch f._special_value {
				case "[[+=1]]":
					sqlstr = append(sqlstr, f.Name+"="+f.Name+"+1")
				case "[[-=1]]":
					sqlstr = append(sqlstr, f.Name+"="+f.Name+"-1")
				case "[[=NULL]]":
					sqlstr = append(sqlstr, f.Name+"=NULL")
				default:
					log.Println("[error]", "bad special value", f._special_value)
				}
			} else { // Если обычное значение
				sqlstr = append(sqlstr, f.Name+"=?")
			}

			// Если есть особое значение то ниче не добавляем
			if f._special_value == "" {
				if f.Type == "time.Time" {
					params = append(params, f.FormatTime())
				} else {
					params = append(params, f.Value)
				}
			}
		}
		// Запоминаем главный ключ
		if f.Name == p.PKey {
			pkv = f.Value
		}
	}

	if len(sqlstr) > 0 {
		// Если создаем новую запись
		if !p.Existed {
			var r1 sql.Result
			sqlrq := fmt.Sprintf(`INSERT INTO %s SET %s`, p.GetTableName(),
				strings.Join(sqlstr, ","))

			if p.Tx != nil {
				r1, err = p.Tx.Exec(sqlrq, params...)
			} else {
				r1, err = Dbh.Exec(sqlrq, params...)
			}
			if err != nil {
				log.Println("[error]", err)
				return
			}

			// Отмечаем что это существующая запись
			p.Existed = true

			var id int64
			id, err = r1.LastInsertId()
			if err != nil {
				log.Println("[error]", err)
				return
			}

			if id > 0 {
				for i := range p.Fields {
					// Запоминаем главный ключ
					if p.Fields[i].Name == p.PKey {
						if p.Fields[i].Type == "int" {
							p.Fields[i].Value = int(id)
						} else {
							p.Fields[i].Value = id
						}
					}
				}
			}
		} else {
			// Добавляем главный ключ в запрос
			params = append(params, pkv)

			sqlrq := fmt.Sprintf(`UPDATE %s SET %s WHERE %s=?`, p.GetTableName(),
				strings.Join(sqlstr, ","), p.PKey)

			if p.Tx != nil {
				_, err = p.Tx.Exec(sqlrq, params...)
			} else {
				_, err = Dbh.Exec(sqlrq, params...)
			}
			if err != nil {
				log.Println("[error]", err)
				return
			}
		}
	}

	// Если надо сделать коммит
	if p.Tx != nil && txcommit {
		err = p.Tx.Commit()
		if err != nil {
			log.Println("[error]", err)
			return
		}
	}

	return
}

// Коммит данных в базу
func (p *Parent) Commit() (err error) {
	return p.CommitTx(true)
}

// Откат
func (p *Parent) Rollback() (err error) {
	err = p.Tx.Rollback()
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}

// Удаление записи
func (p *Parent) Delete() (err error) {
	pkv := p.Get(p.PKey)
	sqlrq := fmt.Sprintf(`DELETE FROM %s WHERE %s=?`, p.GetTableName(), p.PKey)

	if p.Tx != nil {
		_, err = p.Tx.Exec(sqlrq, pkv)
	} else {
		_, err = Dbh.Exec(sqlrq, pkv)
	}
	if err != nil {
		log.Println("[error]", err)
		return
	}

	// Если надо сделать коммит
	if p.Tx != nil {
		err = p.Tx.Commit()
		if err != nil {
			log.Println("[error]", err)
			return
		}
	}

	return
}
