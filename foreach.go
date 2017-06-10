package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// Получаем группу интерфейсов из базы
func ForeachItemInterface(fiio ForeachItemInterfaceObj) (objs []interface{}, err error) {
	// Создаем новый интерфейс объекта
	i, err := fiio.NewFunc(nil)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	// Делаем выборку из базы пор параметрам
	i, rows, err := fiio.ForeachFunc(i, fiio.Param, fiio.Vals...)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Println("[error]", err)
		return
	}

	// Проверяем нужна ли пранзакция
	tx := fiio.TxFunc(i)

	// Читаем параметры из базы
	for {
		// Новый интерфейс объекта
		i, err = fiio.NewFunc(&InitObj{Tx: tx, Empty: true})
		if err != nil {
			log.Println("[error]", err)
			return
		}

		// Вставляем в интерфейс данные из базы
		i, err = fiio.ParseFunc(i, rows)
		if err != nil {
			// Если уже обработали все полученные объекты из базы
			if err.Error() == "sql: no rows in result set" {
				err = nil
				break
			}
			log.Println("[error]", err)
			return
		}

		// Собираем срез интерфейсов
		objs = append(objs, i)
	}

	return
}

// Получаем группу объектов
func (p *Parent) ForeachItem(param *ForeachParam, vals ...interface{}) (rows *sql.Rows, err error) {
	if param == nil {
		param = &ForeachParam{}
	}

	// Подчищаем переданные параметры
	param.Clean()

	// Если не указана сортировка - то по основному ключу
	if param.OrderBy == "" {
		param.OrderBy = p.PKey
	}

	// Если не указаны параметры для выборки - выбираем все
	if param.Fields == "" {
		param.Fields = p.GetFiledsString()
	}

	// Если не указано условие выборки - выбираем все
	if param.Where == "" {
		param.Where = "1"
	}

	// Если указан лимит - то подставляем ключевое слово
	if param.Limit != "" {
		param.Limit = "LIMIT " + param.Limit
	}

	// Если указан GROUP BY - то подставляем ключевое слово
	if param.GroupBy != "" {
		param.GroupBy = "GROUP BY " + param.GroupBy
	}

	// Дополнительные параметры запроса
	where2 := "1"
	if len(param.CondEntries) > 0 {
		where2 = strings.Join(param.CondEntries, " AND ")
	}

	// строка запроса
	sqlrq := fmt.Sprintf(`SELECT %s FROM %s WHERE (%s) AND (%s) %s ORDER BY %s %s`,
		param.Fields, p.DbTable, param.Where, where2, param.GroupBy, param.OrderBy,
		param.Limit)

	if param.ForUpdate {
		sqlrq += " FOR UPDATE"
		if p.Tx == nil {
			p.Tx = MustBegin()
		}
	}

	if p.Tx != nil {
		rows, err = p.Tx.Query(sqlrq, vals...)
	} else {
		rows, err = Dbh.Query(sqlrq, vals...)
	}
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}
