package db

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"
)

// Получаем название таблицы
func (p *Parent) GetTableName() string {
	return "`" + p.DbTable + "`"
}

// Формируем набор строк для запроса в mysql
func (p *Parent) GetFiledsString() string {
	fields := []string{}

	for _, v := range p.Fields {
		if v.IsDb {
			fields = append(fields, "`"+v.Name+"`")
		}
	}

	return strings.Join(fields, ", ")
}

// Добавляем поле в объект
func (p *Parent) AddField(f Field) {
	p.Fields = append(p.Fields, f)
}

// Устанавливаем особое значение
func (p *Parent) SetSpecial(n, v string) {
	p.Lock()
	for i := range p.Fields {
		if p.Fields[i].Name == n {
			p.Fields[i]._to_commit = true
			p.Fields[i]._special_value = v
			break
		}
	}
	p.Unlock()
}

// Устанавливаем значение объекта
func (p *Parent) Set(n string, v interface{}) {
	p.Lock()
	for i := range p.Fields {
		if p.Fields[i].Name == n {
			// Првоеряем тип переменной
			if p.Fields[i].Type != reflect.TypeOf(v).String() {
				log.Fatalln("[fatal]", "bad type field", p.Fields[i].Type, reflect.TypeOf(v), v)
			}

			// Для времени своя сверка
			if p.Fields[i].Type == "time.Time" {
				// Если старое значение не указано
				if p.Fields[i].Value == nil {
					p.Fields[i]._to_commit = true
				} else {
					t1 := p.Fields[i].Value.(time.Time)
					t2 := v.(time.Time)
					if t1.String() != t2.String() {
						p.Fields[i]._to_commit = true
					}
				}
			} else {
				// Отмечаем надо ли обновить переменную при коммите
				if p.Fields[i].IsDb && p.Fields[i].Value != v {
					p.Fields[i]._to_commit = true
				}
			}

			// Только если изменилось значение
			if p.Fields[i]._to_commit {
				// Проверяем не NULL ли это
				p.Fields[i].CheckNullValue(v)
			}

			// Обновляем
			p.Fields[i].Value = v

			break
		}
	}
	p.Unlock()
}

// Получаем значение объекта
func (p *Parent) Get(n string) (v interface{}) {
	p.RLock()
	for _, f := range p.Fields {
		if f.Name == n {
			v = f.Value
			break
		}
	}
	p.RUnlock()
	return
}

// Получаем значение объекта строки
func (p *Parent) GetStr(n string) (v string) {
	i := p.Get(n)
	if i == nil {
		return
	}

	return i.(string)
}

// Получаем значение объекта срез строк
func (p *Parent) GetStrArr(n string) (v []string) {
	i := p.Get(n)
	if i == nil {
		return
	}

	ia := i.([]interface{})

	v = make([]string, len(ia))
	for k, d := range ia {
		v[k] = d.(string)
	}

	return
}

// Получаем значение объекта int
func (p *Parent) GetInt(n string) (v int) {
	i := p.Get(n)
	if i == nil {
		return
	}

	v = i.(int)

	return
}

// Получаем значение объекта int64
func (p *Parent) GetInt64(n string) (v int64) {
	i := p.Get(n)
	if i == nil {
		return
	}

	v = i.(int64)

	return
}

// Получаем значение объекта float64
func (p *Parent) GetFloat(n string) (v float64) {
	i := p.Get(n)
	if i == nil {
		return
	}

	v = i.(float64)

	return
}

// Получаем значение объекта срез строк
func (p *Parent) GetIntArr(n string) (v []int) {
	i := p.Get(n)
	if i == nil {
		return
	}

	v = i.([]int)

	return
}

// Получаем значение объекта строки
func (p *Parent) GetBool(n string) (v bool) {
	i := p.Get(n)
	if i == nil {
		return
	}

	return i.(bool)
}

// Получаем значение объекта строки
func (p *Parent) GetTime(n string) (v time.Time) {
	i := p.Get(n)
	if i == nil {
		return
	}

	return i.(time.Time)
}

// Возвращаем json объета
func (p *Parent) GetJson() (b []byte) {
	js := make(map[string]interface{}, len(p.Fields))
	p.RLock()
	for _, f := range p.Fields {
		if f.IsJson {
			js[f.Name] = f.Value
		}
	}
	p.RUnlock()

	b, err := json.Marshal(js)
	if err != nil {
		fmt.Println(err)
	}

	return
}

// Возвращаем хэш объекта
func (p *Parent) GetMap() (m map[string]interface{}) {
	m = make(map[string]interface{}, len(p.Fields))
	p.RLock()
	for _, f := range p.Fields {
		if f.IsJson {
			m[f.Name] = f.Value
		}
	}
	p.RUnlock()

	return
}

// Сохраняем описание объекта
func (p *Parent) DescribeToByte() (b []byte) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(p.Fields)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	b = buf.Bytes()
	return
}

// Сохраняем описание объекта
func (p *Parent) ByteToDescribe(b []byte) {
	var buf bytes.Buffer
	_, err := buf.Write(b)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	dec := gob.NewDecoder(&buf)
	err = dec.Decode(&p.Fields)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}

// Возвращаем формат даты для mysql
func GetMysqlTimeFormat() string {
	return "2006-01-02 15:04:05"
}
