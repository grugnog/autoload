package clickhouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/grugnog/autoload"
)

type Driver struct {
	DB        *sql.DB
	ID        string
	Datestamp string
	Timestamp string
}

func (d *Driver) Insert(input interface{}, tablename string, id json.Number, timestamp string) error {
	input = d.addIdentifiers(input, id, timestamp)
	names, types, values, err := d.parseColumns(input)
	if err != nil {
		return err
	}
	err = d.schema(tablename, names, types)
	if err != nil {
		return err
	}
	err = d.insertQuery(tablename, names, values)
	if err != nil {
		return err
	}
	return nil
}

func (d *Driver) schema(tablename string, names, types []string) error {
	if d.tableExists(tablename) == false {
		var columnDefs []string
		for i, name := range names {
			columnDefs = append(columnDefs, fmt.Sprintf("%8s%-60s%s", " ", name, types[i]))
		}
		query := fmt.Sprintf("CREATE TABLE %s (\n%s\n) Engine = ReplacingMergeTree(%s, (%s), 8192, %s)", tablename, strings.Join(columnDefs, ",\n"), d.Datestamp, d.ID, d.Timestamp)
		_, err := d.DB.Exec(query)
		if err != nil {
			return err
		}
	} else {
		tableColumns, err := d.tableColumns(tablename)
		if err != nil {
			return err
		}
		for i, name := range names {
			columnType, ok := tableColumns[name]
			if ok == false {
				// Missing column - add it now.
				_, err = d.DB.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tablename, name, types[i]))
				if err != nil {
					return err
				}
			}
			if columnType == "Int64" && types[i] == "Float64" {
				// Numeric type is too narrow - expand it.
				_, err = d.DB.Exec(fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", tablename, name, types[i]))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *Driver) insertQuery(tablename string, names []string, values []interface{}) error {
	var placeholders []string
	for range names {
		placeholders = append(placeholders, "?")
	}
	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tablename, strings.Join(names, ", "), strings.Join(placeholders, ", "))
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	if _, err := stmt.Exec(values...); err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (d *Driver) addIdentifiers(input interface{}, id json.Number, timestamp string) interface{} {
	_, _, t := autoload.ToDateTime(timestamp)
	switch input.(type) {
	case map[string]interface{}:
		input.(map[string]interface{})[d.ID] = id
		input.(map[string]interface{})[d.Datestamp] = t.Format("2006-01-02")
		input.(map[string]interface{})[d.Timestamp] = timestamp
	}
	return input
}

func (d *Driver) parseColumns(input interface{}) (names, types []string, values []interface{}, err error) {
	var t string
	flat, err := autoload.Flatten(input, "_")
	if err != nil {
		return
	}
	for _, column := range flat {
		t = ""
		var v interface{}
		// We only support JSON types, with the json.Number decoder
		// https://golang.org/pkg/encoding/json/#Unmarshal
		switch column.Value.(type) {
		case bool:
			t = "UInt8"
			if column.Value.(bool) == true {
				v = 1
			} else {
				v = 0
			}
		case json.Number:
			v, err = column.Value.(json.Number).Int64()
			t = "Int64"
			if err != nil {
				fmt.Println(err)
				v, err = column.Value.(json.Number).Float64()
				t = "Float64"
				if err != nil {
					column.Value = column.Value.(json.Number).String()
					t = "String"
				}
			}
		case string:
			t = "String"
			v = fmt.Sprint(column.Value)
			if strings.IndexAny(column.Value.(string), "-/ :") > 0 {
				date, time, d := autoload.ToDateTime(column.Value.(string))
				if date {
					v = d
					t = "Date"
					if time {
						t = "DateTime"
					}
				}
			}
		default:
			continue
		}
		names = append(names, column.Name)
		types = append(types, t)
		values = append(values, v)
	}
	return
}

func (d *Driver) tableExists(tablename string) bool {
	var table bool
	err := d.DB.QueryRow(fmt.Sprintf("EXISTS TABLE %s", tablename)).Scan(&table)
	return (err == nil) && table
}

func (d *Driver) tableColumns(tablename string) (table map[string]string, err error) {
	table = make(map[string]string)
	rows, err := d.DB.Query(fmt.Sprintf("DESCRIBE TABLE %s", tablename))
	if err != nil {
		return
	}
	for rows.Next() {
		var n, t, x, y string
		err = rows.Scan(&n, &t, &x, &y)
		if err != nil {
			return
		}
		table[n] = t
	}
	return
}
