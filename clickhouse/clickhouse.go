package clickhouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/grugnog/autoload"
)

type logger interface {
	Printf(string, ...interface{})
}

type Driver struct {
	DB         *sql.DB
	ID         string
	Datestamp  string
	Timestamp  string
	Hash       string
	LatestView string
	Logger     logger
	Debug      logger
}

func (d *Driver) Insert(input interface{}, table string, id int64, timestamp string) error {
	input, hash, err := d.addIdentifiers(input, id, timestamp)
	if err != nil {
		return err
	}
	tableExists, err := d.isTableExisting(table)
	if err != nil {
		return err
	}
	if tableExists == true {
		dataLoaded, err := d.isDataLoaded(table, id, hash)
		if err != nil {
			return err
		}
		if dataLoaded == true {
			d.Debug.Printf("Skipping insert for %d, record is already loaded", id)
			return nil
		}
	}
	names, types, values, err := d.parseColumns(input)
	if err != nil {
		return err
	}
	err = d.setSchema(table, names, types, tableExists)
	if err != nil {
		return err
	}
	d.Debug.Printf("Inserting record %d into %s", id, table)
	err = d.insertData(table, names, values)
	if err != nil {
		return err
	}
	return nil
}

func (d *Driver) setSchema(table string, names, types []string, exists bool) error {
	if exists == false {
		d.Logger.Printf("Creating table %s", table)
		var columnDefs []string
		for i, name := range names {
			columnDefs = append(columnDefs, fmt.Sprintf("%8s%-60s%s", " ", name, types[i]))
		}
		query := fmt.Sprintf("CREATE TABLE %s (\n%s\n) Engine = MergeTree(%s, (%s, %s), 8192)", table, strings.Join(columnDefs, ",\n"), d.Datestamp, d.ID, d.Hash)
		_, err := d.DB.Exec(query)
		if err != nil {
			return err
		}
		if d.LatestView != "" {
			view := fmt.Sprintf(d.LatestView, table)
			d.Logger.Printf("Creating latest view %s", view)
			query = fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s ALL INNER JOIN (SELECT MAX(%s) AS %s, %s FROM %s GROUP BY %s) USING %s, %s", view, table, d.Timestamp, d.Timestamp, d.ID, table, d.ID, d.Timestamp, d.ID)
			_, err := d.DB.Exec(query)
			if err != nil {
				return err
			}
		}
	} else {
		tableColumns, err := d.getTableColumns(table)
		if err != nil {
			return err
		}
		for i, name := range names {
			columnType, ok := tableColumns[name]
			if ok == false {
				// Missing column - add it now.
				d.Logger.Printf("Adding column %s to %s", name, table)
				_, err = d.DB.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, name, types[i]))
				if err != nil {
					return err
				}
			}
			if columnType == "Int64" && types[i] == "Float64" {
				// Numeric type is too narrow - expand it.
				d.Logger.Printf("Expanding int to float for column %s in %s", name, table)
				_, err = d.DB.Exec(fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", table, name, types[i]))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *Driver) isDataLoaded(table string, id int64, hash uint64) (bool, error) {
	var exists bool
	query := fmt.Sprintf("SELECT 1 FROM %s WHERE %s = ? AND %s = ?", table, d.ID, d.Hash)
	err := d.DB.QueryRow(query, id, hash).Scan(&exists)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}
	return true, nil
}

func (d *Driver) insertData(table string, names []string, values []interface{}) error {
	var placeholders []string
	for range names {
		placeholders = append(placeholders, "?")
	}
	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(names, ", "), strings.Join(placeholders, ", "))
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

func (d *Driver) addIdentifiers(input interface{}, id int64, timestamp string) (interface{}, uint64, error) {
	var hash uint64
	var err error
	_, _, t := autoload.ToDateTime(timestamp)
	switch input.(type) {
	case map[string]interface{}:
		if _, ok := input.(map[string]interface{})[d.Hash]; !ok {
			// It's important that the hash is added first so we don't mix in the
			// timestamp (which may be the current time for non-date-related input).
			hash, err = autoload.ToHash(input)
			if err != nil {
				return input, 0, err
			}
			input.(map[string]interface{})[d.Hash] = hash
		}
		if _, ok := input.(map[string]interface{})[d.ID]; !ok {
			input.(map[string]interface{})[d.ID] = id
		}
		if _, ok := input.(map[string]interface{})[d.Datestamp]; !ok {
			input.(map[string]interface{})[d.Datestamp] = t.Format("2006-01-02")
		}
		if _, ok := input.(map[string]interface{})[d.Timestamp]; !ok {
			input.(map[string]interface{})[d.Timestamp] = timestamp
		}
	}
	return input, hash, nil
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
		// Special handling for int64 & uint64 which are used for the internal id/hash fields.
		if column.Name == d.ID {
			v = column.Value.(int64)
			t = "Int64"
		} else if column.Name == d.Hash {
			v = column.Value.(uint64)
			t = "UInt64"
		} else {
			// Otherwise, we only support JSON types, with the json.Number decoder
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
				// All other fields are ignored!
				continue
			}
		}
		names = append(names, column.Name)
		types = append(types, t)
		values = append(values, v)
	}
	return
}

func (d *Driver) isTableExisting(table string) (bool, error) {
	var tableExists int
	err := d.DB.QueryRow(fmt.Sprintf("EXISTS TABLE %s", table)).Scan(&tableExists)
	if err != nil {
		return false, err
	}
	if tableExists == 1 {
		return true, nil
	}
	d.Logger.Printf("Table %s does not exist", table)
	return false, nil
}

func (d *Driver) getTableColumns(table string) (tableColumns map[string]string, err error) {
	tableColumns = make(map[string]string)
	rows, err := d.DB.Query(fmt.Sprintf("DESCRIBE TABLE %s", table))
	if err != nil {
		return
	}
	for rows.Next() {
		var n, t, x, y string
		err = rows.Scan(&n, &t, &x, &y)
		if err != nil {
			return
		}
		tableColumns[n] = t
	}
	return
}
