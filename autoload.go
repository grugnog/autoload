package autoload

import (
	"errors"
	"sort"
	"time"

	"github.com/araddon/dateparse"
)

type Column struct {
	Name  string
	Value interface{}
}

// Flatten generates a flat map from a nested one.  The original may include values of type map and scalar,
// but not slice or struct.  Keys in the flat map will be a compound of descending map keys and slice iterations.
// The presentation of keys is set by a separator.
// Based on github.com/jeremywohl/flatten but only supporting maps, accepting an interface{} and returning a
// sorted slice of columns.
func Flatten(input interface{}, separator string) ([]Column, error) {
	flatMap := make(map[string]interface{})
	err := flatten(true, flatMap, input, "", separator)
	if err != nil {
		return nil, err
	}
	var columns []Column
	for k, v := range flatMap {
		columns = append(columns, Column{Name: k, Value: v})
	}
	sort.Slice(columns, func(i, j int) bool { return columns[i].Name < columns[j].Name })
	return columns, nil
}

func flatten(top bool, flatMap map[string]interface{}, nested interface{}, prefix string, separator string) error {
	assign := func(newKey string, v interface{}) error {
		switch v.(type) {
		case map[string]interface{}:
			if err := flatten(false, flatMap, v, newKey, separator); err != nil {
				return err
			}
		default:
			flatMap[newKey] = v
		}
		return nil
	}

	switch nested.(type) {
	case map[string]interface{}:
		for k, v := range nested.(map[string]interface{}) {
			key := prefix
			if top {
				key += k
			} else {
				key += separator + k
			}
			assign(key, v)
		}
	default:
		return errors.New("nested input must be a map")
	}
	return nil
}

func ToDateTime(datestr string) (date, time bool, d time.Time) {
	d, err := dateparse.ParseAny(datestr)
	if err != nil {
		return
	}
	date = true
	if d.Nanosecond() > 0 || d.Second() > 0 || d.Minute() > 0 || d.Hour() > 0 {
		time = true
	}
	return
}
