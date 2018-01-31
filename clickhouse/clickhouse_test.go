package clickhouse

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/andreyvit/diff"
	_ "github.com/kshvakov/clickhouse"
)

// Really basic end-to-end test.
func TestIntegration(t *testing.T) {
	cmd := exec.Command("docker-compose", "up", "--force-recreate")
	err := cmd.Start()
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Second * 5)
	var input interface{}
	source, err := ioutil.ReadFile("../testdata/harvest.json")
	if err != nil {
		t.Error(err)
	}
	err = json.Unmarshal(source, &input)
	if err != nil {
		t.Error(err)
	}
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?username=&debug=false")
	if err != nil {
		t.Error(err)
	}
	logger := log.New(os.Stderr, "", log.Ldate|log.Ltime)
	driver := Driver{
		DB:         connect,
		ID:         "_test_id",
		Datestamp:  "_test_date",
		Timestamp:  "_test_timestamp",
		Hash:       "_test_hash",
		LatestView: "%s_latest",
		Logger:     logger,
	}
	err = driver.Insert(input, "name", 1, "2017-06-27T16:47:14Z")
	if err != nil {
		t.Error(err)
	}
	target, err := ioutil.ReadFile("testdata/harvest-schema.txt")
	if err != nil {
		t.Error(err)
	}
	output, err := exec.Command("docker", "run", "--rm", "--network", "container:clickhouse", "yandex/clickhouse-client", "--host", "clickhouse", "-q", "SHOW CREATE TABLE name").Output()
	lines := strings.Replace(string(output), ",", ",\n", -1)
	if err != nil {
		t.Error(err)
	}
	if lines != string(target) {
		t.Errorf("Output schema does not match target:\n%v", diff.LineDiff(lines, string(target)))
	}
	target, err = ioutil.ReadFile("testdata/harvest-insert.txt")
	if err != nil {
		t.Error(err)
	}
	output, err = exec.Command("docker", "run", "--rm", "--network", "container:clickhouse", "yandex/clickhouse-client", "--host", "clickhouse", "-q", "SELECT * FROM name FORMAT Vertical").Output()
	if err != nil {
		t.Error(err)
	}
	if string(output) != string(target) {
		t.Errorf("Insert query does not match target:\n%v", diff.LineDiff(string(output), string(target)))
	}
	if err := exec.Command("docker-compose", "rm", "-s", "-f").Run(); err != nil {
		t.Error(err)
	}
	cmd.Wait()
}
