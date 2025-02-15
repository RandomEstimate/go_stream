package task

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/RandomEstimate/go_stream/controller"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strings"
	"time"
)

/*
snapshot2 版本
需要支持对同一个db 不同数据表的支持

snapshot4 版本
需要支持对未建立数据表建立数据表功能
*/

type MysqlSinkTask struct {
	controller.BaseTask

	TaskName    string
	host        string
	port        int
	user        string
	passwd      string
	db          string
	batchUpdate int
	timeUpdate  int // ms

	conn *sql.DB

	sql string // insert ignore into %s (id, name, age) values 即可后续会自动拼接数据

	dataToSql func(data any) (string, string) // 返回的第一个字段是数据表、第二个字段是数据
	cache     map[string][]string

	dataToCreateTable func(data any) (string, string) // 返回的第一个字段是数据表、第二个字段是建表语句
	createTableCache  map[string][]string             // tableName -- [sql, 标志位（是否被执行）]
}

func NewMysqlSinkTask(taskName string, host string, port int, user string, passwd string, db string,
	batchUpdate int, timeUpdate int, sql string, dataToSql func(data any) (string, string),
	dataToCreateTable func(data any) (string, string)) *MysqlSinkTask {
	return &MysqlSinkTask{
		TaskName:          taskName,
		host:              host,
		port:              port,
		user:              user,
		passwd:            passwd,
		db:                db,
		batchUpdate:       batchUpdate,
		timeUpdate:        timeUpdate,
		sql:               sql,
		cache:             make(map[string][]string),
		dataToSql:         dataToSql,
		dataToCreateTable: dataToCreateTable,
		createTableCache:  make(map[string][]string),
	}
}

func (m *MysqlSinkTask) Open(controller *controller.StatusController) {
	controller.Register("mysql_sink_cache_"+m.TaskName, &m.cache)
	controller.Register("mysql_sink_create_cache_"+m.TaskName, &m.createTableCache)

	// 连接sql
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", m.user, m.passwd, m.host, m.port, m.db)
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	m.conn = conn

	m.SetRegisterStatusName([]string{"mysql_sink_cache_" + m.TaskName, "mysql_sink_create_cache_" + m.TaskName})

}

func (m *MysqlSinkTask) Process(message controller.Message[any], controller *controller.StatusController) {

	if message.IsInner {
		if m.checkSend() {
			// 写入
			m.writeToMysql()
		}
	} else {
		t, d := m.dataToSql(message.Data)
		if _, ok := m.cache[t]; !ok {
			m.cache[t] = make([]string, 0)
		}

		m.cache[t] = append(m.cache[t], d)

		if m.dataToCreateTable != nil {
			t, createSql := m.dataToCreateTable(message.Data)
			if _, ok := m.createTableCache[t]; !ok {
				m.createTableCache[t] = make([]string, 0, 2)
				m.createTableCache[t] = append(m.createTableCache[t], createSql, "")
			}
		}

		if m.checkSend() {
			m.writeToMysql()
		}
	}
}

func (m *MysqlSinkTask) checkSend() bool {
	for k := range m.cache {
		if len(m.cache[k]) > 0 {
			return true
		}
	}

	return false
}

func (m *MysqlSinkTask) writeToMysql() {

	// 先进行创建表
	if m.dataToCreateTable != nil {
		for k := range m.createTableCache {
			if m.createTableCache[k][1] == "" {
				_, err := m.conn.Exec(m.createTableCache[k][0])
				if err != nil {
					log.Fatal(err)
				}
				m.createTableCache[k][1] = "ok"
			}
		}
	}

	for k := range m.cache {
		if len(m.cache[k]) > 0 {
			dataSql := fmt.Sprintf(m.sql, k) + strings.Join(m.cache[k], ",")
			_, err := m.conn.Exec(dataSql)
			if err != nil {
				log.Fatal(err)
			}
			m.cache[k] = m.cache[k][:0]
		}
	}
}

func (m *MysqlSinkTask) SinkRun(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(m.timeUpdate))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			message := controller.Message[any]{
				Ts:      0,
				IsCk:    false,
				IsInner: true,
				Data:    nil,
			}

			m.GetReceiveMessageChan() <- message
		}
	}
}

func (m *MysqlSinkTask) Close(controller *controller.StatusController) {
	m.conn.Close()
}
