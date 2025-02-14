package task

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"go_stream/controller"
	"log"
	"strings"
	"time"
)

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

	sql       string // insert ignore into xxx(id, name, age) values 即可后续会自动拼接数据
	dataToSql func(data any) string
	cache     []string
}

func NewMysqlSinkTask(taskName string, host string, port int, user string, passwd string, db string, batchUpdate int, timeUpdate int, sql string, dataToSql func(data any) string) *MysqlSinkTask {
	return &MysqlSinkTask{
		TaskName:    taskName,
		host:        host,
		port:        port,
		user:        user,
		passwd:      passwd,
		db:          db,
		batchUpdate: batchUpdate,
		timeUpdate:  timeUpdate,
		sql:         sql,
		cache:       make([]string, 0, batchUpdate),
		dataToSql:   dataToSql,
	}
}

func (m *MysqlSinkTask) Open(controller *controller.StatusController) {
	controller.Register("mysql_sink_"+m.TaskName, &m.cache)

	// 连接sql
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", m.user, m.passwd, m.host, m.port, m.db)
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	m.conn = conn

	m.SetRegisterStatusName([]string{"mysql_sink_" + m.TaskName})

}

func (m *MysqlSinkTask) Process(message controller.Message[any], controller *controller.StatusController) {

	if message.IsInner {
		if len(m.cache) != 0 {
			// 写入
			m.writeToMysql()
		}
	} else {
		m.cache = append(m.cache, m.dataToSql(message.Data))
		if len(m.cache) >= m.batchUpdate {
			m.writeToMysql()

		}
	}
}

func (m *MysqlSinkTask) writeToMysql() {
	dataSql := m.sql + strings.Join(m.cache, ",")
	_, err := m.conn.Exec(dataSql)
	if err != nil {
		log.Fatal(err)
	}
	m.cache = m.cache[:0]
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
