package main

import (
	"database/sql"
	"fmt"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/prometheus/common/log"
	"github.com/qiffang/tools/client"
	"github.com/qiffang/tools/data"
	"github.com/wushilin/stream"
	"strings"
)

func main() {
	//conf := meta.TiDBConfig{
	//	Server: "127.0.0.1",
	//	Port:   10080,
	//}

	//regions, err := meta.GetRegions(conf, "test", "t110")
	//if err != nil {
	//	log.Error(err)
	//	return
	//}
	//log.Info("Table Info ", fmt.Sprintf("ID=%d", regions.TableID))
	//
	c, err := client.NewClient([]string{"127.0.0.1:2379"}, config.Security{})
	if err != nil {
		log.Error("Create client failed", fmt.Sprintf("err=%v", err))
		return
	}

	tbl, err := c.GetTable("test", "thash")

	if _, ok := tbl.(table.PartitionedTable); ok {
		log.Info(" Table Info ")
	}

	tblInfo, err := c.GetTableInfo("test", "thash_const")

	log.Info(tblInfo.Name)

	//c.Scan(tablecodec.EncodeTablePrefix(regions.TableID), tablecodec.EncodeTablePrefix(regions.TableID+1))

	//ctx := context.Background()
	//for _, id := range regions.RegionIDs {
	//	log.Info("Region Info ", fmt.Sprintf("ID=%d", id))
	//	_, err := c.GetRegionInfo(ctx, id)
	//	if err != nil {
	//		log.Error(err)
	//		continue
	//	}
	//
	//	kr := meta.GetKeyRange(c, id)
	//	c.Scan(kr.Startkey, kr.EndKey)
	//}
	//stream.FromArray(regions.RegionIDs).Each(func(id interface{}) {
	//
	//})

	tableSQL := "create table t1111(id int) partition by hash(id) partitions 4"
	_, err = genSQL(tableSQL)
	if err != nil {
		log.Error("Generate sql faield", err)
		return
	}

	tidbConfig := data.DBConfig{
		Host:     "127.0.0.1",
		User:     "root",
		Password: "",
		Name:     "test",
		Port:     4000,
	}
	//insertTiDBCount := insertData(sqls, tableSQL, tidbConfig, TIDB)

	mysqlConfig := data.DBConfig{
		Host:     "127.0.0.1",
		User:     "root",
		Password: "root",
		Name:     "test",
		Port:     3306,
	}
	//insertMySQLCount := insertData(sqls, tableSQL, mysqlConfig, MYSQL)

	//if insertMySQLCount != insertTiDBCount {
	//	log.Error("Insert number does not match. ", fmt.Sprintf("mysql=%d, tidb=%d", insertMySQLCount, insertTiDBCount))
	//}

	dataInMySQL := queryMysql(mysqlConfig, "test", "t1111")
	dataInTiDB := queryTiDB(tidbConfig, []string{"127.0.0.1:2379"}, "test", "t1111")

	for key, v := range dataInMySQL {
		if vt, exist := dataInTiDB[key]; exist {
			if vt == v {
				log.Info("Success. ", fmt.Sprintf("partitionName=%s", key))
				continue
			} else {
				log.Warn("Failed. ", fmt.Sprintf("key=%s, valueInMySQL=%d, valueInTiDB=%d", key, v, vt))
			}
		}

		stream.FromMapKeys(dataInTiDB).Each(func(k interface{}) {
			log.Warn("Failed", fmt.Sprintf("keyInMySQL=%s, KeysInTiDB=%s", key, k))
		})

	}

}

func queryTiDB(cfg data.DBConfig, pdAddrs []string, dbName, tableName string) map[string]int {
	r := make(map[string]int)

	c, err := client.NewClient(pdAddrs, config.Security{})
	if err != nil {
		log.Error("Create client failed. ", err)
		return r
	}

	info, err := c.GetTableInfo(dbName, tableName)
	if err != nil {
		log.Error("Fetch table info failed", err)
		return r
	}

	stream.FromArray(info.GetPartitionInfo().Definitions).Each(func(definition interface{}) {
		id := definition.(model.PartitionDefinition).ID
		r[definition.(model.PartitionDefinition).Name.L] = c.Scan(tablecodec.EncodeTablePrefix(id), tablecodec.EncodeTablePrefix(id+1), id)
	})

	return r
}

type DbType string

const (
	TIDB  DbType = "TiDB"
	MYSQL DbType = "MySQL"
)

var (
	mysqlPartitionSql = "select PARTITION_NAME, TABLE_ROWS from information_schema.partitions where TABLE_SCHEMA='%s' and TABLE_NAME='%s'"
)

type MysqlPartitionInfo struct {
	name string
	rows int
}

func MysqlPartitionInfoScan(rows *sql.Rows) (interface{}, error) {
	info := MysqlPartitionInfo{}
	err := rows.Scan(&info.name, &info.rows)

	if err != nil {
		return nil, err
	}

	return info, nil
}

func queryMysql(cfg data.DBConfig, dbName, tableName string) map[string]int {
	qr := data.ExecSQLWithReurn(cfg, fmt.Sprintf(mysqlPartitionSql, dbName, tableName), MysqlPartitionInfoScan)

	r := make(map[string]int)
	for _, info := range qr {
		r[strings.ToLower(info.(MysqlPartitionInfo).name)] = info.(MysqlPartitionInfo).rows
	}

	return r
}

func insertData(sql []string, tableSQL string, config data.DBConfig, dbtype DbType) int {
	err := data.ExecSQLWithoutConn(config, tableSQL)
	if err != nil {
		log.Error("Create table failed", fmt.Sprintf("err=%v", err))
		return -1
	}

	conn, err := data.CreateDB(config)
	if err != nil {
		log.Error("Create failed", fmt.Sprintf("err=%v", err))
		return -1
	}
	defer data.CloseDB(conn)

	success, _ := stream.FromArray(sql).Map(func(sql interface{}) interface{} {
		if err := data.ExecSQL(conn, sql.(string)); err != nil {
			log.Warn("Execute SQL failed. ", fmt.Sprintf("type=%s, sql=%v, err=%v", dbtype, sql, err))
			return 0
		}

		return 1
	}).Sum().Value()

	return success.(int)
}

func genSQL(tableSql string) ([]string, error) {
	table := data.NewTable()
	err := data.ParseTableSQL(table, tableSql)
	if err != nil {
		log.Fatal(err.Error())
	}

	return data.GenRowDatas(table, 10000)
}
