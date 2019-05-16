package meta

import (
	"encoding/json"
	"fmt"
	"github.com/pingcap/parser/model"
	"github.com/qiffang/tools/util"
)

type TiDBConfig struct {
	Server string
	Port   int
}

type RegionInfos struct {
	TableID   int64
	RegionIDs []uint64
}

// Get schema Information about db.table
// url: http://{TiDBIP}:10080/schema/{db}/{table}
func GetTableInfo(config TiDBConfig, dbName, tableName string) (*model.TableInfo, error) {
	path := fmt.Sprintf("schema/%s/%s", dbName, tableName)

	body, err := util.HttpGet(config.Server, config.Port, path)
	if err != nil {
		return nil, err
	}

	info := &model.TableInfo{}
	if err = json.Unmarshal(body, info); err != nil {
		return nil, err
	}
	return info, nil
}
