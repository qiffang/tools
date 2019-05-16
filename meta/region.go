package meta

import (
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/server"
	"github.com/prometheus/common/log"
	"github.com/qiffang/tools/client"
	"github.com/qiffang/tools/util"
	"github.com/wushilin/stream"
)

type KeyRange struct {
	Startkey kv.Key
	EndKey   kv.Key
}

// GetKeyRange get key range by region id
func GetKeyRange(c *client.ClusterClient, id uint64) *KeyRange {
	rm, err := c.GetRegion(id)
	if err != nil {
		log.Error("GET keyRange failed. ", fmt.Sprintln("id=%d, err=%v", id, err))
		return nil
	}

	return &KeyRange{
		Startkey: rm.Region.StartKey,
		EndKey:   rm.Region.EndKey,
	}
}

// Get regions Information from db.table
// Return tableID and region id list
// url: http://{TiDBIP}:10080/tables/{db}/{table}/regions
func GetRegions(config TiDBConfig, dbName, tableName string) (*RegionInfos, error) {
	path := fmt.Sprintf("tables/%s/%s/regions", dbName, tableName)

	body, err := util.HttpGet(config.Server, config.Port, path)
	if err != nil {
		return nil, err
	}

	info := &server.TableRegions{}
	if err = json.Unmarshal(body, info); err != nil {
		return nil, err
	}

	ids := make([]uint64, len(info.RecordRegions))
	stream.FromArray(info.RecordRegions).Map(func(info interface{}) interface{} {
		return info.(server.RegionMeta).ID
	}).CollectTo(ids)

	return &RegionInfos{
		TableID:   info.TableID,
		RegionIDs: ids,
	}, nil
}
