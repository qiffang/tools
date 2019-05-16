package main

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/config"
	"github.com/prometheus/common/log"
	"github.com/qiffang/tools/client"
	"github.com/qiffang/tools/meta"
	"github.com/wushilin/stream"
)

func main() {
	conf := meta.TiDBConfig{
		Server: "127.0.0.1",
		Port:   10080,
	}

	regions, err := meta.GetRegions(conf, "test", "t")
	if err != nil {
		log.Error(err)
		return
	}
	log.Info("Table Info ", fmt.Sprintf("ID=%d", regions.TableID))

	c, err := client.NewClient([]string{"127.0.0.1:2379"}, config.Security{})
	if err != nil {
		log.Error(err)
		return
	}

	ctx := context.Background()
	for _, id := range regions.RegionIDs {
		log.Info("Region Info ", fmt.Sprintf("ID=%d", id))
		_, err := c.GetRegionInfo(ctx, id)
		if err != nil {
			log.Error(err)
			continue
		}

		kr := meta.GetKeyRange(c, id)
		c.Scan(kr.Startkey, kr.EndKey)
	}
	stream.FromArray(regions.RegionIDs).Each(func(id interface{}) {

	})

}
