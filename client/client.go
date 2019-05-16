package client

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"github.com/prometheus/common/log"
	"github.com/qiffang/tools/util"
	"math"
	"strings"

	//"github.com/pingcap/log"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/config"
	kvstore "github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"time"
)

const (
	readTimeout = 20 * time.Second
)

// Client is a client that sends RPC.
type ClusterClient struct {
	PdClient    pd.Client
	RpcClient   *rpcClient
	RegionCache *tikv.RegionCache
	Storage     kv.Storage
}

type RegionMeta struct {
	Region *metapb.Region
	Peer   *metapb.Peer
}

// NewRawKVClient creates a client with PD cluster addrs.
func NewClient(pdAddrs []string, security config.Security) (*ClusterClient, error) {
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	})
	if err != nil {
		return nil, err
	}

	kvstore.Register("tikv", tikv.Driver{})
	fullPath := fmt.Sprintf(fmt.Sprintf("tikv://%s?disableGC=true", strings.Join(pdAddrs, ",")))
	storage, err := kvstore.New(fullPath)

	if err != nil {
		return nil, err
	}

	return &ClusterClient{
		PdClient:    pdCli,
		RegionCache: tikv.NewRegionCache(pdCli),
		RpcClient:   newRPCClient(security),
		Storage:     storage,
	}, nil
}

func (c *ClusterClient) GetRegionInfo(ctx context.Context, id uint64) (*tikv.KeyLocation, error) {
	return c.RegionCache.LocateRegionByID(NewBackOffer(ctx), id)
}

func NewBackOffer(ctx context.Context) *tikv.Backoffer {
	return tikv.NewBackoffer(ctx, 20000)
}

func (c *ClusterClient) GetRegion(id uint64) (*RegionMeta, error) {
	r, peer, err := c.PdClient.GetRegionByID(getContext(), id)
	if err != nil {
		return nil, err
	}

	return &RegionMeta{
		Region: r,
		Peer:   peer,
	}, nil
}

func getContext() context.Context {
	return context.Background()
}

func (c *ClusterClient) Scan(start, end []byte) {
	snapshot, err := c.Storage.GetSnapshot(kv.Version{
		Ver: math.MaxInt64,
	})

	if err != nil {
		log.Error(err)
		return
	}

	it, err := snapshot.Iter(start, end)
	defer it.Close()

	for it.Valid() {
		log.Info("Snapshot", util.Escape(it.Key()))
		it.Next()
	}
	//store, _ := c.PdClient.GetStore(ctx, storeId)
	//
	//store.Sn
}

//func (c *ClusterClient) ScanByRange(startKey, endKey []byte) {
//	loc, err := c.regionCache.LocateKey(bo, startKey)
//}

// Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs.
func (c *ClusterClient) ScanByRegion(ctx context.Context, tableID int64, location *tikv.KeyLocation, limit uint32) (*tikvrpc.Response, error) {

	//c.PdClient.
	r, peer, err := c.PdClient.GetRegionByID(ctx, 3)

	if err != nil {
		return nil, err
	}

	//storeId := peer.StoreId
	bo := tikv.NewBackoffer(context.Background(), 20000)

	for {
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdRawScan,
			RawScan: &kvrpcpb.RawScanRequest{
				StartKey: r.StartKey,
				EndKey:   nil,
				//tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(tableID), math.MaxInt64).PrefixNext(),
				Limit:   limit,
				KeyOnly: false,
				//Version: uint64(time.Now().Unix()),
				Context: &kvrpcpb.Context{
					RegionId: r.GetId(),
					Peer:     peer,
					//ScanDetail:  true,
					RegionEpoch: r.RegionEpoch,
				},

				//Version:  s.startTS(),
				//KeyOnly:  s.snapshot.keyOnly,
			},
		}

		req.Context = kvrpcpb.Context{
			RegionId:   r.GetId(),
			Peer:       peer,
			ScanDetail: true,
		}

		//resp, err := sender.SendReq(bo, req, location.Region, readTimeout)

		addr, err := c.loadStoreAddr(ctx, bo, peer.StoreId)

		resp, err := c.RpcClient.SendRequest(ctx, addr, req, readTimeout)

		if err != nil {
			return nil, errors.Trace(err)
		}
		//regionErr, err := resp.GetRegionError()
		//if err != nil {
		//	return nil, errors.Trace(err)
		//}

		//need to check in the feature
		//if regionErr != nil {
		//	return nil, errors.Trace(err)
		//	//err := bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
		//	//if err != nil {
		//	//	return nil, errors.Trace(err)
		//	//}
		//	//continue
		//}

		return resp, nil
	}
}

func (c *ClusterClient) loadStoreAddr(ctx context.Context, bo *tikv.Backoffer, id uint64) (string, error) {
	for {
		store, err := c.PdClient.GetStore(ctx, id)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return "", err
			}
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", id, err)
			if err = bo.Backoff(tikv.BoPDRPC, err); err != nil {
				return "", errors.Trace(err)
			}
			continue
		}
		if store == nil {
			return "", nil
		}
		return store.GetAddress(), nil
	}
}
