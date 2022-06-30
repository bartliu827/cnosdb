// Package snapshotter provides the meta snapshot service.
package snapshotter

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/json"
	errors2 "errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/pkg/errors"
	"github.com/cnosdb/cnosdb/pkg/escape"
	"github.com/cnosdb/cnosdb/pkg/network"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	"github.com/cnosdb/cnosdb/vend/db/tsdb/engine/tsm1"

	"go.uber.org/zap"
)

const (
	// MuxHeader is the header byte used for the TCP muxer.
	MuxHeader = "snapshotter"

	// BackupMagicHeader is the first 8 bytes used to identify and validate
	// a metastore backup file
	BackupMagicHeader = 0x6b6d657461 //kmeta
)

// Service manages the listener for the snapshot endpoint.
type Service struct {
	wg sync.WaitGroup

	Node *meta.Node

	MetaClient interface {
		encoding.BinaryMarshaler
		Database(name string) *meta.DatabaseInfo
		Data() meta.Data
		SetData(data *meta.Data) error
		TruncateShardGroups(t time.Time) error
		UpdateShardOwners(shardID uint64, addOwners []uint64, delOwners []uint64) error
		ShardOwner(shardID uint64) (database, rp string, sgi *meta.ShardGroupInfo)
	}

	TSDBStore interface {
		BackupShard(id uint64, since time.Time, w io.Writer) error
		ExportShard(id uint64, ExportStart time.Time, ExportEnd time.Time, w io.Writer) error
		Shard(id uint64) *tsdb.Shard
		ShardRelativePath(id uint64) (string, error)
		SetShardEnabled(shardID uint64, enabled bool) error
		RestoreShard(id uint64, r io.Reader) error
		CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error
		DeleteShard(shardID uint64) error
	}

	Listener      net.Listener
	Logger        *zap.Logger
	CopyingShards map[string]*Record
}

type Record struct {
	quit          chan int
	copyShardInfo *CopyShardInfo
	wc            *WriteCounter
}

type WriteCounter struct {
	CurrentSize uint64
}

func (wc *WriteCounter) Write(p []byte) (int, error) {
	n := len(p)
	wc.CurrentSize += uint64(n)
	return n, nil
}

// NewService returns a new instance of Service.
func NewService() *Service {
	return &Service{
		Logger:        zap.NewNop(),
		CopyingShards: make(map[string]*Record),
	}
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting snapshot service")

	s.wg.Add(1)
	go s.serve()
	return nil
}

// Close implements the Service interface.
func (s *Service) Close() error {
	if s.Listener != nil {
		if err := s.Listener.Close(); err != nil {
			return err
		}
	}
	s.wg.Wait()
	return nil
}

// Check checks if the shards has the same value in different servers in a cluster.
// return time slot (startTime,endTime) array that shard has diff values.
func (s *Service) Check(shardID uint64, interval int64) ([][]int64, error) {
	s.Logger.Info("1111")
	data := s.MetaClient.Data()
	_, _, si := data.ShardDBRetentionAndInfo(shardID)
	nodeList := make([]uint64, 0)
	//get NodeIDs
	for _, owner := range si.Owners {
		nodeList = append(nodeList, owner.NodeID)
	}
	//get node_address
	nodeAddrList := make([]string, 0)
	for _, nid := range nodeList {
		nodeAddrList = append(nodeAddrList, data.DataNode(nid).TCPHost)
	}
	//get shard startTime, endTime, and their hash values
	localHash := make([]uint64, 0)
	_, _, sg := s.MetaClient.ShardOwner(shardID)
	start := sg.StartTime.UnixNano()
	end := sg.EndTime.UnixNano()

	s.Logger.Info(fmt.Sprintf("%d: %d", start, end))

	s.Logger.Error("22222")

	for i := start; i < end; i += interval {
		//s.Logger.Info("33333")
		//TODO: get data from shard
		err := s.DumpShard2ProtocolLine(shardID, i, i+interval-1)
		if err != nil {
			return nil, err
		}
		//s.Logger.Info("44444")
		d, err := ioutil.ReadFile("/Users/cnosdb/" + strconv.Itoa(int(shardID)) + ".txt")
		if err != nil {
			return nil, err
		}
		fnv64Hash := fnv.New64()
		fnv64Hash.Write(d)
		h := fnv64Hash.Sum64()
		//s.Logger.Info("555555")
		//s.Logger.Info("hash:" + strconv.FormatUint(h, 10))
		localHash = append(localHash, h)
	}
	fmt.Printf("localhash:")
	fmt.Println(localHash)

	otherHashs := make(map[string][]uint64, 0)
	for _, addr := range nodeAddrList {
		conn, err := network.Dial("tcp", addr, MuxHeader)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		request := &Request{
			Type:     RequestShardIntervalHash,
			ShardID:  shardID,
			Interval: interval,
		}

		_, err = conn.Write([]byte{byte(request.Type)})
		if err != nil {
			return nil, err
		}

		// Write the request
		if err := json.NewEncoder(conn).Encode(request); err != nil {
			return nil, fmt.Errorf("encode snapshot request: %s", err)
		}

		var resp Response
		// Read the response
		if err := json.NewDecoder(conn).Decode(&resp); err != nil {
			return nil, err
		}

		otherHashs[addr] = resp.Hash
	}
	fmt.Printf("others hash:")
	fmt.Println(otherHashs)

	result := make([][]int64, 0)
	//validate the hash values, find out the diff, add the time duration into result
	for idx, val := range localHash {
		for _, v := range otherHashs {
			if val != v[idx] {
				st := start + int64(idx)*interval
				et := st + interval
				result = append(result, []int64{st, et})
			}
		}
	}

	return result, nil
}

//func (s *Service) getNodeIdListByShardID(shardID uint64) []uint64 {
//	data := s.MetaClient.Data()
//	nodeIdList := make([]uint64, 0)
//	for dbidx, dbi := range data.Databases {
//		for rpidx, rpi := range dbi.RetentionPolicies {
//			for sgidx, rg := range rpi.ShardGroups {
//				for sidx, s := range rg.Shards {
//					if s.ID == shardID {
//						for _, nid := range data.Databases[dbidx].RetentionPolicies[rpidx].ShardGroups[sgidx].Shards[sidx].Owners {
//							nodeIdList = append(nodeIdList, nid.NodeID)
//						}
//						return nodeIdList
//					}
//				}
//			}
//		}
//	}
//	return nil
//}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "snapshot"))
}

// serve serves snapshot requests from the listener.
func (s *Service) serve() {
	defer s.wg.Done()

	s.Logger.Info("snapshotter service start")

	for {
		// Wait for next connection.
		conn, err := s.Listener.Accept()
		if err != nil && strings.Contains(err.Error(), "connection closed") {
			s.Logger.Info("Listener closed")
			return
		} else if err != nil {
			s.Logger.Info("Error accepting snapshot request", zap.Error(err))
			continue
		}

		// Handle connection in separate goroutine.
		s.wg.Add(1)
		go func(conn net.Conn) {
			defer s.wg.Done()
			defer conn.Close()
			if err := s.handleConn(conn); err != nil {
				s.Logger.Info("snapshotter service handle conn error", zap.Error(err))
			}
		}(conn)
	}
}

// handleConn processes conn. This is run in a separate goroutine.
func (s *Service) handleConn(conn net.Conn) error {
	var typ [1]byte

	if _, err := io.ReadFull(conn, typ[:]); err != nil {
		return err
	}

	if RequestType(typ[0]) == RequestShardUpdate {
		return s.updateShardsLive(conn)
	}

	r, bytes, err := s.readRequest(conn)
	if err != nil {
		return fmt.Errorf("read request: %s", err)
	}

	switch RequestType(typ[0]) {
	case RequestShardBackup:
		if err := s.TSDBStore.BackupShard(r.ShardID, r.Since, conn); err != nil {
			return err
		}
	case RequestShardExport:
		if err := s.TSDBStore.ExportShard(r.ShardID, r.ExportStart, r.ExportEnd, conn); err != nil {
			return err
		}
	case RequestMetastoreBackup:
		if err := s.writeMetaStore(conn); err != nil {
			return err
		}
	case RequestDatabaseInfo:
		return s.writeDatabaseInfo(conn, r.BackupDatabase)
	case RequestRetentionPolicyInfo:
		return s.writeRetentionPolicyInfo(conn, r.BackupDatabase, r.BackupRetentionPolicy)
	case RequestMetaStoreUpdate:
		return s.updateMetaStore(conn, bytes, r.BackupDatabase, r.RestoreDatabase, r.BackupRetentionPolicy, r.RestoreRetentionPolicy)
	case RequestCopyShard:
		return s.copyShardToDest(conn, r.CopyShardDestHost, r.ShardID)
	case RequestRemoveShard:
		return s.removeShardCopy(conn, r.ShardID)
	case RequestCopyShardStatus:
		return s.copyShardStatus(conn)
	case RequestTruncateShards:
		return s.truncateShardGroups(conn, r.DelaySecond)
	case RequestKillCopyShard:
		return s.killCopyShard(conn, r.CopyShardDestHost, r.ShardID)
	case RequestShardIntervalHash:
		return s.getShardIntervalHash(conn, r.ShardID, r.Interval)
	default:
		return fmt.Errorf("snapshotter request type unknown: %v", r.Type)
	}

	return nil
}

func (s *Service) sendConn(conn net.Conn) {

}

func (s *Service) updateShardsLive(conn net.Conn) error {
	var sidBytes [8]byte
	if _, err := io.ReadFull(conn, sidBytes[:]); err != nil {
		return err
	}
	sid := binary.BigEndian.Uint64(sidBytes[:])

	metaData := s.MetaClient.Data()
	dbName, rp, _ := metaData.ShardDBRetentionAndInfo(sid)
	if err := s.TSDBStore.CreateShard(dbName, rp, sid, true); err != nil {
		return err
	}

	if err := s.TSDBStore.SetShardEnabled(sid, false); err != nil {
		return err
	}
	defer s.TSDBStore.SetShardEnabled(sid, true)

	return s.TSDBStore.RestoreShard(sid, conn)
}

func (s *Service) updateMetaStore(conn net.Conn, bits []byte, backupDBName, restoreDBName, backupRPName, restoreRPName string) error {
	md := meta.Data{}
	err := md.UnmarshalBinary(bits)
	if err != nil {
		if err := s.respondIDMap(conn, map[uint64]uint64{}); err != nil {
			return err
		}
		return fmt.Errorf("failed to decode meta: %s", err)
	}

	data := s.MetaClient.Data()

	IDMap, newDBs, err := data.ImportData(md, backupDBName, restoreDBName, backupRPName, restoreRPName)
	if err != nil {
		if err := s.respondIDMap(conn, map[uint64]uint64{}); err != nil {
			return err
		}
		return err
	}

	err = s.MetaClient.SetData(&data)
	if err != nil {
		return err
	}

	err = s.createNewDBShards(data, newDBs)
	if err != nil {
		return err
	}

	err = s.respondIDMap(conn, IDMap)
	return err
}

// copy a shard to remote host
func (s *Service) copyShardToDest(conn net.Conn, destHost string, shardID uint64) error {
	localAddr := s.Listener.Addr().String()
	s.Logger.Info("copy shard command ",
		zap.String("Local", localAddr),
		zap.String("Dest", destHost),
		zap.Uint64("ShardID", shardID))

	data := s.MetaClient.Data()
	info := data.DataNodeByAddr(destHost)
	if info == nil {
		io.WriteString(conn, "Can't found data node: "+destHost)
		return nil
	}

	dbName, rp, shardInfo := data.ShardDBRetentionAndInfo(shardID)

	if !shardInfo.OwnedBy(s.Node.ID) {
		io.WriteString(conn, "Can't found shard in: "+localAddr)
		return nil
	}

	if shardInfo.OwnedBy(info.ID) {
		io.WriteString(conn, fmt.Sprintf("Shard %d already in %s", shardID, destHost))
		return nil
	}

	key := fmt.Sprintf("%s_%s_%d", localAddr, destHost, shardID)
	if _, ok := s.CopyingShards[key]; ok {
		io.WriteString(conn, fmt.Sprintf("The Shard %d from %s to %s is copying, please wait", shardID, localAddr, destHost))
		return nil
	}

	copyShardInfo := &CopyShardInfo{
		ShardID:   shardID,
		SrcHost:   localAddr,
		DestHost:  destHost,
		Database:  dbName,
		Retention: rp,
		Status:    "copying",
		StartTime: time.Now(),
	}

	quit := make(chan int, 1)

	go func(quit chan int) {
		reader, writer := io.Pipe()
		defer reader.Close()

		counter := &WriteCounter{}
		teeReader := io.TeeReader(reader, counter)
		s.CopyingShards[key] = &Record{quit: quit, copyShardInfo: copyShardInfo, wc: counter}

		go func() {
			defer writer.Close()
			if err := s.TSDBStore.BackupShard(shardID, time.Time{}, writer); err != nil {
				s.Logger.Error("Error backup shard", zap.Error(err))
			}
		}()
		go func() {
			tr := tar.NewReader(teeReader)
			client := NewClient(destHost)
			if err := client.UploadShard(shardID, shardID, dbName, rp, tr); err != nil {
				s.Logger.Error("Error upload shard", zap.Error(err))
				return
			}

			if err := s.MetaClient.UpdateShardOwners(shardID, []uint64{info.ID}, nil); err != nil {
				s.Logger.Error("Error update owner", zap.Error(err))
				return
			}

			s.Logger.Info("Success Copy Shard ", zap.Uint64("ShardID", shardID), zap.String("Host", destHost))
			delete(s.CopyingShards, key)
			quit <- 1
			close(quit)
		}()

		select {
		case _, ok := <-quit:
			s.Logger.Info("receive a quit single", zap.Bool("Exit normally", ok), zap.Uint64("ShardID", shardID), zap.String("Host", destHost))
			return
		}

	}(quit)

	io.WriteString(conn, "Copying ......")

	return nil
}

// show all copy shard that it is running
func (s *Service) copyShardStatus(conn net.Conn) error {
	localAddr := s.Listener.Addr().String()
	s.Logger.Info("copy shard status command ", zap.String("Local", localAddr))

	infos := make([]CopyShardInfo, 0)
	for _, record := range s.CopyingShards {
		record.copyShardInfo.CopiedSize = record.wc.CurrentSize
		infos = append(infos, *(record.copyShardInfo))
	}

	infoJson, _ := json.Marshal(infos)
	io.WriteString(conn, string(infoJson))
	s.Logger.Error("111111")
	_, err := s.Check(4, 86400000000000)
	fmt.Println(err)
	if err != nil {
		return err
	}
	return nil
}

// kill a copy-shard command
func (s *Service) killCopyShard(conn net.Conn, destHost string, shardID uint64) error {
	localAddr := s.Listener.Addr().String()
	s.Logger.Info("kill copy shard command ",
		zap.String("Local", localAddr),
		zap.String("Dest", destHost),
		zap.Uint64("ShardID", shardID))

	data := s.MetaClient.Data()
	info := data.DataNodeByAddr(destHost)
	if info == nil {
		io.WriteString(conn, "Can't found data node: "+destHost)
		return nil
	}

	key := fmt.Sprintf("%s_%s_%d", localAddr, destHost, shardID)
	if _, ok := s.CopyingShards[key]; !ok {
		io.WriteString(conn, fmt.Sprintf("The Copy Shard %d from %s to %s is not exist", shardID, localAddr, destHost))
		return nil
	}

	record := s.CopyingShards[key]
	if record == nil {
		io.WriteString(conn, fmt.Sprintf("The Copy Shard %d from %s to %s is not exist or it's finished", shardID, localAddr, destHost))
		return nil
	}

	close(record.quit)

	delete(s.CopyingShards, key)

	request := &Request{
		Type:    RequestRemoveShard,
		ShardID: shardID,
	}

	destconn, err := network.Dial("tcp", destHost, MuxHeader)
	if err != nil {
		return err
	}
	defer destconn.Close()

	_, err = destconn.Write([]byte{byte(request.Type)})
	if err != nil {
		return err
	}

	if err := json.NewEncoder(destconn).Encode(request); err != nil {
		return fmt.Errorf("encode snapshot request: %s", err)
	}

	bytes, err := ioutil.ReadAll(destconn)
	if string(bytes) != "Success " || err != nil {
		io.WriteString(conn, "The Copy Shard is killed, but delete shard in the destHost failed: "+err.Error())
		return nil
	}

	io.WriteString(conn, "Kill Copy Shard Succeeded")

	return nil
}

// remove a shard replication
func (s *Service) removeShardCopy(conn net.Conn, shardID uint64) error {
	localAddr := s.Listener.Addr().String()
	s.Logger.Info("remove shard command ",
		zap.String("Local", localAddr),
		zap.Uint64("ShardID", shardID))

	if err := s.TSDBStore.DeleteShard(shardID); err != nil {
		io.WriteString(conn, err.Error())
		return err
	}

	if err := s.MetaClient.UpdateShardOwners(shardID, nil, []uint64{s.Node.ID}); err != nil {
		io.WriteString(conn, err.Error())
		return err
	}
	io.WriteString(conn, "Success ")
	return nil
}

// remove a shard replication
func (s *Service) truncateShardGroups(conn net.Conn, delaySecond int) error {
	localAddr := s.Listener.Addr().String()
	s.Logger.Info("truncate shards command ",
		zap.String("Local", localAddr),
		zap.Int("Delay second", delaySecond))

	timestamp := time.Now().Add(time.Duration(delaySecond) * time.Second).UTC()
	if err := s.MetaClient.TruncateShardGroups(timestamp); err != nil {
		io.WriteString(conn, err.Error())
		return err
	}
	io.WriteString(conn, "Success ")
	return nil
}

// iterate over a list of newDB's that should have just been added to the metadata
// If the db was not created in the metadata return an error.
// None of the shards should exist on a new DB, and CreateShard protects against double-creation.
func (s *Service) createNewDBShards(data meta.Data, newDBs []string) error {
	for _, restoreDBName := range newDBs {
		dbi := data.Database(restoreDBName)
		if dbi == nil {
			return fmt.Errorf("db %s not found when creating new db shards", restoreDBName)
		}
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, shard := range sgi.Shards {
					err := s.TSDBStore.CreateShard(restoreDBName, rpi.Name, shard.ID, true)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// send the IDMapping based on the metadata from the source server vs the shard ID
// metadata on this server.  Sends back [BackupMagicHeader,0] if there's no mapped
// values, signaling that nothing should be imported.
func (s *Service) respondIDMap(conn net.Conn, IDMap map[uint64]uint64) error {
	npairs := len(IDMap)
	// 2 information ints, then npairs of 8byte ints.
	numBytes := make([]byte, (npairs+1)*16)

	binary.BigEndian.PutUint64(numBytes[:8], BackupMagicHeader)
	binary.BigEndian.PutUint64(numBytes[8:16], uint64(npairs))
	next := 16
	for k, v := range IDMap {
		binary.BigEndian.PutUint64(numBytes[next:next+8], k)
		binary.BigEndian.PutUint64(numBytes[next+8:next+16], v)
		next += 16
	}

	_, err := conn.Write(numBytes[:])
	return err
}

func (s *Service) writeMetaStore(conn net.Conn) error {
	// Retrieve and serialize the current meta data.
	metaBlob, err := s.MetaClient.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal meta: %s", err)
	}

	var nodeBytes bytes.Buffer
	if err := json.NewEncoder(&nodeBytes).Encode(s.Node); err != nil {
		return err
	}

	var numBytes [24]byte

	binary.BigEndian.PutUint64(numBytes[:8], BackupMagicHeader)
	binary.BigEndian.PutUint64(numBytes[8:16], uint64(len(metaBlob)))
	binary.BigEndian.PutUint64(numBytes[16:24], uint64(nodeBytes.Len()))

	// backup header followed by meta blob length
	if _, err := conn.Write(numBytes[:16]); err != nil {
		return err
	}

	if _, err := conn.Write(metaBlob); err != nil {
		return err
	}

	if _, err := conn.Write(numBytes[16:24]); err != nil {
		return err
	}

	if _, err := nodeBytes.WriteTo(conn); err != nil {
		return err
	}
	return nil
}

// writeDatabaseInfo will write the relative paths of all shards in the database on
// this server into the connection.
func (s *Service) writeDatabaseInfo(conn net.Conn, database string) error {
	res := Response{}
	dbs := []meta.DatabaseInfo{}
	if database != "" {
		db := s.MetaClient.Database(database)
		if db == nil {
			return errors.ErrDatabaseNotFound(database)
		}
		dbs = append(dbs, *db)
	} else {
		// we'll allow collecting info on all databases
		dbs = s.MetaClient.Data().Databases
	}

	for _, db := range dbs {
		for _, rp := range db.RetentionPolicies {
			for _, sg := range rp.ShardGroups {
				for _, sh := range sg.Shards {
					// ignore if the shard isn't on the server
					if s.TSDBStore.Shard(sh.ID) == nil {
						continue
					}

					path, err := s.TSDBStore.ShardRelativePath(sh.ID)
					if err != nil {
						return err
					}

					res.Paths = append(res.Paths, path)
				}
			}
		}
	}
	if err := json.NewEncoder(conn).Encode(res); err != nil {
		return fmt.Errorf("encode response: %s", err.Error())
	}

	return nil
}

// writeDatabaseInfo will write the relative paths of all shards in the retention policy on
// this server into the connection
func (s *Service) writeRetentionPolicyInfo(conn net.Conn, database, retentionPolicy string) error {
	res := Response{}
	db := s.MetaClient.Database(database)
	if db == nil {
		return errors.ErrDatabaseNotFound(database)
	}

	var ret *meta.RetentionPolicyInfo

	for _, rp := range db.RetentionPolicies {
		if rp.Name == retentionPolicy {
			ret = &rp
			break
		}
	}

	if ret == nil {
		return errors.ErrRetentionPolicyNotFound(retentionPolicy)
	}

	for _, sg := range ret.ShardGroups {
		for _, sh := range sg.Shards {
			// ignore if the shard isn't on the server
			if s.TSDBStore.Shard(sh.ID) == nil {
				continue
			}

			path, err := s.TSDBStore.ShardRelativePath(sh.ID)
			if err != nil {
				return err
			}

			res.Paths = append(res.Paths, path)
		}
	}

	if err := json.NewEncoder(conn).Encode(res); err != nil {
		return fmt.Errorf("encode resonse: %s", err.Error())
	}

	return nil
}

func (s *Service) getShardIntervalHash(conn net.Conn, shardID uint64, interval int64) error {
	s.Logger.Info("get the request msg")

	res := Response{}
	//获取shqrd段的hash值
	//data := s.MetaClient.Data()

	_, _, sg := s.MetaClient.ShardOwner(shardID)
	start := sg.StartTime.UnixNano()
	end := sg.EndTime.UnixNano()

	for i := start; i < end; i += interval {
		//TODO: get data from shard
		err := s.DumpShard2ProtocolLine(shardID, i, i+interval-1)
		if err != nil {
			return err
		}
		data, err := ioutil.ReadFile("/Users/cnosdb/" + strconv.Itoa(int(shardID)) + ".txt")
		if err != nil {
			return err
		}

		fnv64Hash := fnv.New64()
		fnv64Hash.Write(data)
		h := fnv64Hash.Sum64()
		res.Hash = append(res.Hash, h)
	}
	s.Logger.Info("return the hash value")
	//s.Logger.Info(fmt.Sprint(res))
	if err := json.NewEncoder(conn).Encode(res); err != nil {
		return fmt.Errorf("encode resonse: %s", err.Error())
	}

	return nil
}

// readRequest unmarshals a request object from the conn.
func (s *Service) readRequest(conn net.Conn) (Request, []byte, error) {
	var r Request
	d := json.NewDecoder(conn)

	if err := d.Decode(&r); err != nil {
		return r, nil, err
	}

	bits := make([]byte, r.UploadSize+1)

	if r.UploadSize > 0 {

		remainder := d.Buffered()

		n, err := remainder.Read(bits)
		if err != nil && err != io.EOF {
			return r, bits, err
		}

		// it is a bit random but sometimes the Json decoder will consume all the bytes and sometimes
		// it will leave a few behind.
		if err != io.EOF && n < int(r.UploadSize+1) {
			_, err = conn.Read(bits[n:])
		}

		if err != nil && err != io.EOF {
			return r, bits, err
		}
		// the JSON encoder on the client side seems to write an extra byte, so trim that off the front.
		return r, bits[1:], nil
	}

	return r, bits, nil
}

// RequestType indicates the typeof snapshot request.
type RequestType uint8

const (
	// RequestShardBackup represents a request for a shard backup.
	RequestShardBackup RequestType = iota

	// RequestMetastoreBackup represents a request to back up the metastore.
	RequestMetastoreBackup

	// RequestSeriesFileBackup represents a request to back up the database series file.
	RequestSeriesFileBackup

	// RequestDatabaseInfo represents a request for database info.
	RequestDatabaseInfo

	// RequestRetentionPolicyInfo represents a request for retention policy info.
	RequestRetentionPolicyInfo

	// RequestShardExport represents a request to export Shard data.  Similar to a backup, but shards
	// may be filtered based on the start/end times on each block.
	RequestShardExport

	// RequestMetaStoreUpdate represents a request to upload a metafile that will be used to do a live update
	// to the existing metastore.
	RequestMetaStoreUpdate

	// RequestShardUpdate will initiate the upload of a shard data tar file
	// and have the engine import the data.
	RequestShardUpdate

	// RequestCopyShard represents a request for copy a shard to dest host
	RequestCopyShard

	// RequestRemoveShard represents a request for remove a shard copy
	RequestRemoveShard

	RequestCopyShardStatus
	RequestKillCopyShard
	RequestTruncateShards

	RequestShardIntervalHash
)

// Request represents a request for a specific backup or for information
// about the shards on this server for a database or retention policy.
type Request struct {
	Type                   RequestType
	CopyShardDestHost      string
	BackupDatabase         string
	RestoreDatabase        string
	BackupRetentionPolicy  string
	RestoreRetentionPolicy string
	ShardID                uint64
	Since                  time.Time
	ExportStart            time.Time
	ExportEnd              time.Time
	UploadSize             int64
	DelaySecond            int
	Interval               int64
}

// Response contains the relative paths for all the shards on this server
// that are in the requested database or retention policy.
type Response struct {
	Paths []string
	Hash  []uint64
}

type CopyShardInfo struct {
	ShardID    uint64
	SrcHost    string
	DestHost   string
	Database   string
	Retention  string
	Status     string
	StartTime  time.Time
	CopiedSize uint64
}

type DumpShard struct {
	dataDir   string
	walDir    string
	out       string
	startTime int64
	endTime   int64

	manifest struct{}
	tsmFiles []string
	walFiles []string
}

func newDumpShard(shard *tsdb.Shard, relPath string, start, end int64) DumpShard {
	//out := shard.Path()[0 : len(shard.Path())-len(relPath)-5]
	return DumpShard{
		dataDir:   shard.Path(),
		walDir:    shard.WalPath(),
		out:       "/Users/cnosdb/" + strconv.Itoa(int(shard.ID())) + ".txt",
		startTime: start,
		endTime:   end,

		tsmFiles: make([]string, 0),
		walFiles: make([]string, 0),
	}
}

func (s *Service) DumpShard2ProtocolLine(shardID uint64, start, end int64) error {
	shard := s.TSDBStore.Shard(shardID)
	if shard == nil {
		return errors2.New("the shard isn't exist")
	}
	p, err := s.TSDBStore.ShardRelativePath(shardID)
	if err != nil {
		return err
	}

	dumpShard := newDumpShard(shard, p, start, end)
	s.Logger.Info("NewDumpShard", zap.String("dataDir", dumpShard.dataDir), zap.String("walDir", dumpShard.walDir), zap.String("out", dumpShard.out))
	if err := dumpShard.walkTSMFiles(); err != nil {
		return err
	}
	if err := dumpShard.walkWALFiles(); err != nil {
		return err
	}
	s.Logger.Info("walkFiles", zap.Any("manifest", dumpShard.manifest), zap.Any("TSM", dumpShard.tsmFiles), zap.Any("WAL", dumpShard.walFiles))
	return dumpShard.write()
}

func (d *DumpShard) walkTSMFiles() error {
	return filepath.Walk(d.dataDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a tsm file
		if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
			return nil
		}

		d.manifest = struct{}{}
		d.tsmFiles = append(d.tsmFiles, path)
		return nil
	})
}

func (d *DumpShard) walkWALFiles() error {
	return filepath.Walk(d.walDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a wal file
		fileName := filepath.Base(path)
		if filepath.Ext(path) != "."+tsm1.WALFileExtension || !strings.HasPrefix(fileName, tsm1.WALFilePrefix) {
			return nil
		}

		d.manifest = struct{}{}
		d.walFiles = append(d.walFiles, path)
		return nil
	})
}

func (d *DumpShard) write() error {
	// open our output file and create an output buffer
	f, err := os.Create(d.out)
	if err != nil {
		return err
	}
	defer f.Close()

	// Because calling (*os.File).Write is relatively expensive,
	// and we don't *need* to sync to disk on every written line of export,
	// use a sized buffered writer so that we only sync the file every megabyte.
	bw := bufio.NewWriterSize(f, 1024*1024)
	defer bw.Flush()

	var w io.Writer = bw

	s, e := time.Unix(0, d.startTime).Format(time.RFC3339), time.Unix(0, d.endTime).Format(time.RFC3339)
	fmt.Fprintf(w, "# CNOSDB EXPORT: %s - %s\n", s, e)

	fmt.Fprintln(w, "# DML")
	files := d.tsmFiles
	if err := d.writeTsmFiles(w, files); err != nil {
		return err
	}

	if err := d.writeWALFiles(w, d.walFiles); err != nil {
		return err
	}

	return nil
}

func (d *DumpShard) writeTsmFiles(w io.Writer, files []string) error {
	fmt.Fprintln(w, "# writing tsm data")

	// we need to make sure we write the same order that the files were written
	sort.Strings(files)

	for _, f := range files {
		if err := d.exportTSMFile(f, w); err != nil {
			return err
		}
	}

	return nil
}

func (d *DumpShard) exportTSMFile(tsmFilePath string, w io.Writer) error {
	f, err := os.Open(tsmFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(w, "skipped missing file: %s", tsmFilePath)
			return nil
		}
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		return err
	}
	defer r.Close()
	if sgStart, sgEnd := r.TimeRange(); sgStart > d.endTime || sgEnd < d.startTime {
		return nil
	}
	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(key)
		if err != nil {
			continue
		}
		measurement, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		field = escape.Bytes(field)

		if err := d.writeValues(w, measurement, string(field), values); err != nil {
			// An error from writeValues indicates an IO error, which should be returned.
			return err
		}
	}
	return nil
}

func (d *DumpShard) writeWALFiles(w io.Writer, files []string) error {
	fmt.Fprintln(w, "# writing wal data")

	// we need to make sure we write the same order that the wal received the data
	sort.Strings(files)

	for _, f := range files {
		if err := d.exportWALFile(f, w); err != nil {
			return err
		}
	}

	return nil
}

// exportWAL reads every WAL entry from r and exports it to w.
func (d *DumpShard) exportWALFile(walFilePath string, w io.Writer) error {
	f, err := os.Open(walFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(w, "skipped missing file: %s", walFilePath)
			return nil
		}
		return err
	}
	defer f.Close()

	r := tsm1.NewWALSegmentReader(f)
	defer r.Close()

	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			break
		}

		switch t := entry.(type) {
		case *tsm1.DeleteWALEntry, *tsm1.DeleteRangeWALEntry:
			continue
		case *tsm1.WriteWALEntry:
			for key, values := range t.Values {
				measurement, field := tsm1.SeriesAndFieldFromCompositeKey([]byte(key))
				// measurements are stored escaped, field names are not
				field = escape.Bytes(field)

				if err := d.writeValues(w, measurement, string(field), values); err != nil {
					// An error from writeValues indicates an IO error, which should be returned.
					return err
				}
			}
		}
	}
	return nil
}

// writeValues writes every value in values to w, using the given series key and field name.
// If any call to w.Write fails, that error is returned.
func (d *DumpShard) writeValues(w io.Writer, seriesKey []byte, field string, values []tsm1.Value) error {
	buf := []byte(string(seriesKey) + " " + field + "=")
	prefixLen := len(buf)

	for _, value := range values {
		ts := value.UnixNano()
		if (ts < d.startTime) || (ts > d.endTime) {
			continue
		}

		// Re-slice buf to be "<series_key> <field>=".
		buf = buf[:prefixLen]

		// Append the correct representation of the value.
		switch v := value.Value().(type) {
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
			buf = append(buf, 'i')
		case uint64:
			buf = strconv.AppendUint(buf, v, 10)
			buf = append(buf, 'u')
		case bool:
			buf = strconv.AppendBool(buf, v)
		case string:
			buf = append(buf, '"')
			buf = append(buf, models.EscapeStringField(v)...)
			buf = append(buf, '"')
		default:
			// This shouldn't be possible, but we'll format it anyway.
			buf = append(buf, fmt.Sprintf("%v", v)...)
		}

		// Now buf has "<series_key> <field>=<value>".
		// Append the timestamp and a newline, then write it.
		buf = append(buf, ' ')
		buf = strconv.AppendInt(buf, ts, 10)
		buf = append(buf, '\n')
		if _, err := w.Write(buf); err != nil {
			// Underlying IO error needs to be returned.
			return err
		}
	}

	return nil
}
