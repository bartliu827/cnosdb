use std::collections::HashMap;
use std::fs;
use std::path::Path;

use heed::flags::Flags;
use heed::types::*;
use heed::{Database, Env};
use openraft::{LogId, StoredMembership, Vote};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::errors::{HeedSnafu, IOErrSnafu, MsgInvalidSnafu, ReplicationResult};
use crate::{RaftNodeId, RaftNodeInfo};

pub struct Key {}
impl Key {
    fn node_summary(id: u32) -> String {
        format!("node_summary_{}", id)
    }

    fn applied_log(id: u32) -> String {
        format!("applied_log_{}", id)
    }

    fn snapshot_applied_log(id: u32) -> String {
        format!("snapshot_applied_log_{}", id)
    }

    fn purged_log_id(id: u32) -> String {
        format!("purged_log_id_{}", id)
    }

    fn vote_key(id: u32) -> String {
        format!("vote_{}", id)
    }

    fn membership(id: u32) -> String {
        format!("membership_{}", id)
    }

    fn membership_list_prefix(id: u32) -> String {
        format!("membership_{}_", id)
    }

    fn membership_list(id: u32, index: u64) -> String {
        format!("membership_{}_{}", id, index)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct RaftNodeSummary {
    pub tenant: String,
    pub db_name: String,
    pub group_id: u32,
    pub raft_id: u64,
}
pub struct StateStorage {
    env: Env,
    db: Database<Str, OwnedSlice<u8>>,
}

impl StateStorage {
    pub fn open(path: impl AsRef<Path>, size: usize) -> ReplicationResult<Self> {
        fs::create_dir_all(&path).context(IOErrSnafu)?;

        let mut env_builder = heed::EnvOpenOptions::new();
        unsafe {
            env_builder.flag(Flags::MdbNoSync);
        }

        let env = env_builder
            .map_size(size)
            .max_dbs(1)
            .open(path)
            .context(HeedSnafu)?;
        let db: Database<Str, OwnedSlice<u8>> =
            env.create_database(Some("data")).context(HeedSnafu)?;
        let storage = Self { env, db };

        Ok(storage)
    }

    fn reader_txn(&self) -> ReplicationResult<heed::RoTxn> {
        let reader = self.env.read_txn().context(HeedSnafu)?;

        Ok(reader)
    }

    fn writer_txn(&self) -> ReplicationResult<heed::RwTxn> {
        let writer = self.env.write_txn().context(HeedSnafu)?;

        Ok(writer)
    }

    fn get<T>(&self, reader: &heed::RoTxn, key: &str) -> ReplicationResult<Option<T>>
    where
        for<'a> T: Deserialize<'a>,
    {
        if let Some(data) = self.db.get(reader, key).context(HeedSnafu)? {
            let val = serde_json::from_slice(&data)
                .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;

            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    fn set<T>(&self, writer: &mut heed::RwTxn, key: &str, val: &T) -> ReplicationResult<()>
    where
        for<'a> T: Serialize,
    {
        let data =
            serde_json::to_vec(val).map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;

        self.db.put(writer, key, &data).context(HeedSnafu)?;

        Ok(())
    }

    fn del(&self, writer: &mut heed::RwTxn, key: &str) -> ReplicationResult<()> {
        self.db.delete(writer, key).context(HeedSnafu)?;

        Ok(())
    }

    pub fn get_last_membership(
        &self,
        group_id: u32,
    ) -> ReplicationResult<StoredMembership<RaftNodeId, RaftNodeInfo>> {
        let reader = self.reader_txn()?;
        let mem_ship: StoredMembership<RaftNodeId, RaftNodeInfo> = self
            .get(&reader, &Key::membership(group_id))?
            .unwrap_or_default();

        Ok(mem_ship)
    }

    pub fn set_last_membership(
        &self,
        group_id: u32,
        membership: StoredMembership<RaftNodeId, RaftNodeInfo>,
    ) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::membership(group_id), &membership)?;
        if let Some(log_id) = membership.log_id() {
            self.set(
                &mut writer,
                &Key::membership_list(group_id, log_id.index),
                &membership,
            )?;
        }

        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    pub fn get_membership_list(
        &self,
        group_id: u32,
    ) -> ReplicationResult<HashMap<String, StoredMembership<RaftNodeId, RaftNodeInfo>>> {
        let reader = self.reader_txn()?;
        let mut memberships = HashMap::new();
        let mut iter = self
            .db
            .prefix_iter(&reader, &Key::membership_list_prefix(group_id))
            .context(HeedSnafu)?;
        while let Some((key, data)) = iter.next().transpose().context(HeedSnafu)? {
            let value = serde_json::from_slice(&data)
                .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;
            memberships.insert(key.to_string(), value);
        }

        Ok(memberships)
    }

    pub fn clear_memberships(&self, group_id: u32, index: u64) -> ReplicationResult<()> {
        let memberships = self.get_membership_list(group_id)?;
        let mut list: Vec<(String, StoredMembership<u64, RaftNodeInfo>)> =
            memberships.into_iter().collect();

        list.sort_by_key(|x| x.1.log_id().unwrap_or_default().index);
        list.retain(|x| x.1.log_id().unwrap_or_default().index <= index);
        list.pop();

        let keys: Vec<String> = list.iter().map(|(first, _)| first.clone()).collect();
        self.del_keys(&keys)?;

        Ok(())
    }

    pub fn get_last_applied_log(
        &self,
        group_id: u32,
    ) -> ReplicationResult<Option<LogId<RaftNodeId>>> {
        let reader = self.reader_txn()?;
        let log_id: Option<LogId<RaftNodeId>> = self.get(&reader, &Key::applied_log(group_id))?;

        Ok(log_id)
    }

    pub fn set_last_applied_log(
        &self,
        group_id: u32,
        log_id: LogId<RaftNodeId>,
    ) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::applied_log(group_id), &log_id)?;
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    pub fn get_snapshot_applied_log(
        &self,
        group_id: u32,
    ) -> ReplicationResult<Option<LogId<RaftNodeId>>> {
        let reader = self.reader_txn()?;
        let key = Key::snapshot_applied_log(group_id);
        let log_id: Option<LogId<RaftNodeId>> = self.get(&reader, &key)?;

        Ok(log_id)
    }

    pub fn set_snapshot_applied_log(
        &self,
        group_id: u32,
        log_id: LogId<RaftNodeId>,
    ) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        let key = Key::snapshot_applied_log(group_id);
        self.set(&mut writer, &key, &log_id)?;
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    pub fn get_last_purged(&self, group_id: u32) -> ReplicationResult<Option<LogId<u64>>> {
        let reader = self.reader_txn()?;
        let log_id: Option<LogId<RaftNodeId>> = self.get(&reader, &Key::purged_log_id(group_id))?;

        Ok(log_id)
    }

    pub fn set_last_purged(&self, group_id: u32, log_id: LogId<u64>) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::purged_log_id(group_id), &log_id)?;
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    pub fn get_vote(&self, group_id: u32) -> ReplicationResult<Option<Vote<RaftNodeId>>> {
        let reader = self.reader_txn()?;
        let vote_val: Option<Vote<RaftNodeId>> = self.get(&reader, &Key::vote_key(group_id))?;

        Ok(vote_val)
    }

    pub fn set_vote(&self, group_id: u32, vote: &Vote<RaftNodeId>) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::vote_key(group_id), vote)?;
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    pub fn get_node_summary(&self, group_id: u32) -> ReplicationResult<Option<RaftNodeSummary>> {
        let reader = self.reader_txn()?;
        let summary: Option<RaftNodeSummary> = self.get(&reader, &Key::node_summary(group_id))?;

        Ok(summary)
    }

    pub fn set_node_summary(
        &self,
        group_id: u32,
        summary: &RaftNodeSummary,
    ) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::node_summary(group_id), summary)?;
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    pub fn del_keys(&self, keys: &[String]) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        for key in keys {
            self.del(&mut writer, key)?;
        }
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    pub fn all_nodes_summary(&self) -> ReplicationResult<Vec<RaftNodeSummary>> {
        let mut nodes_summary = vec![];
        let reader = self.reader_txn()?;
        let iter = self
            .db
            .prefix_iter(&reader, "node_summary_")
            .context(HeedSnafu)?;
        for pair in iter {
            let (_, data) = pair.context(HeedSnafu)?;
            let summary: RaftNodeSummary = serde_json::from_slice(&data)
                .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;
            nodes_summary.push(summary);
        }

        Ok(nodes_summary)
    }

    pub fn del_group(&self, group_id: u32) -> ReplicationResult<()> {
        let memberships = self.get_membership_list(group_id)?;
        let mut writer = self.writer_txn()?;
        self.del(&mut writer, &Key::applied_log(group_id))?;
        self.del(&mut writer, &Key::membership(group_id))?;
        self.del(&mut writer, &Key::purged_log_id(group_id))?;
        self.del(&mut writer, &Key::vote_key(group_id))?;
        self.del(&mut writer, &Key::node_summary(group_id))?;
        self.del(&mut writer, &Key::snapshot_applied_log(group_id))?;
        for (key, _) in memberships {
            self.del(&mut writer, &key)?;
        }
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    pub fn debug(&self) {
        let reader = self.reader_txn().unwrap();
        let iter = self.db.iter(&reader).unwrap();
        for pair in iter {
            let (key, val) = pair.unwrap();
            println!("{}: {}", key, String::from_utf8_lossy(&val));
        }
    }
}

pub fn set_raft_state(path: &str, key: &str, val: &str) {
    let state = StateStorage::open(path, 1024 * 1024 * 1024).unwrap();
    let mut writer = state.writer_txn().unwrap();
    state
        .db
        .put(&mut writer, key, val.as_bytes())
        .context(HeedSnafu)
        .unwrap();
    writer.commit().context(HeedSnafu).unwrap();
}

pub fn print_raft_state(path: &str) {
    let state = StateStorage::open(path, 1024 * 1024 * 1024).unwrap();
    let all_nodes = state.all_nodes_summary().unwrap();
    let reader = state.reader_txn().unwrap();

    for info in all_nodes {
        println!(
            "*** group: {}, id: {}, tenant: {}, db-name: {} ***",
            info.group_id, info.raft_id, info.tenant, info.db_name
        );

        let key = Key::node_summary(info.group_id);
        if let Some(data) = state.db.get(&reader, &key).unwrap() {
            println!("├── {} :{}", key, String::from_utf8_lossy(&data));
        }

        let key = Key::applied_log(info.group_id);
        if let Some(data) = state.db.get(&reader, &key).unwrap() {
            println!("├── {} :{}", key, String::from_utf8_lossy(&data));
        }

        let key = Key::snapshot_applied_log(info.group_id);
        if let Some(data) = state.db.get(&reader, &key).unwrap() {
            println!("├── {} :{}", key, String::from_utf8_lossy(&data));
        }

        let key = Key::purged_log_id(info.group_id);
        if let Some(data) = state.db.get(&reader, &key).unwrap() {
            println!("├── {} :{}", key, String::from_utf8_lossy(&data));
        }

        let key = Key::vote_key(info.group_id);
        if let Some(data) = state.db.get(&reader, &key).unwrap() {
            println!("├── {} :{}", key, String::from_utf8_lossy(&data));
        }

        let key = Key::membership(info.group_id);
        if let Some(data) = state.db.get(&reader, &key).unwrap() {
            println!("├── {} :{}", key, String::from_utf8_lossy(&data));
        }

        let key = Key::membership_list_prefix(info.group_id);
        let mut iter = state.db.prefix_iter(&reader, &key).unwrap();
        while let Some((key, data)) = iter.next().transpose().unwrap() {
            println!("├── {} :{}", key, String::from_utf8_lossy(&data));
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::path::Path;

    use heed::types::*;
    use heed::Database;

    #[test]
    #[ignore]
    fn test_heed() {
        let path = "/tmp/cnosdb/test_heed";
        fs::create_dir_all(Path::new(&path)).unwrap();
        let env = heed::EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(128)
            .open(path)
            .unwrap();

        let tdb: Database<OwnedType<u64>, Str> = env.create_database(Some("test")).unwrap();
        let mut writer = env.write_txn().unwrap();
        tdb.put(&mut writer, &100, "v100").unwrap();
        tdb.put(&mut writer, &101, "v101").unwrap();
        tdb.put(&mut writer, &102, "v102").unwrap();
        tdb.put(&mut writer, &103, "v103").unwrap();
        tdb.put(&mut writer, &104, "v104").unwrap();
        writer.commit().unwrap();

        let mut writer = env.write_txn().unwrap();
        tdb.delete_range(&mut writer, &(..80)).unwrap();
        writer.commit().unwrap();

        let reader = env.read_txn().unwrap();
        let iter = tdb.range(&reader, &(101..103)).unwrap();
        for pair in iter {
            let (index, data) = pair.unwrap();
            println!("--- {}, {}", index, data);
        }

        fs::remove_dir_all(path).unwrap();
    }
}
