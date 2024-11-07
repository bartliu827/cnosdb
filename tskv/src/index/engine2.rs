use std::fs;
use std::ops::{BitAnd, BitOr, Bound, RangeBounds};
use std::path::Path;

use bytes::BufMut;
use heed::flags::Flags;
use heed::types::*;
use heed::{Database, Env};

use roaring::RoaringBitmap;
use snafu::ResultExt;
use trace::info;

use super::{IndexResult, IndexStorageSnafu, RoaringBitmapSnafu};

pub struct IndexEngine2 {
    env: Env,
    pub db: Database<OwnedSlice<u8>, OwnedSlice<u8>>,
}

impl IndexEngine2 {
    pub fn new(path: impl AsRef<Path>) -> IndexResult<Self> {
        let path = path.as_ref();
        let _ = fs::create_dir_all(path);
        info!("Using index engine path : {:?}", path);

        let mut env_builder = heed::EnvOpenOptions::new();
        unsafe {
            env_builder.flag(Flags::MdbNoSync);
        }

        let env = env_builder
            .map_size(1024 * 1024 * 1024 * 1000)
            .max_dbs(1)
            .max_readers(4096)
            .open(path)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;
        let db: Database<OwnedSlice<u8>, OwnedSlice<u8>> = env
            .create_database(Some("data"))
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(Self { env, db })
    }

    pub fn reader_txn(&self) -> IndexResult<heed::RoTxn> {
        let reader = self
            .env
            .read_txn()
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(reader)
    }

    pub fn writer_txn(&self) -> IndexResult<heed::RwTxn> {
        let writer = self
            .env
            .write_txn()
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(writer)
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> IndexResult<()> {
        let mut writer = self.writer_txn()?;
        self.db
            .put(&mut writer, key, value)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        writer
            .commit()
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> IndexResult<Option<Vec<u8>>> {
        let reader = self.reader_txn()?;
        if let Some(data) = self
            .db
            .get(&reader, key)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?
        {
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> IndexResult<()> {
        let mut writer = self.writer_txn()?;
        self.db
            .delete(&mut writer, key)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        writer
            .commit()
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(())
    }

    pub fn exist(&self, key: &[u8]) -> IndexResult<bool> {
        if let Some(_v) = self.get(key)? {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn flush(&mut self) -> IndexResult<()> {
        let _ = self.env.force_sync();
        Ok(())
    }

    pub fn del_prefix(&mut self, prefix: &[u8]) -> IndexResult<()> {
        let start = prefix.to_vec();
        let mut end = start.clone();
        end.put_u8(0xff);
        let range = (Bound::Included(&*start), Bound::Included(&*end));

        let mut writer = self.writer_txn()?;
        self.db
            .delete_range(&mut writer, &range)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        writer
            .commit()
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(())
    }

    pub fn get_rb(&self, key: &[u8]) -> IndexResult<Option<roaring::RoaringBitmap>> {
        if let Some(data) = self.get(key)? {
            let rb = RoaringBitmap::deserialize_from(&*data).context(RoaringBitmapSnafu)?;

            Ok(Some(rb))
        } else {
            Ok(None)
        }
    }

    pub fn set_rb(&self, key: &[u8], rb: RoaringBitmap) -> IndexResult<()> {
        let mut bytes = vec![];
        rb.serialize_into(&mut bytes)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        self.set(key, &bytes)
    }

    pub fn modify(&mut self, key: &[u8], id: u32, add: bool) -> IndexResult<()> {
        let mut rb = match self.get_rb(key)? {
            Some(val) => val,
            None => roaring::RoaringBitmap::new(),
        };

        if add {
            rb.insert(id);
        } else {
            rb.remove(id);
        }

        self.set_rb(key, rb)
    }

    pub fn merge_rb(&mut self, key: &[u8], rb: &roaring::RoaringBitmap) -> IndexResult<()> {
        let value = match self.get_rb(key)? {
            Some(val) => val,
            None => roaring::RoaringBitmap::new(),
        };

        let value = value.bitor(rb);

        self.set_rb(key, value)
    }

    pub fn get_series_id_by_range(
        &self,
        range: impl RangeBounds<Vec<u8>>,
    ) -> IndexResult<roaring::RoaringBitmap> {
        // process equal special situation
        if let std::ops::Bound::Included(start) = range.start_bound() {
            if let std::ops::Bound::Included(end) = range.end_bound() {
                if start == end {
                    if let Some(rb) = self.get_rb(start)? {
                        return Ok(rb);
                    };
                    return Ok(roaring::RoaringBitmap::new());
                }
            }
        }

        let mut bitmap = roaring::RoaringBitmap::new();

        // let reader = self.reader_txn()?;
        // let it = self
        //     .db
        //     .range(&reader, &range)
        //     .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;
        // for val in it {
        //     let val = val.map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;
        //     let rb = RoaringBitmap::deserialize_from(&*val.1).context(RoaringBitmapSnafu)?;
        //     bitmap = bitmap.bitor(rb);
        // }

        Ok(bitmap)
    }

    pub fn get_series_id_by_tags(
        &self,
        tab: &str,
        tags: &[models::Tag],
    ) -> IndexResult<roaring::RoaringBitmap> {
        let mut bitmap = roaring::RoaringBitmap::new();
        if tags.is_empty() {
            let reader = self.reader_txn()?;
            let prefix = format!("{}.", tab);
            let it = self
                .db
                .prefix_iter(&reader, prefix.as_bytes())
                .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;
            for val in it {
                let val = val.map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;
                let rb = RoaringBitmap::deserialize_from(&*val.1).context(RoaringBitmapSnafu)?;
                bitmap = bitmap.bitor(rb);
            }
        } else {
            let key = super::ts_index::encode_inverted_index_key(tab, &tags[0].key, &tags[0].value);
            if let Some(rb) = self.get_rb(&key)? {
                bitmap = rb;
            }

            for tag in &tags[1..] {
                let key = super::ts_index::encode_inverted_index_key(tab, &tag.key, &tag.value);
                if let Some(rb) = self.get_rb(&key)? {
                    bitmap = bitmap.bitand(rb);
                } else {
                    return Ok(roaring::RoaringBitmap::new());
                }
            }
        }

        Ok(bitmap)
    }
}

#[cfg(test)]
mod test {

    use super::IndexEngine2;

    #[tokio::test]
    async fn test_engine() {
        let mut engine = IndexEngine2::new("/tmp/test/1").unwrap();
        // engine.set(b"key1", b"v11111").unwrap();
        // engine.set(b"key2", b"v22222").unwrap();
        // engine.set(b"key3", b"v33333").unwrap();
        // engine.set(b"key4", b"v44444").unwrap();
        // engine.set(b"key5", b"v55555").unwrap();

        engine.set(b"key3", b"v333334").unwrap();
        engine.flush().unwrap();

        println!("=== {:?}", engine.get(b"key"));
        println!("=== {:?}", engine.get(b"key1"));
        println!("=== {:?}", engine.get(b"key2"));
        println!("=== {:?}", engine.get(b"key3"));
        println!("=== {:?}", engine.delete(b"key3"));
        println!("=== {:?}", engine.get(b"key3"));
    }

    #[tokio::test]
    async fn scan_engine() {
        let path = "/nvme/yanyun/datas/1001/db/data/cnosdb.db1/3/index";
        let engine = IndexEngine2::new(path).unwrap();
        let reader = engine.reader_txn().unwrap();
        let iter = engine.db.iter(&reader).unwrap();

        println!("------scan begin.... {}", path);

        let mut id_info = (0, 0, 0);
        let mut key_info = (0, 0, 0);
        let mut inverted = (0, 0, 0);
        for item in iter {
            let item = item.unwrap();
            let key = item.0;
            let val = item.1;
            if key.starts_with("_id_".as_bytes()) {
                id_info.0 += 1;
                id_info.1 += key.len();
                id_info.2 += val.len();
            } else if key.starts_with("_key_".as_bytes()) {
                key_info.0 += 1;
                key_info.1 += key.len();
                key_info.2 += val.len();
            } else {
                inverted.0 += 1;
                inverted.1 += key.len();
                inverted.2 += val.len();
            }
        }

        println!("------ series id: {:?}", id_info);
        println!("------ series key: {:?}", key_info);
        println!("------ series inverted: {:?}", inverted);
    }
}
