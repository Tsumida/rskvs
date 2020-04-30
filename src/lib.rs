#![deny(missing_docs)]
//! rskvs is a little key-value storage.

use std::fs::File;
use std::io::{Read, Write, BufWriter, BufReader, SeekFrom, Seek};
use std::collections::HashMap;
use std::path::PathBuf;
// use std::sync::{Arc, Mutex};
use std::cell::RefCell;


use failure::{Error};
use serde::{Serialize, Deserialize};

type Result<T> = std::result::Result<T, Error>;
type Key = String;
type Value = String;

/// A KvStoree offers method get/set/remove methods like HashMap.
/// Additionally, KvStore provides stablizability.
pub struct KvStore{
    kd: KeyDir,
    st: StableStorage, 
}

impl KvStore{
    /// Create a empty KvStore.
    pub fn new()->Self{
        KvStore{
            kd: KeyDir::new(),
            st: StableStorage::new(),
        }
    }

    /// Insert a new key-value pair or update one already exists.
    /// # Example
    /// ```
    /// use rskvs::KvStore;
    /// 
    /// let mut kvs = KvStore::new();
    /// kvs.set("abc".to_string(), "def".to_string());
    /// 
    /// ```
    pub fn set(&mut self, key:Key, value:Value) -> Result<()>{
        let (fid, offset, sz) = self.st
            .stablize(key.clone(), value, StableEntryState::Valid)?;
            
        self.kd.update(&key, fid, offset, sz)?;
        Ok(())
    }

    /// Get value for a given key, which is  wrapped in Option.
    /// Note that if KeyDir doesn't contains such key, KVStorage won't lookup StableStorage.
    /// # Example
    /// ```
    /// use rskvs::KvStore;
    /// 
    /// let mut kvs = KvStore::new();
    /// kvs.set("abc".to_string(), "def".to_string());
    /// assert_eq!("def".to_string(), kvs.get("abc".to_string()).unwrap().unwrap());
    /// 
    /// ```
    pub fn get(&mut self, key:Key) -> Result<Option<Value>>{
        match self.kd.get(&key){
            None => Ok(None),
            Some(md) => {
                let p = self.st.load(md.fid, md.offset, md.sz)?;
                Ok(Some(p.val))
            },
        }
    }

    /// Remove the key-value pair for given key. 
    /// It's ok to remove a pair not in the KvStore.
    /// # Example
    /// ```
    /// use rskvs::KvStore;
    /// 
    /// let mut kvs = KvStore::new();
    /// kvs.set("abc".to_string(), "def".to_string());
    /// kvs.remove("abc".to_string());
    /// kvs.remove("abc".to_string()); // double removement, ok
    /// ```
    pub fn remove(&mut self, key: Key) -> Result<()>{
        match self.kd.get(&key){
            None => Ok(()),
            Some(ref mut me) => {
                self.st.stablize(key.clone(), String::default(), StableEntryState::Deleted)?;
                self.kd.remove(&key);
                Ok(())
            },
        }
    }

    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore>{
        Ok(KvStore::new())
    }
}

#[derive(Debug, Clone)]
struct MemDirEntry{
    fid: u32,
    offset: u32, 
    sz: u32, 
    // vsz: u32,
    // val_pos: u32, 
    // timestamp.
}

struct KeyDir{
    map: RefCell<HashMap<Key, MemDirEntry>>,
}

impl KeyDir{
    fn new() -> KeyDir{
        KeyDir{
            map: RefCell::new(HashMap::new()),
        }
    }

    /// Get Key directory entry.
    fn get(&self, k: &Key) -> Option<MemDirEntry>{
        match self.map.borrow().get(k){
            None => None,
            Some(r) => Some(r.clone()),
        }
    }

    /// Update diretory entry.
    fn update(&mut self, k: &Key, fid: u32, offset: u32, sz: u32) -> Result<()>{
        let me = MemDirEntry{
            fid: fid,
            offset: offset,
            sz: sz,
        };
        let e = self.map.get_mut().get_mut(k).unwrap();
        *e = me;
        Ok(())
    }

    fn remove(&mut self, k: &Key){
        let _ = self.map.borrow_mut().remove(k);
    }

    /*
    fn rebuild_key_dir(&mut self){
        unimplemented!()
    }
    */
}

#[derive(Debug, Serialize, Deserialize)]
struct StableEntry{
    // crc
    // timestamp
    // ksz: u32, // key size,
    // vsz: u32, // value size,
    state: StableEntryState, 
    key: String,
    val: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
enum StableEntryState{
    Valid,
    Deleted, 
}

struct StableStorage{
    // id of the active file .
    fid: u32, 
    // file size threshold. 
    // When a active file reach this value, it will be closed and become immutable.
    fsz_th: u32,

    // TODO: use efficient obj.
    active_f: Option<RefCell<BufWriter<File>>>,

    // = len(file), Bytes.
    total_bs: u32, 

}

impl StableStorage{
    fn new() -> StableStorage{
        StableStorage{
            fid: 0,
            fsz_th: 4096,
            active_f: None, 
            total_bs: 0,
        }
    }

    /// TODO: Consider log compaction.
    /// Return Error if there is no such key. 
    fn load(&mut self, fid: u32, offset: u32, sz: u32) -> Result<StableEntry>{
        // open file fid 
        // seek to offset
        if self.active_f.is_none(){
            self.create_new_active_file()?;
        }

        let mut bf = BufReader::new(File::open(self.get_file_path(fid))?);
        let _ = bf.seek(SeekFrom::Start(offset as u64));
        let mut buf = Vec::with_capacity(sz as usize);

        bf.read_exact(&mut buf)?;
        let se = serde_json::from_slice::<StableEntry>(&buf)?;

        assert_eq!(&StableEntryState::Valid, &se.state);
        eprintln!("get {:?}", &se);
        Ok(se)
    }

    /// Append key-value pair to active file. Return file id, offset and pair size.
    fn stablize(&mut self, k: Key, v: Value, st: StableEntryState) -> Result<(u32, u32, u32)>{
        let se = StableEntry{
            state: st,
            key: k,
            val: v,
        };

        if self.active_f.is_none() || 
            self.reach_threshold()
        {
            self.create_new_active_file()?;
        }

        let f = self.active_f.as_mut().unwrap().get_mut();
        let size = f.write(serde_json::to_string(&se)?.as_bytes())? as u32;
        self.total_bs += size;
        Ok((self.fid, self.total_bs - size, size))  // offset and length
    }

    #[inline]
    fn reach_threshold(&self) -> bool{
        self.total_bs >= self.fsz_th
    }

    #[inline]
    fn active_file_path(&self) -> String{
        format!("miao_{}", self.fid)
    }

    #[inline]
    fn get_file_path(&self, fid: u32) -> String{
        format!("miao_{}", fid)
    }

    fn create_new_active_file(&mut self) -> Result<()>{
        self.fid += 1;
        let f = std::fs::OpenOptions::new()
            .append(true)
            .open(self.active_file_path())?; 
        let nf = BufWriter::new(f);
        self.active_f = Some(RefCell::new(nf));
        Ok(())
    }

}
