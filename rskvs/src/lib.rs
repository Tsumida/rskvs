#![deny(missing_docs)]
//! rskvs is a little key-value storage.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
// use std::sync::{Arc, Mutex};

use failure::Error;
use std::default::Default;

use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

/// Result type.
pub type Result<T> = std::result::Result<T, Error>;
/// Key type.
pub type Key = String;
/// Value type.
pub type Value = String;

/// A KvStoree offers method get/set/remove methods like HashMap.
/// Additionally, KvStore provides stablizability.
pub struct KvStore {
    kd: KeyDir,
    st: StableStorage,
}

impl KvStore {
    /// Create a empty KvStore.
    pub fn new() -> Self {
        KvStore {
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
    pub fn set(&mut self, key: Key, value: Value) -> Result<()> {
        let (fid, offset, sz) = self
            .st
            .stablize(key.clone(), value, StableEntryState::Valid)?;
        //eprintln!("set key={}, fid={}, offset={}, size={}", &key, fid, offset, sz);
        self.kd.update(&key, fid, offset, sz)?;
        Ok(())
    }

    /// Get value for a given key, which is  wrapped in Option.
    /// Note that if KeyDir doesn't contains such key, KVStorage won't lookup StableStorage.
    /// # Example1
    /// ```
    /// use rskvs::KvStore;
    ///
    /// let mut kvs = KvStore::new();
    /// kvs.set("abc".to_string(), "def".to_string());
    /// assert_eq!("def".to_string(), kvs.get("abc".to_string()).unwrap().unwrap());
    ///
    /// ```
    pub fn get(&mut self, key: Key) -> Result<Option<Value>> {
        match self.kd.get(&key) {
            None => Ok(None),
            Some(md) => {
                let p = self.st.load(md.fid, md.offset, md.sz)?;
                Ok(Some(p.val))
            }
        }
    }

    /// Remove the key-value pair for given key.
    /// It's ok to remove a pair not in the KvStore.
    /// Return true if the key-value pair is removed, or false if key doesn't exist.
    /// # Example
    /// ```
    /// use rskvs::KvStore;
    ///
    /// let mut kvs = KvStore::new();
    /// kvs.set("abc".to_string(), "def".to_string());
    /// kvs.remove("abc".to_string());
    /// kvs.remove("abc".to_string()); // double removement, ok
    /// ```
    pub fn remove(&mut self, key: Key) -> Result<bool> {
        if !self.kd.map.contains_key(&key) {
            return Ok(false);
        }
        self.st
            .stablize(key.clone(), String::default(), StableEntryState::Deleted)?;
        self.kd.remove(&key);
        Ok(true)
    }

    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut kvs = KvStore::new();
        let mut p: PathBuf = dir_path.into();

        if !p.exists() {
            std::fs::create_dir_all(&p)?;
        }

        p.push(0.to_string());
        kvs.st.path = p;

        if let Err(_) = kvs.rebuild() {
            std::process::exit(-1);
        }

        Ok(kvs)
    }

    /// Rebuild KeyDir from data file or hint file.
    pub fn rebuild(&mut self) -> Result<()> {
        let hmap = self.st.rebuild_from_all()?;
        self.kd = hmap;
        Ok(())
    }

    /// print state after rebuilding.
    pub fn init_state(&self) {
        eprintln!("========================== KVS state =========================");
        eprintln!(
            "StableStorage status:\ntotal_bs:{}\nleft={}, right={}\npath={:?}",
            self.st.total_bs, self.st.left, self.st.right, &self.st.path
        );
        eprintln!("========================== Key Dir ===========================");
        for (k, v) in &self.kd.map {
            eprintln!("key = {:12} val = {:?}", k, v);
        }
        eprintln!("========================== Init done =========================");
    }

    /// Set data threshold.
    pub fn set_data_threshold(&mut self, th: u32){
        self.st.fsz_th = th;
    }
}

/// Builder for KVStore.
pub struct KVStoreBuilder {
    path: Option<PathBuf>,
    fsz_th: u32,
}

impl KVStoreBuilder {
    /// Create new builder.
    pub fn new() -> KVStoreBuilder {
        KVStoreBuilder {
            path: None,
            fsz_th: 512,
        }
    }

    /// Set directory for persistent data.
    pub fn set_path(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.path = Some(path.into());
        self
    }

    /// Set threshold of data file size.
    pub fn set_data_threshold(&mut self, th: u32) -> &mut Self {
        self.fsz_th = th;
        self
    }

    /// Build
    pub fn build(&mut self) -> Result<KvStore> {
        let p: PathBuf = if let Some(ref path) = self.path {
            Path::new(path).to_path_buf()
        } else {
            [".", "stab"].iter().collect::<PathBuf>()
        };
        let mut kvs = KvStore::open(p)?;
        kvs.set_data_threshold(self.fsz_th);
        Ok(kvs)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MemDirEntry {
    fid: u32,
    offset: u32,
    sz: u32,
    // vsz: u32,
    // val_pos: u32,
    // timestamp.
}

impl Default for MemDirEntry {
    fn default() -> Self {
        MemDirEntry {
            fid: 0,
            offset: 0,
            sz: 0,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct KeyDir {
    map: HashMap<Key, MemDirEntry>,
}

impl KeyDir {
    fn new() -> KeyDir {
        KeyDir {
            map: HashMap::new(),
        }
    }

    /// Get Key directory entry.
    fn get(&self, k: &Key) -> Option<&MemDirEntry> {
        self.map.get(k)
    }

    /// Update diretory entry.
    fn update(&mut self, k: &Key, fid: u32, offset: u32, sz: u32) -> Result<()> {
        let e = self.map.entry(k.clone()).or_insert(MemDirEntry::default());
        e.fid = fid;
        e.offset = offset;
        e.sz = sz;
        Ok(())
    }

    fn remove(&mut self, k: &Key) {
        let _ = self.map.remove(k);
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StableEntry {
    // crc
    // timestamp
    // ksz: u32, // key size,
    // vsz: u32, // value size,
    st: StableEntryState,
    key: String,
    val: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
enum StableEntryState {
    Valid,
    Deleted,
}

const PATH_FID_LIST: &str = "fids";

struct StableStorage {
    // id of the active file .
    left: u32,
    right: u32,
    // file size threshold.
    // When a active file reach this value, it will be closed and become immutable.
    fsz_th: u32,

    active_f: Option<BufWriter<File>>,
    // = len(file), Bytes.
    total_bs: u32,
    // path.
    path: PathBuf,
}

impl StableStorage {
    fn new() -> StableStorage {
        StableStorage {
            left: 1,
            right: 1,
            fsz_th: 4096,
            active_f: None,
            total_bs: 0,
            path: PathBuf::new(),
        }
    }

    /// TODO: Consider log compaction.
    /// Return Error if there is no such key.
    fn load(&mut self, fid: u32, offset: u32, sz: u32) -> Result<StableEntry> {
        // open file fid
        // seek to offset
        if self.active_f.is_none() || self.reach_threshold() {
            self.create_new_active_file()?;
        }

        // eprintln!("attempt load: fid={}, offset={}, sz={}", fid, offset, sz);

        /* =====================================================================
        ref: https://github.com/Tsumida/rskvs/issues/2
            offset
            |
            | obj_size: u32 | json_string |
        =====================================================================*/
        let fname = self.get_file_path(fid);
        let mut bf = BufReader::new(File::open(fname)?);
        let mut buf = vec![0; sz as usize];
        let _ = bf.seek(SeekFrom::Start(offset as u64))?;
        let obj_size = bf.read_u32::<NativeEndian>().unwrap();
        assert_eq!(obj_size, sz);
        bf.read_exact(&mut buf).unwrap();

        // deserilization.
        let se = serde_json::from_slice::<StableEntry>(&buf).unwrap();

        assert_eq!(&StableEntryState::Valid, &se.st);
        Ok(se)
    }

    /// Append key-value pair to active file. Return file id, offset and pair size.
    fn stablize(&mut self, k: Key, v: Value, st: StableEntryState) -> Result<(u32, u32, u32)> {
        let se = StableEntry {
            st: st,
            key: k,
            val: v,
        };

        if self.active_f.is_none() || self.reach_threshold() {
            self.create_new_active_file()?;
        }
        let f = self.active_f.as_mut().unwrap().get_mut();
        let s = serde_json::to_string(&se)?;
        let size = s.as_bytes().len() as u32;
        self.total_bs = StableStorage::store(f, self.total_bs, s.as_bytes())?;

        Ok((self.right - 1, self.total_bs - size - 4, size)) // offset and length
    }

    /// Store.
    #[inline]
    fn store(mut f: impl Write, mut total_bs: u32, s:&[u8]) -> Result<u32> {
        let size = s.len() as u32;
        let _ = f.write_u32::<NativeEndian>(size)?;
        total_bs += 4;
        assert_eq!(size, f.write(s)? as u32);
        total_bs += size;

        Ok(total_bs)
    }

    #[inline]
    fn reach_threshold(&self) -> bool {
        self.total_bs >= self.fsz_th
    }

    #[inline]
    fn get_file_path(&self, fid: u32) -> PathBuf {
        let mut pp = self.path.parent().unwrap().to_path_buf();
        pp.push(StableStorage::data_file_name(fid));
        pp
    }

    #[inline]
    fn data_file_name(fid: u32) -> String {
        fid.to_string()
    }

    #[inline]
    fn active_fid(&self) -> u32{
        self.right - 1
    }

    /// 1. left==None
    fn update_fid_list(&mut self, left: u32, right:u32) -> Result<()> {
        // | st: u32 | end: u32 |  -->  [st, end)
        self.path.push(PATH_FID_LIST);
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&self.path)?;
        self.path.pop();

        f.write_u32::<NativeEndian>(left)?;
        f.write_u32::<NativeEndian>(right)?;
        Ok(())
    }

    /// Create new active file. Previous data become immutable.
    fn create_new_active_file(&mut self) -> Result<()> {
        self.right += 1;
        eprintln!("create active file - {}", self.right - 1);
        self.path.pop();
        self.update_fid_list(self.left, self.right)?; // record this new fid.

        self.path.push(StableStorage::data_file_name(self.active_fid()));
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        self.total_bs = f.metadata().unwrap().len() as u32;

        let nf = BufWriter::new(f);
        self.active_f = Some(nf);
        Ok(())
    }

    /// Rebuild KeyDir before KVStorage is available for user.
    fn rebuild_from_all(&mut self) -> Result<KeyDir> {
        //self.fid = 0;
        let mut kd = KeyDir::new();
        let mut dir_path = self.path.parent().unwrap().to_path_buf();

        // check existance of fids.
        let mut fid_path = dir_path.to_path_buf();
        fid_path.push(PATH_FID_LIST);
        if !fid_path.exists() {
            self.path = dir_path.clone();
            self.update_fid_list(1, 1).unwrap();
        }

        // read fids from PATH_FID_LIST
        let mut bf = BufReader::new(
            std::fs::OpenOptions::new()
            .read(true)
            .open(fid_path)?);

        // update [left, right)
        match bf.read_u32::<NativeEndian>() {
            Ok(st) => {
                bf.seek(SeekFrom::Start(4)).unwrap();
                self.left = st;
                self.right = bf.read_u32::<NativeEndian>().unwrap();
            }
            _ => {}
        }

        // read all data files.
        for fid in self.left..self.right {
            dir_path.push(StableStorage::data_file_name(fid));
            self.total_bs = StableStorage::rebuild_from_single(&mut kd, &dir_path, fid)?;
            dir_path.pop();
        }

        if self.right > self.left{
            self.log_compact(&mut dir_path,&mut kd, self.left, self.right).unwrap();
        }

        self.path = dir_path;
        self.path.push(StableStorage::data_file_name(0));
        if self.right > self.left {
            if !self.reach_threshold() {
                self.right -= 1
            }
            self.create_new_active_file()?;
        }

        Ok(kd)
    }

    /// Rebuild a signle data file.
    fn rebuild_from_single(kd: &mut KeyDir, p: &PathBuf, fid: u32) -> Result<u32> {
        let mut total_bs = 0u32;
        let mut f = BufReader::new(File::open(p)?);
        while let Ok(obj_sz) = f.read_u32::<NativeEndian>() {
            let mut tmp = vec![0; obj_sz as usize];
            f.read_exact(&mut tmp).unwrap(); // read or write will change seek ptr.

            let v = serde_json::from_slice::<StableEntry>(&tmp).unwrap();
            match v.st {
                StableEntryState::Deleted => {
                    kd.map.remove(&v.key).unwrap();
                }
                StableEntryState::Valid => {
                    let sz = serde_json::to_string(&v)?.as_bytes().len();
                    kd.map.insert(
                        v.key,
                        MemDirEntry {
                            fid: fid,
                            offset: total_bs as u32,
                            sz: sz as u32,
                        },
                    );
                }
            }
            total_bs += obj_sz + 4;
        }
        eprintln!();
        Ok(total_bs)
    }

    /// rebuild all file and merge them.
    fn log_compact(&mut self, dir_p: &mut PathBuf, kd:&mut KeyDir, left: u32, right:u32) -> Result<()>{
        let fid = right - 1;
        eprintln!("log compacting... {}-{}", left, right);
        assert!(fid >= left);

        dir_p.push(format!("{}.mege", fid));

        {
            let mut bf = BufWriter::new(
                File::create(&dir_p)?
            );
            let mut meta = Vec::new();
            for (k, v) in kd.map.iter(){
                meta.push((k.clone(), v.clone()));
            }
    
            let mut total_bs = 0;
            for (k, v) in &meta{
                kd.update(k, fid, total_bs as u32, v.sz).unwrap();
                let s: StableEntry = self.load(v.fid, v.offset, v.sz).unwrap();
                bf.write_u32::<NativeEndian>(v.sz).unwrap();
                total_bs += bf.write(
                    serde_json::to_string(&s).unwrap().as_bytes()
                ).unwrap();
                total_bs += 4;
            }
        }// bf drop here.
        
        
        // delete data files. [left, fid]
        let mut tmp = dir_p.clone();
        for fid in left..right{
            tmp.pop();
            tmp.push(
                StableStorage::data_file_name(fid)
            );
            std::fs::remove_file(&tmp).unwrap();
        }
        
        // rename.
        std::fs::rename(dir_p.clone(), tmp).unwrap();
        dir_p.pop();
        // generate hint file.

        // update fid_list
        // |    merge     |
        // |left, right - 1, right,  ... act_fid | 
        //      | right - 1  ........... act_fid |
        //      |                                |
        //  self.left ----------------------- self.right
        self.left = right - 1;
        self.path = dir_p.clone();
        self.update_fid_list(self.left, self.right).unwrap();

        Ok(())
    }
}
