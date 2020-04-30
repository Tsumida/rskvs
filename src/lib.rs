#![deny(missing_docs)]
//! rskvs is a little key-value storage.

use std::fs::File;
use std::io::{Read, Write, BufWriter, BufReader, SeekFrom, Seek};
use std::collections::HashMap;
use std::path::PathBuf;
// use std::sync::{Arc, Mutex};

use std::default::Default;


use failure::{Error};
use serde::{Serialize, Deserialize};

/// Result type.
pub type Result<T> = std::result::Result<T, Error>;
/// Key type.
pub type Key = String;
/// Value type.
pub type Value = String;

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
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<KvStore>{
        let mut kvs = KvStore::new();
        let mut p = dir_path.into();
        p.push(0.to_string());
        kvs.set_path(p);
        Ok(kvs)
    }

    /// Rebuild KeyDir from data file or hint file.
    pub fn rebuild(&mut self) -> Result<()>{
        let hmap = self.st.rebuild_from_all()?;
        self.kd = hmap;
        Ok(())
    }

    fn set_path(&mut self, path: impl Into<PathBuf>){
        // check
        self.st.path = path.into();
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

impl Default for MemDirEntry{
    fn default() -> Self{
        MemDirEntry{
            fid: 0,
            offset: 0,
            sz: 0,
        }
    }
}

struct KeyDir{
    map: HashMap<Key, MemDirEntry>,
}

impl KeyDir{
    fn new() -> KeyDir{
        KeyDir{
            map: HashMap::new(),
        }
    }

    /// Get Key directory entry.
    fn get(&self, k: &Key) -> Option<MemDirEntry>{
        match self.map.get(k){
            None => None,
            Some(r) => Some(r.clone()),
        }
    }

    /// Update diretory entry.
    fn update(&mut self, k: &Key, fid: u32, offset: u32, sz: u32) -> Result<()>{
        let e = self.map.entry(k.clone()).or_insert(MemDirEntry::default());
        e.fid = fid;
        e.offset = offset;
        e.sz = sz;
        Ok(())
    }

    fn remove(&mut self, k: &Key){
        let _ = self.map.remove(k);
    }

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
struct FID{
    fid: u32,
}

//const SUFFIX_HINT:&str = "hint";
const EXTENSION_DATA:&str = "data";
const PATH_FID_LIST:&str = "fids";

struct StableStorage{
    // id of the active file .
    fid: u32, 
    // file size threshold. 
    // When a active file reach this value, it will be closed and become immutable.
    fsz_th: u32,
    // TODO: use efficient obj.
    active_f: Option<BufWriter<File>>,
    // = len(file), Bytes.
    total_bs: u32, 
    // path.
    path: PathBuf,
}

impl StableStorage{
    fn new() -> StableStorage{
        StableStorage{
            fid: 0,
            fsz_th: 4096,
            active_f: None, 
            total_bs: 0,
            path: PathBuf::new(),
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
    fn active_file_path(&self) -> PathBuf{
        eprintln!("{:?}", self.path);
        self.path.to_path_buf()
    }

    #[inline]
    fn get_file_path(&self, fid: u32) -> PathBuf{
        let mut pp = self.path.parent().unwrap().to_path_buf();
        pp.push(StableStorage::data_file_name(fid));
        pp.set_extension(EXTENSION_DATA);
        pp
    }

    #[inline]
    fn data_file_name(fid: u32) -> String{
        fid.to_string()
    }

    fn create_new_active_file(&mut self) -> Result<()>{
        eprintln!("P");
        self.fid += 1;

        self.path.pop();
        self.path.push(PATH_FID_LIST);
        // TODO:
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?
            .write(serde_json::to_string(&FID{fid: self.fid})?.as_bytes())?;

        self.path.pop();
        self.path.push(StableStorage::data_file_name(self.fid));

        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        let nf = BufWriter::new(f);
        self.active_f = Some(nf);
        eprintln!("V");

        Ok(())
    }

    /// Rebuild KeyDir before KVStorage is available for user.
    fn rebuild_from_all(&mut self) -> Result<KeyDir>{
        let mut kd = KeyDir::new();
        let dir_path = self.path.parent().unwrap().to_path_buf();
        assert_eq!(true, dir_path.is_dir());
        // make sure loading in order.
        let mut fid_path = dir_path.to_path_buf();
        fid_path.push(PATH_FID_LIST);

        eprintln!("fids: {:?}", &fid_path);

        // TODO: read fids from PATH_FID_LIST
        let mut bf = String::new();
        std::fs::OpenOptions::new()
            //.create(true)
            .read(true)
            .open(fid_path)?
            .read_to_string(&mut bf)?;

        match serde_json::from_str::<Vec<FID>>(&bf){
            Ok(v) => {
                for e in v{
                    eprintln!("--{:?}", e);
                }
            },
            Err(e) => {
                eprintln!("rebuild all: {}", e);
            }
        }
        Ok(kd)
    }

    /// Rebuild signle data file.
    fn rebuild_from_single(kd:&mut KeyDir, p:&PathBuf, fid: u32) -> Result<usize>{
        eprintln!("--");

        let mut total_bs = 0usize;
        let mut bf = Vec::new();
        if let Ok(mut f) = File::open(p){
            let sz = f.read_to_end(&mut bf)?;
            /*
            for v in vc{
                match v.state{
                    StableEntryState::Deleted => {
                        kd.map.remove(&v.key).unwrap();
                    },
                    StableEntryState::Valid => {
                        let sz = serde_json::to_string(&v)?.as_bytes().len();
                        kd.map.insert(v.key, MemDirEntry{
                            fid: fid,
                            offset: total_bs as u32,
                            sz: sz as u32,
                        });
                    },
                }
            }*/
            total_bs += sz;
            eprintln!("load {:?}, totol size: {} Byte", &p, sz);
        }else{
            eprintln!("failed to load data {:?}", &p);
        }
        Ok(total_bs)
    }
    
}
