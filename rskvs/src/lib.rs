#![deny(missing_docs)]
//! rskvs is a little key-value storage.

use std::collections::{BTreeMap, HashMap};
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
    /// use rskvs::KvStoreBuilder;
    /// let mut kvs =  KvStoreBuilder::new().set_data_threshold(512).build().unwrap();
    /// kvs.set("key1".to_owned(), "value1".to_owned()).unwrap();
    ///
    /// ```
    pub fn set(&mut self, key: Key, value: Value) -> Result<()> {
        let (fid, offset, sz) =
            self.st
                .stablize(&mut self.kd, key.clone(), value, StableEntryState::Valid)?;
        self.kd.update(&key, fid, offset, sz)?;
        Ok(())
    }

    /// Get value for a given key, which is  wrapped in Option.
    /// Note that if KeyDir doesn't contains such key, KVStorage won't lookup StableStorage.
    /// # Example1
    /// ```
    /// use rskvs::KvStoreBuilder;
    ///
    /// let mut kvs =  KvStoreBuilder::new().set_data_threshold(512).build().unwrap();
    /// kvs.set("abc".to_string(), "def".to_string()).unwrap();
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
    /// use rskvs::KvStoreBuilder;
    ///
    /// let mut kvs =  KvStoreBuilder::new().set_data_threshold(512).build().unwrap();
    /// kvs.set("abc".to_string(), "def".to_string());
    /// kvs.remove("abc".to_string());
    /// kvs.remove("abc".to_string()); // double removement, ok
    /// ```
    pub fn remove(&mut self, key: Key) -> Result<bool> {
        if !self.kd.map.contains_key(&key) {
            return Ok(false);
        }
        self.st.stablize(
            &mut self.kd,
            key.clone(),
            String::default(),
            StableEntryState::Deleted,
        )?;
        self.kd.remove(&key);
        Ok(true)
    }

    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut kvs = KvStore::new();
        kvs.st.dir_path = dir_path.into();

        if !kvs.st.dir_path.exists() {
            std::fs::create_dir_all(&kvs.st.dir_path)?;
        }
        kvs.st.active_path = kvs.st.dir_path.clone();

        if let Err(e) = kvs.rebuild() {
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
            "StableStorage status:\ntotal_bs:{}\nleft={}, right={}\ndir_path={:?}",
            self.st.total_bs, self.st.left, self.st.right, &self.st.dir_path
        );
        eprintln!("========================== Readers ===========================");
        for k in self.st.readers.keys() {
            eprintln!("fid = {}", k);
        }
        eprintln!("========================== Key Dir ===========================");
        for (k, v) in &self.kd.map {
            eprintln!("key = {:12} val = {:?}", k, v);
        }
        eprintln!("========================== Init done =========================");
    }

    /// Set data threshold.
    pub fn set_data_threshold(&mut self, th: u32) {
        self.st.fsz_th = th;
    }
}

/// Builder for KVStore.
pub struct KvStoreBuilder {
    path: Option<PathBuf>,
    fsz_th: u32,
    enable_compress: bool,
}

impl KvStoreBuilder {
    /// Create new builder.
    pub fn new() -> KvStoreBuilder {
        KvStoreBuilder {
            path: None,
            fsz_th: 512,
            enable_compress: true,
        }
    }

    /// Set directory for persistent data.
    pub fn set_dir_path(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.path = Some(path.into());
        self
    }

    /// Set threshold of data file size.
    pub fn set_data_threshold(&mut self, th: u32) -> &mut Self {
        self.fsz_th = th;
        self
    }

    /// Enable or disable log compression.
    pub fn enable_log_compress(&mut self, b: bool) -> &mut Self {
        self.enable_compress = b;
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
        kvs.st.enable_log_compress = self.enable_compress;
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

    #[inline]
    /// Get Key directory entry.
    fn get(&self, k: &Key) -> Option<&MemDirEntry> {
        self.map.get(k)
    }

    #[inline]
    /// Update diretory entry.
    fn update(&mut self, k: &Key, fid: u32, offset: u32, sz: u32) -> Result<()> {
        let e = self.map.entry(k.clone()).or_insert(MemDirEntry::default());
        e.fid = fid;
        e.offset = offset;
        e.sz = sz;
        Ok(())
    }

    #[inline]
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
    // [left, right)
    // right-1 is the number of the active file.
    left: u32,
    right: u32,
    // file size threshold.
    // When a active file reach this value, it will be closed and become immutable.
    fsz_th: u32,

    // Bufferd reader
    readers: BTreeMap<u32, BufReader<File>>,

    active_f: Option<BufWriter<File>>,
    // = len(file), Bytes.
    total_bs: u32,

    // dir_path
    dir_path: PathBuf,
    // path.
    active_path: PathBuf,

    enable_log_compress: bool,
}

impl StableStorage {
    fn new() -> StableStorage {
        StableStorage {
            left: 1,
            right: 1,
            fsz_th: 4096,
            readers: BTreeMap::new(),
            active_f: None,
            total_bs: 0,
            dir_path: PathBuf::new(),
            active_path: PathBuf::new(),
            enable_log_compress: true,
        }
    }

    /// TODO: Consider log compaction.
    /// Return Error if there is no such key.
    fn load(&mut self, fid: u32, offset: u32, sz: u32) -> Result<StableEntry> {
        /* =====================================================================
        ref: https://github.com/Tsumida/rskvs/issues/2
            offset
            |
            | obj_size: u32 | json_string |
        =====================================================================*/
        let mut buf = vec![0; sz as usize];

        assert!(self.left <= fid && fid < self.right);

        let reader = self.readers.get_mut(&fid).unwrap();
        reader.seek(SeekFrom::Start(offset as u64))?;
        let obj_size = reader.read_u32::<NativeEndian>().unwrap();

        assert_eq!(obj_size, sz);
        reader.read_exact(&mut buf).unwrap();

        // deserilization.
        let se = serde_json::from_slice::<StableEntry>(&buf).unwrap();
        assert_eq!(&StableEntryState::Valid, &se.st);
        Ok(se)
    }

    /// Load raw bytes without deserializaion.
    fn load_raw(&mut self, fid: u32, offset: u32, sz: u32) -> Result<Vec<u8>> {
        /* =====================================================================
        ref: https://github.com/Tsumida/rskvs/issues/2
            offset
            |
            | obj_size: u32 | json_string |
        =====================================================================*/
        let mut buf = vec![0; sz as usize];
        assert!(self.left <= fid && fid < self.right);
        let reader = self.readers.get_mut(&fid).unwrap();
        reader.seek(SeekFrom::Start(offset as u64))?;
        let obj_size = reader.read_u32::<NativeEndian>().unwrap();

        assert_eq!(obj_size, sz);
        reader.read_exact(&mut buf).unwrap();

        // deserilization.
        Ok(buf)
    }

    /// Append key-value pair to active file. Return file id, offset and pair size.
    fn stablize(
        &mut self,
        kd: &mut KeyDir,
        k: Key,
        v: Value,
        st: StableEntryState,
    ) -> Result<(u32, u32, u32)> {
        let se = StableEntry {
            st: st,
            key: k,
            val: v,
        };

        if self.right - self.left >= 5 && self.enable_log_compress {
            self.log_compress(kd, self.left, self.right)?; // self.left changed.
            self.rebuild_readers(self.left, self.right)?;
        }

        if self.active_f.is_none() || self.reach_threshold() {
            self.create_new_active_file()?;
        }

        if !self.readers.contains_key(&self.active_fid()) {
            self.insert_new_reader(self.active_fid())?;
        }

        let f = self.active_f.as_mut().unwrap().get_mut();
        let s = serde_json::to_string(&se)?;
        let size = s.as_bytes().len() as u32;
        let offset = self.total_bs;
        self.total_bs = StableStorage::store(f, offset, s.as_bytes())?;

        Ok((self.active_fid(), offset, size)) // offset and length
    }

    /// Store.
    #[inline]
    fn store(mut f: impl Write, mut total_bs: u32, s: &[u8]) -> Result<u32> {
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
    fn data_file_name(fid: u32) -> String {
        fid.to_string()
    }

    #[inline]
    fn active_fid(&self) -> u32 {
        self.right - 1
    }

    fn insert_new_reader(&mut self, fid: u32) -> Result<()> {
        let mut p = self.dir_path.clone();
        p.push(StableStorage::data_file_name(fid));
        let bf = BufReader::new(File::open(p)?);
        self.readers.insert(fid, bf);
        Ok(())
    }

    /// 1. left==None
    fn update_fid_list(&mut self, left: u32, right: u32) -> Result<()> {
        // | st: u32 | end: u32 |  -->  [st, end)
        let mut p = self.dir_path.clone();
        p.push(PATH_FID_LIST);
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&p)?;

        f.write_u32::<NativeEndian>(left)?;
        f.write_u32::<NativeEndian>(right)?;
        Ok(())
    }

    /// Create new active file. Previous data become immutable.
    fn create_new_active_file(&mut self) -> Result<()> {
        self.right += 1;
        //eprintln!("create active file -- {}", self.active_fid());
        self.update_fid_list(self.left, self.right)?; // record this new fid.

        self.active_path = self.dir_path.clone();
        self.active_path
            .push(StableStorage::data_file_name(self.active_fid()));
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.active_path)?;

        self.total_bs = f.metadata().unwrap().len() as u32;
        //self.total_bs = 0;
        //eprintln!("active file size: {}", self.total_bs);
        self.active_f = Some(BufWriter::new(f));
        Ok(())
    }

    /// Rebuild KeyDir before KVStorage is available for user.
    fn rebuild_from_all(&mut self) -> Result<KeyDir> {
        let mut kd = KeyDir::new();
        let mut dir_path = self.dir_path.clone();

        // check existance of fids.
        let mut fid_path = dir_path.clone();
        fid_path.push(PATH_FID_LIST);
        if !fid_path.exists() {
            self.update_fid_list(1, 1).unwrap();
        }

        // read fids from PATH_FID_LIST
        let mut bf = BufReader::new(std::fs::OpenOptions::new().read(true).open(fid_path)?);

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
            self.insert_new_reader(fid)?;
        }
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
        Ok(total_bs)
    }

    fn rebuild_readers(&mut self, left: u32, right: u32) -> Result<()> {
        let mut p = self.dir_path.clone();
        for id in left..right {
            p.push(StableStorage::data_file_name(id));
            self.readers.insert(id, BufReader::new(File::open(&p)?));
            p.pop();
        }
        Ok(())
    }

    /// Take log entires in [left, right), moving them into file numbered right - 1.
    fn log_compress(&mut self, kd: &mut KeyDir, left: u32, right: u32) -> Result<()> {
        assert!(right > left);

        let mut merged_path = self.dir_path.clone();
        let mut tmp = self.dir_path.clone();
        let fid = right - 1;

        merged_path.push(format!("{}.mege", fid));
        // moving log entries...
        {
            let mut bf = BufWriter::new(File::create(&merged_path)?);
            let mut meta = Vec::new();
            // kd -> meta
            for (k, v) in kd.map.iter() {
                meta.push((k.clone(), v.clone()));
            }
            let mut total_bs = 0;

            // moving ..
            for (k, v) in &meta {
                let buf = self.load_raw(v.fid, v.offset, v.sz).unwrap();
                bf.write_u32::<NativeEndian>(v.sz).unwrap();
                let obj_sz = bf.write(&buf).unwrap() as u32;
                kd.update(k, fid, total_bs, obj_sz).unwrap();

                assert_eq!(obj_sz, v.sz);
                total_bs += obj_sz + 4;
            }
            self.total_bs = total_bs as u32;
        } // bf drop here.
        for id in left..right {
            tmp.push(StableStorage::data_file_name(id));
            std::fs::remove_file(&tmp).unwrap();
            self.readers.remove(&id);
            tmp.pop();
        }
        // rename.
        tmp.push(StableStorage::data_file_name(fid));
        std::fs::rename(&merged_path, &tmp).unwrap();
        // generate hint file.

        // update fid_list
        // |    compressed     |
        // |left,        right - 1, right,  ... act_fid |
        //      |        right - 1  ........... act_fid |
        //      |                                  |
        //  self.left ----------------------- self.right
        self.left = right - 1;
        self.active_path = self.dir_path.clone();
        self.update_fid_list(self.left, self.right).unwrap();
        self.active_path
            .push(StableStorage::data_file_name(self.active_fid()));

        eprintln!("compression done. [{}, {})", left, right);
        Ok(())
    }
}
