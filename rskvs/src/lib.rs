//#![deny(missing_docs)]
//! rskvs is a little key-value storage.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::{
    collections::{BTreeMap, HashMap},
    unimplemented,
};

use log::{debug, error, info};

use failure::Error;
use std::default::Default;

use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

/// State about compreesing.
const STATE_DONE: u8 = 0;
const STATE_COMPRESSING: u8 = 1;

const KB: u64 = 1 << 10;
const MB: u64 = 1 << 20;

const DEFAULT_ITEM_NUM: u32 = 3;
const DEFAULT_DUP_MIN_TH: u64 = 10 * KB;
const DEFAULT_DUP_PROP: f64 = 0.5;

/// Result type.
pub type Result<T> = std::result::Result<T, Error>;
/// Key type.
pub type Key = String;
/// Value type.
pub type Value = String;

#[derive(Debug, Clone)]
/// Statistic for compressing
pub struct Stat {
    total: u64,
    dup_byte_size: u64,
    dup_th_prop: f64,
    dup_th_min: u64,
}

impl Stat {
    fn new(dup_th_prop: f64, dup_th_min: u64) -> Self {
        Stat {
            total: 0,
            dup_byte_size: 0,
            dup_th_min,
            dup_th_prop,
        }
    }

    fn reset(&mut self) {
        self.dup_byte_size = 0;
    }

    fn increase_dup_size(&mut self, delta: u64) {
        self.dup_byte_size += delta;
    }
}

#[derive(Debug, Clone)]
pub enum Cmd {
    Get(String),
    Set(String, String),
    Remove(String),
    Batch(Vec<Cmd>),
    Tx(Vec<Cmd>),
}

#[derive(Debug, Clone)]
pub enum Reply {
    Get(Option<String>),
    Set(bool),
    Remove(bool),
    Batch(Vec<Reply>),
    Tx(Vec<Reply>),
}

pub type Request = (Cmd, SyncSender<Reply>);

pub struct KvAdaptor {
    // to recvr
    pub op_sender: SyncSender<Request>,
}

#[derive(Debug)]
pub struct CompressInfo {
    stat: Stat,
    dir_path: PathBuf,
    merged_log_path: PathBuf,
    left: u32,
    right: u32,
    kd: KeyDir,
}

/// A KvStoree offers method get/set/remove methods like HashMap.
/// Additionally, KvStore provides stablizability.
pub struct KvStore {
    kd: KeyDir,
    st: StableStorage,

    //op_sender: SyncSender<Request>,
    op_recvr: Receiver<Request>,

    compress_state: AtomicU8,
    compress_notifier: SyncSender<CompressInfo>,
    compress_done: Receiver<CompressInfo>,
}

impl KvStore {
    /// Create a empty KvStore.
    pub fn new(op_recvr: Receiver<Request>) -> Self {
        // TODO:
        let (compress_notifier, cmp_recvr) = sync_channel(4);
        let (done_sender, compress_done) = sync_channel(4);

        std::thread::spawn(move || {
            KvStore::compress(cmp_recvr, done_sender).unwrap();
        });

        KvStore {
            kd: KeyDir::new(),
            st: StableStorage::new(),
            op_recvr,
            compress_state: AtomicU8::new(0),
            compress_notifier,
            compress_done,
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
        let stale_sz = self.kd.get(&key).map_or(0, |e| e.sz);
        let (fid, offset, sz) =
            self.st
                .stablize(key.clone(), value, StableEntryState::Valid, stale_sz)?;
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
        let stale_sz = self.kd.remove(&key);
        if stale_sz == 0 {
            return Ok(false);
        }
        let _ = self.st.stablize(
            key.clone(),
            String::default(),
            StableEntryState::Deleted,
            stale_sz,
        )?;
        Ok(true)
    }

    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<(KvAdaptor, KvStore)> {
        let (req_s, req_r) = sync_channel(128);
        let mut kvs = KvStore::new(req_r);
        kvs.st.dir_path = dir_path.into();

        if !kvs.st.dir_path.exists() {
            std::fs::create_dir_all(&kvs.st.dir_path)?;
        }
        kvs.st.active_path = kvs.st.dir_path.clone();

        if let Err(e) = kvs.rebuild() {
            std::process::exit(-1);
        }

        Ok((KvAdaptor { op_sender: req_s }, kvs))
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
            "StableStorage status:\ntotal_bs:{}\nleft={}, right={}\ndir_path={:?}\nstale_size:{}\n",
            self.st.last_file_size(),
            self.st.left,
            self.st.right,
            &self.st.dir_path,
            self.st.stat.dup_byte_size,
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

    /// Start kvs engine.
    pub fn run(&mut self) {
        info!("kvs up");
        let mut down = false;
        // open compressor
        loop {
            match self.op_recvr.recv() {
                Ok(req) => {
                    if let Err(e) = self.process_cmd(req) {
                        error!("{}", e);
                        down = true;
                    }
                }
                Err(_) => {
                    down = true;
                }
            }
            if !down && !self.is_compressing() && self.need_compress() {
                // active file excluded
                let kd = self.kd.clone();
                self.update_compress_state(STATE_COMPRESSING);
                self.compress_notifier
                    .send(CompressInfo {
                        stat: self.st.stat.clone(),
                        kd,
                        dir_path: self.st.dir_path.clone(),
                        merged_log_path: self.st.dir_path.clone(),
                        left: self.st.left,
                        right: self.st.right,
                    })
                    .unwrap();
            }
            if let Ok(cmp_info) = self.compress_done.try_recv() {
                self.merge_compress_result(cmp_info).unwrap();
                self.update_compress_state(STATE_DONE);
            }
            if down {
                break;
            }
        }
        info!("kvs down");
    }

    fn need_compress(&self) -> bool {
        let stat = &self.st.stat;
        stat.dup_byte_size >= stat.dup_th_min
            && stat.dup_byte_size >= (stat.total as f64 * stat.dup_th_prop) as u64
            // at least one file (active log executed).
            && self.st.left + 1 + DEFAULT_ITEM_NUM < self.st.right
    }

    fn is_compressing_completed(&self) -> bool {
        self.compress_state.load(Ordering::SeqCst) == STATE_DONE
    }

    fn is_compressing(&self) -> bool {
        self.compress_state.load(Ordering::SeqCst) == STATE_COMPRESSING
    }

    fn update_compress_state(&mut self, new_state: u8) {
        self.compress_state.store(new_state, Ordering::SeqCst);
    }

    fn update_key_dir(&mut self, cmp_info: &CompressInfo) {
        let (left, last) = (cmp_info.left, cmp_info.right - 1);
        let cmp_kd = &cmp_info.kd.map;
        // merging keydir and remove entries with tombstone
        self.kd.map.retain(|k, v| {
            // keep entries in previous or current active log.
            v.fid >= last
            // update entries in merging internal 
                || (cmp_kd.get(k).map_or(false, |v_merged| {
                    *v = v_merged.clone();
                    true
                }))
        });
    }

    fn merge_compress_result(&mut self, cmp_info: CompressInfo) -> Result<()> {
        self.update_key_dir(&cmp_info);
        self.delete_stale_log(cmp_info.dir_path, cmp_info.left, cmp_info.right - 2)?;

        // update dup_byte_size
        self.st.stat.dup_byte_size -= cmp_info.stat.dup_byte_size;
        //  left           right
        // [1, 2, 3, 4, 5, 6)
        //        3, 4, 5, 6)
        // |-------|  |
        //   merged   | active log
        self.st.left = cmp_info.right - 2;

        // rename
        let mut new_name = cmp_info.merged_log_path.clone();
        new_name.pop();
        new_name.push(StableStorage::data_file_name(self.st.left));

        std::fs::rename(&cmp_info.merged_log_path, &new_name).unwrap();

        Ok(())
    }

    fn process_cmd(&mut self, req: Request) -> Result<()> {
        let (cmd, reply_ch) = req;
        let reply = match cmd {
            Cmd::Get(key) => Reply::Get(self.get(key)?),
            Cmd::Set(key, value) => Reply::Set(self.set(key, value).is_ok()),
            Cmd::Remove(key) => Reply::Remove(self.remove(key)?),
            _ => panic!("unsupported cmds"),
        };
        reply_ch.send(reply)?;
        Ok(())
    }

    fn delete_stale_log(&mut self, mut dir_path: PathBuf, left: u32, last: u32) -> Result<()> {
        for fid in left..=last {
            dir_path.push(StableStorage::data_file_name(fid));
            std::fs::remove_file(&dir_path)?;
            debug!("delete stale file: {:?}", &dir_path);
            dir_path.pop();
        }
        Ok(())
    }

    fn compress(
        cmp_recvr: Receiver<CompressInfo>,
        done_sender: SyncSender<CompressInfo>,
    ) -> Result<()> {
        info!("compressor up");
        let mut quit = false;
        loop {
            if let Ok(cmp_info) = cmp_recvr.recv() {
                info!("compressing [{}-{}]", cmp_info.left, cmp_info.right - 2);
                let cmp = Compressor::new(cmp_info);
                match cmp.log_compress() {
                    Err(e) => panic!(e),
                    Ok(cmp_info) => {
                        done_sender.send(cmp_info).unwrap();
                    }
                }
            } else {
                quit = true;
            }
            if quit {
                break;
            }
        }
        info!("compressor down");
        Ok(())
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
            fsz_th: 512 * KB as u32,
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
    pub fn build(&mut self) -> Result<(KvAdaptor, KvStore)> {
        let p: PathBuf = if let Some(ref path) = self.path {
            Path::new(path).to_path_buf()
        } else {
            [".", "stab"].iter().collect::<PathBuf>()
        };
        let (kva, mut kvs) = KvStore::open(p)?;
        kvs.set_data_threshold(self.fsz_th);
        kvs.st.enable_log_compress = self.enable_compress;
        Ok((kva, kvs))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MemDirEntry {
    fid: u32,
    offset: u32,
    sz: u32,
}

impl MemDirEntry {
    fn plain(&self) -> (u32, u32, u32) {
        (self.fid, self.offset, self.sz)
    }
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

#[derive(Debug, Clone, Deserialize, Serialize)]
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
    /// Update diretory entry and return size of stale kv entry.
    fn update(&mut self, k: &Key, fid: u32, offset: u32, sz: u32) -> Result<u32> {
        let e = self.map.entry(k.clone()).or_insert(MemDirEntry::default());
        let stale_sz = e.sz;
        e.fid = fid;
        e.offset = offset;
        e.sz = sz;
        Ok(stale_sz)
    }

    /// return size (in Byte) of stale kv entry.
    #[inline]
    fn remove(&mut self, k: &Key) -> u32 {
        self.map.remove(k).map_or(0, |e| e.sz)
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
    // readers: BTreeMap<u32, BufReader<File>>,
    readers: BTreeMap<u32, BufReader<File>>,

    active_f: Option<BufWriter<File>>,

    // = len(file), Bytes.
    last_file_size: u32,

    // Path of directory storing log files.
    dir_path: PathBuf,

    // total path of active log file.
    active_path: PathBuf,

    enable_log_compress: bool,

    // statistic about data.
    stat: Stat,
}

impl StableStorage {
    fn new() -> StableStorage {
        StableStorage {
            left: 1,
            right: 1,
            fsz_th: 4096,
            readers: BTreeMap::new(),
            active_f: None,
            last_file_size: 0,
            dir_path: PathBuf::new(),
            active_path: PathBuf::new(),
            enable_log_compress: true,
            stat: Stat::new(DEFAULT_DUP_PROP, DEFAULT_DUP_MIN_TH),
        }
    }

    fn last_file_size(&self) -> u32 {
        self.last_file_size
    }

    /// Return Error if there is no such key.
    fn load(&mut self, fid: u32, offset: u32, sz: u32) -> Result<StableEntry> {
        StableStorage::load_raw(self.readers.get_mut(&fid).unwrap(), fid, offset, sz)
            .and_then(|buf| Ok(serde_json::from_slice::<StableEntry>(&buf).unwrap()))
    }

    /// Load raw bytes without deserializaion.
    fn load_raw(reader: &mut BufReader<File>, fid: u32, offset: u32, sz: u32) -> Result<Vec<u8>> {
        /* =====================================================================
        ref: https://github.com/Tsumida/rskvs/issues/2
            offset
            |
            | obj_size: u32 | json_string |
        =====================================================================*/
        let mut buf = vec![0; sz as usize];

        //let reader = self.readers.get_mut(&fid).unwrap();
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
        k: Key,
        v: Value,
        st: StableEntryState,
        stale_sz: u32,
    ) -> Result<(u32, u32, u32)> {
        let se = StableEntry {
            st: st,
            key: k,
            val: v,
        };

        if self.active_f.is_none() || self.reach_threshold() {
            self.create_new_active_file()?;
        }

        if !self.readers.contains_key(&self.active_fid()) {
            self.insert_new_reader(self.active_fid())?;
        }
        // binary format:  | len(json(se)) |        json(se)        |
        //                 | len(json(se)) | state |  key  |  val   |
        let f = self.active_f.as_mut().unwrap().get_mut();
        let s = serde_json::to_string(&se)?;
        let size = s.as_bytes().len() as u32;
        let offset = self.last_file_size;
        self.last_file_size = StableStorage::store(f, offset, s.as_bytes())?;

        self.stat.increase_dup_size(stale_sz as u64);

        Ok((self.active_fid(), offset, size)) // offset and length
    }

    /// Store.
    /// bin format: | len(s) | s |
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
        self.last_file_size >= self.fsz_th
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
    fn update_fid_list(mut dir_path: PathBuf, left: u32, right: u32) -> Result<()> {
        // | st: u32 | end: u32 |  -->  [st, end)
        dir_path.push(PATH_FID_LIST);
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&dir_path)?;

        f.write_u32::<NativeEndian>(left)?;
        f.write_u32::<NativeEndian>(right)?;
        Ok(())
    }

    /// Create new active file. Previous data become immutable.
    fn create_new_active_file(&mut self) -> Result<()> {
        self.right += 1;
        //eprintln!("create active file -- {}", self.active_fid());
        StableStorage::update_fid_list(self.dir_path.clone(), self.left, self.right)?; // record this new fid.

        self.active_path = self.dir_path.clone();
        self.active_path
            .push(StableStorage::data_file_name(self.active_fid()));
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.active_path)?;

        self.last_file_size = f.metadata().unwrap().len() as u32;
        //self.last_file_size = 0;
        //eprintln!("active file size: {}", self.last_file_size);
        self.active_f = Some(BufWriter::new(f));
        Ok(())
    }

    /// Rebuild KeyDir before KVStorage is available for user.
    fn rebuild_from_all(&mut self) -> Result<KeyDir> {
        let mut kd = KeyDir::new();
        let dir_path = self.dir_path.clone();

        // check existance of fids.
        let mut fid_path = dir_path.clone();
        fid_path.push(PATH_FID_LIST);
        if !fid_path.exists() {
            StableStorage::update_fid_list(self.dir_path.clone(), 1, 1).unwrap();
        }

        // read fids from PATH_FID_LIST
        let mut bf = BufReader::new(std::fs::OpenOptions::new().read(true).open(fid_path)?);

        self.update_interval(&mut bf);

        // read all data files.
        self.rebuild_all(&mut kd, dir_path)?;

        if self.right > self.left {
            if !self.reach_threshold() {
                self.right -= 1
            }
            self.create_new_active_file()?;
        }

        Ok(kd)
    }

    fn rebuild_all(&mut self, kd: &mut KeyDir, mut dir_path: PathBuf) -> Result<()> {
        for fid in self.left..self.right {
            dir_path.push(StableStorage::data_file_name(fid));
            self.last_file_size = StableStorage::rebuild_from_single(kd, &dir_path, fid)?;
            dir_path.pop();
            self.insert_new_reader(fid)?;
        }
        Ok(())
    }

    // update [left, right)
    fn update_interval(&mut self, bf: &mut BufReader<File>) {
        match bf.read_u32::<NativeEndian>() {
            Ok(st) => {
                bf.seek(SeekFrom::Start(4)).unwrap();
                self.left = st;
                self.right = bf.read_u32::<NativeEndian>().unwrap();
            }
            _ => {}
        }
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

    fn clean_old_files(&mut self, left: u32, right: u32, tmp_path: &mut PathBuf) {
        for id in left..right {
            tmp_path.push(StableStorage::data_file_name(id));
            std::fs::remove_file(&tmp_path).unwrap();
            self.readers.remove(&id);
            tmp_path.pop();
        }
    }
}

struct Compressor {
    left: u32,
    right: u32,
    dir_path: PathBuf,
    readers: BTreeMap<u32, BufReader<File>>,
    merged_path: PathBuf,
    merged_file: BufWriter<File>,
    kd: KeyDir,
    stat: Stat,
}

impl Compressor {
    /// Take log entires in [left, right), moving them into file numbered right - 1.
    fn log_compress(mut self) -> Result<CompressInfo> {
        let merged_fid = self.right - 2;
        self.merge_stale_log(merged_fid)?;
        // [merged_fid, right)
        let cmp_info = CompressInfo {
            // left: merged_fid,
            left: self.left,
            right: self.right,
            stat: self.stat.clone(),
            dir_path: self.dir_path,
            merged_log_path: self.merged_path,
            kd: self.kd,
        };

        Ok(cmp_info)
    }

    fn merge_stale_log(&mut self, merged_fid: u32) -> Result<u32> {
        let mut new_log = &mut self.merged_file;
        let last = self.right - 2;
        // size of merged file.
        let mut total_bs = 0;

        let kd = &mut self.kd;
        // move all entry in kd to new log file.
        for (k, v) in kd.map.iter_mut() {
            if v.fid < self.left {
                panic!("invalid fid"); // log is copressed and deleted.
            }
            if v.fid > last {
                // kv pair in active file.
                continue;
            }
            let buf = StableStorage::load_raw(
                self.readers.get_mut(&v.fid).unwrap(),
                v.fid,
                v.offset,
                v.sz,
            )?;
            let obj_sz = buf.len() as u32;
            StableStorage::store(&mut new_log, total_bs, &buf)?;

            v.fid = merged_fid;
            v.offset = total_bs;
            v.sz = obj_sz;

            assert_eq!(obj_sz, v.sz);
            total_bs += obj_sz + 4;
        }
        debug!("merged log size: {} bytes", total_bs);
        Ok(total_bs as u32)
    }

    fn new(cmp_info: CompressInfo) -> Self {
        let mut readers = BTreeMap::new();
        let mut dir_path = cmp_info.dir_path.clone();
        let (left, right) = (cmp_info.left, cmp_info.right);

        for fid in left..right - 1 {
            dir_path.push(StableStorage::data_file_name(fid));
            let bfr = BufReader::new(std::fs::File::open(&dir_path).unwrap());
            readers.insert(fid, bfr);
            dir_path.pop();
        }

        let mut merged_path = dir_path.clone();
        // active file excluded.
        merged_path.push(format!("{}.mege", right - 2));
        let bfw = BufWriter::new(std::fs::File::create(&merged_path).unwrap());
        Compressor {
            left: cmp_info.left,
            right: cmp_info.right,
            dir_path: cmp_info.dir_path,
            readers,
            merged_path,
            merged_file: bfw,
            kd: cmp_info.kd,
            stat: cmp_info.stat,
        }
    }
}
