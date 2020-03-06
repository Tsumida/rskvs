#![deny(missing_docs)]
//! rskvs is a little key-value storage.

use std::collections::HashMap;
use std::path::PathBuf;

use failure::{Error};

type Result<T> = std::result::Result<T, Error>;

/// A KvStoree offers method get/set/remove methods like HashMap.
/// Additionally, KvStore provides stablizability.
pub struct KvStore{
    sto: HashMap<String, String>,
}

impl KvStore{
    /// Create a empty KvStore.
    pub fn new()->Self{
        KvStore{
            sto:HashMap::new(),
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
    pub fn set(&mut self, key:String, value:String) -> Result<()>{
        let _ = self.sto.insert(key, value);
        Ok(())
    }

    /// Get value for a given key, which is  wrapped in Option.
    /// # Example
    /// ```
    /// use rskvs::KvStore;
    /// 
    /// let mut kvs = KvStore::new();
    /// kvs.set("abc".to_string(), "def".to_string());
    /// assert_eq!("def".to_string(), kvs.get("abc".to_string()).unwrap().unwrap());
    /// 
    /// ```
    pub fn get(&mut self, key:String) -> Result<Option<String>>{
        match self.sto.get(&key){
            None => Ok(None),
            Some(st) => Ok(Some(st.to_string())),
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
    pub fn remove(&mut self, key:String) -> Result<()>{
        let _ = self.sto.remove(&key);
        Ok(())
    }

    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore>{
        Ok(KvStore::new())
    }
}



