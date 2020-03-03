/*
KvStore::set(&mut self, key: String, value: String)

Set the value of a string key to a string

KvStore::get(&mut self, key: String) -> Option<String>

Get the string value of the a string key. If the key does not exist, return None.

KvStore::remove(&mut self, key: String)

*/


pub struct KvStore{
    
}

impl KvStore{
    pub fn new()->Self{
        KvStore{

        }
    }

    pub fn set(&mut self, key:String, value:String){
        panic!()
    }

    pub fn get(&mut self, key:String) -> Option<String>{
        panic!()
    }

    pub fn remove(&mut self, key:String){
        panic!()
    }
}