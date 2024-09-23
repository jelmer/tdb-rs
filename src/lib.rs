#![deny(missing_docs)]
//! Rust bindings for TDB (Trivial Database)
//!
//! TDB is a simple database that provides a key-value store. It is designed to be fast and
//! reliable, and is used by Samba for storing data. It supports multiple readers and
//! writers at the same time.
//!
//! This crate provides a safe, rustic wrapper around the TDB C API.
//!
//! # Example
//!
//! ```rust
//! use trivialdb::{Flags,Tdb};
//!
//! let mut tdb = Tdb::memory(None, Flags::empty()).unwrap();
//! tdb.store(b"foo", b"bar", None).unwrap();
//! assert_eq!(tdb.fetch(b"foo").unwrap().unwrap(), b"bar");
//! ```
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

mod generated {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/tdb_sys.rs"));

    #[repr(C)]
    pub struct TDB_DATA {
        pub dptr: *mut std::os::raw::c_uchar,
        pub dsize: usize,
    }
}

use generated::TDB_DATA;

use bitflags::bitflags;
use std::ffi::CStr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, RawFd};

pub use libc::{O_CREAT, O_RDONLY, O_RDWR, O_TRUNC};

/// A Trivial Database
pub struct Tdb(*mut generated::tdb_context);

/// Errors that can occur when interacting with a Trivial Database
#[derive(Debug)]
pub enum Error {
    /// Database is corrupt
    Corrupt,
    /// I/O error
    IO,
    /// Locked
    Lock,
    /// Out of memory
    OOM,
    /// Entry Exists
    Exists,
    /// No Lock
    NoLock,
    /// Lock timeout expired
    LockTimeout,
    /// Database is read-only
    ReadOnly,
    /// Entry does not exist
    NoExist,
    /// Invalid error
    Invalid,

    /// Nesting while that was not allowed
    Nesting,
}

bitflags! {
    /// Flags for opening a database
    pub struct Flags: u32 {
        /// Clear database if we are the only one with it open
        const ClearIfFirst = generated::TDB_CLEAR_IF_FIRST;
        /// Don't use a file, instead store the data in memory. The fuile name is ignored in this
        /// case.
        const Internal = generated::TDB_INTERNAL;
        /// Don't use mmap
        const NoMmap = generated::TDB_NOMMAP;
        /// Don't do any locking
        const NoLock = generated::TDB_NOLOCK;
        /// Don't synchronise transactions to disk
        const NoSync = generated::TDB_SEQNUM;
        /// Maintain a sequence number
        const Seqnum = generated::TDB_SEQNUM;
        /// activate the per-hashchain freelist, default 5.
        const Volatile = generated::TDB_VOLATILE;
        /// Allow transactions to nest.
        const AllowNesting = generated::TDB_ALLOW_NESTING;
        /// Disallow transactions to nest.
        const DisallowNesting = generated::TDB_DISALLOW_NESTING;
        /// Better hashing: can't be opened by tdb < 1.2.6.
        const IncompatibleHash = generated::TDB_INCOMPATIBLE_HASH;
        /// Optimized locking using robust mutexes if supported, can't be opened by tdb < 1.3.0.
        /// Only valid in combination with TDB_CLEAR_IF_FIRST after checking tdb_runtime_check_for_robust_mutexes()
        const MutexLocking = generated::TDB_MUTEX_LOCKING;
    }
}

impl Default for Flags {
    fn default() -> Self {
        Flags::empty()
    }
}

/// Store option Flags
#[repr(C)]
pub enum StoreFlags {
    /// Don't overwrite an existing entry.
    Insert = generated::TDB_INSERT as isize,

    /// Don't create a new entry.
    Replace = generated::TDB_REPLACE as isize,

    /// Don't create an existing entry.
    Modify = generated::TDB_MODIFY as isize,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let msg = match self {
            Error::Corrupt => "Database is corrupt",
            Error::IO => "I/O error",
            Error::Lock => "Locked",
            Error::OOM => "OOM",
            Error::Exists => "Exists",
            Error::NoLock => "NoLock",
            Error::LockTimeout => "Lock timeout expired",
            Error::ReadOnly => "Database is read-only",
            Error::NoExist => "NoExist",
            Error::Invalid => "Invalid",
            Error::Nesting => "Nesting",
        };
        write!(f, "{}", msg)
    }
}

impl std::error::Error for Error {}

impl From<u32> for Error {
    fn from(e: u32) -> Self {
        match e {
            generated::TDB_ERROR_TDB_ERR_CORRUPT => Error::Corrupt,
            generated::TDB_ERROR_TDB_ERR_IO => Error::IO,
            generated::TDB_ERROR_TDB_ERR_LOCK => Error::Lock,
            generated::TDB_ERROR_TDB_ERR_OOM => Error::OOM,
            generated::TDB_ERROR_TDB_ERR_EXISTS => Error::Exists,
            generated::TDB_ERROR_TDB_ERR_NOLOCK => Error::NoLock,
            generated::TDB_ERROR_TDB_ERR_LOCK_TIMEOUT => Error::LockTimeout,
            generated::TDB_ERROR_TDB_ERR_RDONLY => Error::ReadOnly,
            generated::TDB_ERROR_TDB_ERR_NOEXIST => Error::NoExist,
            generated::TDB_ERROR_TDB_ERR_EINVAL => Error::Invalid,
            generated::TDB_ERROR_TDB_ERR_NESTING => Error::Nesting,
            _ => panic!("Unknown error code: {}", e),
        }
    }
}

impl From<i32> for Error {
    fn from(e: i32) -> Self {
        From::<u32>::from(e as u32)
    }
}

impl From<Vec<u8>> for TDB_DATA {
    fn from(data: Vec<u8>) -> Self {
        let ptr = data.as_ptr() as *mut std::os::raw::c_uchar;
        let len = data.len();
        std::mem::forget(data);
        TDB_DATA {
            dptr: ptr,
            dsize: len,
        }
    }
}

impl Drop for TDB_DATA {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.dptr as *mut libc::c_void);
        }
    }
}

impl Clone for TDB_DATA {
    fn clone(&self) -> Self {
        unsafe {
            let ptr = libc::malloc(self.dsize) as *mut std::os::raw::c_uchar;
            std::ptr::copy_nonoverlapping(self.dptr, ptr, self.dsize);
            TDB_DATA {
                dptr: ptr,
                dsize: self.dsize,
            }
        }
    }
}

impl From<TDB_DATA> for Vec<u8> {
    fn from(mut data: TDB_DATA) -> Self {
        let ret = unsafe { Vec::from_raw_parts(data.dptr, data.dsize, data.dsize) };
        data.dptr = std::ptr::null_mut();
        ret
    }
}

#[repr(C)]
struct CONST_TDB_DATA {
    pub dptr: *const std::os::raw::c_uchar,
    pub dsize: usize,
}

impl From<&[u8]> for CONST_TDB_DATA {
    fn from(data: &[u8]) -> Self {
        CONST_TDB_DATA {
            dptr: data.as_ptr(),
            dsize: data.len(),
        }
    }
}

extern "C" {
    fn tdb_fetch(tdb: *mut generated::tdb_context, key: CONST_TDB_DATA) -> TDB_DATA;

    fn tdb_store(
        tdb: *mut generated::tdb_context,
        key: CONST_TDB_DATA,
        dbuf: CONST_TDB_DATA,
        flag: ::std::os::raw::c_int,
    ) -> ::std::os::raw::c_int;

    fn tdb_append(
        tdb: *mut generated::tdb_context,
        key: CONST_TDB_DATA,
        new_dbuf: CONST_TDB_DATA,
    ) -> ::std::os::raw::c_int;

    fn tdb_exists(tdb: *mut generated::tdb_context, key: CONST_TDB_DATA) -> bool;

    fn tdb_delete(tdb: *mut generated::tdb_context, key: CONST_TDB_DATA) -> ::std::os::raw::c_int;

    fn tdb_nextkey(tdb: *mut generated::tdb_context, key: CONST_TDB_DATA) -> TDB_DATA;
}

impl Tdb {
    /// Open the database and creating it if necessary.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the db to open.
    /// * `hash_size` - The hash size is advisory, leave None for a default.
    /// * `tdb_flags` The flags to use to open the db:
    /// * `open_flags` Flags for the open(2) function.
    /// * `mode` The mode to use for the open(2) function.
    pub fn open<P: AsRef<std::path::Path>>(
        name: P,
        hash_size: Option<u32>,
        tdb_flags: Flags,
        open_flags: i32,
        mode: u32,
    ) -> Option<Tdb> {
        let name = name.as_ref();
        let hash_size = hash_size.unwrap_or(0);
        let ret = unsafe {
            generated::tdb_open(
                name.as_os_str().as_bytes().as_ptr() as *const std::os::raw::c_char,
                hash_size as i32,
                tdb_flags.bits() as i32,
                open_flags,
                mode,
            )
        };
        if ret.is_null() {
            None
        } else {
            Some(Tdb(ret))
        }
    }

    /// Create a database in memory
    ///
    /// # Arguments
    ///
    /// * `hash_size` - The hash size is advisory, leave None for a default.
    /// * `tdb_flags` The flags to use to open the db:
    pub fn memory(hash_size: Option<u32>, mut tdb_flags: Flags) -> Option<Tdb> {
        let hash_size = hash_size.unwrap_or(0);
        tdb_flags.insert(Flags::Internal);
        let ret = unsafe {
            generated::tdb_open(
                b":memory:\0".as_ptr() as *const std::os::raw::c_char,
                hash_size as i32,
                tdb_flags.bits() as i32,
                O_RDWR | O_CREAT,
                0,
            )
        };
        if ret.is_null() {
            None
        } else {
            Some(Tdb(ret))
        }
    }

    /// Return the latest error that occurred
    fn error(&self) -> Result<(), Error> {
        let err = unsafe { generated::tdb_error(self.0) };
        if err == 0 {
            Ok(())
        } else {
            Err(err.into())
        }
    }

    /// Set the maximum number of dead records per hash chain.
    pub fn set_max_dead(&mut self, max_dead: u32) {
        unsafe { generated::tdb_set_max_dead(self.0, max_dead as i32) };
    }

    /// Reopen the database
    ///
    /// This can be used to reopen a database after a fork, to ensure that we have an independent
    /// seek pointer and to re-establish any locks.
    pub fn reopen(&mut self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_reopen(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Fetch a value from the database.:w
    ///
    /// # Arguments
    /// * `key` - The key to fetch.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` - The value associated with the key.
    /// * `Ok(None)` - The key was not found.
    /// * `Err(e)` - An error occurred.
    pub fn fetch(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let ret = unsafe { tdb_fetch(self.0, key.into()) };
        if ret.dptr.is_null() {
            match self.error() {
                Err(Error::NoExist) => Ok(None),
                Err(e) => Err(e),
                Ok(_) => panic!("error but no error?"),
            }
        } else {
            // TODO(jelmer): Call Vec::from_raw_parts_in here once the allocator API is stable.
            // https://github.com/rust-lang/rust/issues/32838
            Ok(Some(ret.into()))
        }
    }

    /// Store a key/value pair in the database.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store.
    /// * `val` - The value to store.
    /// * `flags` - The flags to use when storing the value.
    pub fn store(
        &mut self,
        key: &[u8],
        val: &[u8],
        flags: Option<StoreFlags>,
    ) -> Result<(), Error> {
        let flags = flags.map_or(0, |f| f as i32);
        let ret = unsafe { tdb_store(self.0, key.into(), val.into(), flags) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Delete a key from the database.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        let ret = unsafe { tdb_delete(self.0, key.into()) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Append a value to an existing key.
    ///
    /// # Arguments
    /// * `key` - The key to append to.
    /// * `val` - The value to append.
    pub fn append(&mut self, key: &[u8], val: &[u8]) -> Result<(), Error> {
        let ret = unsafe { tdb_append(self.0, key.into(), val.into()) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Iterate over all keys in the database.
    pub fn keys(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
        TdbKeys(self, None)
    }

    /// Iterate over all key/value pairs in the database.
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        TdbIter(self, TdbKeys(self, None))
    }

    /// Check if a particular key exists
    pub fn exists(&self, key: &[u8]) -> bool {
        unsafe { tdb_exists(self.0, key.into()) }
    }

    /// Lock the database
    pub fn lockall(&self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_lockall(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Unlock the database
    pub fn unlockall(&self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_unlockall(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Lock the database, non-blocking
    pub fn lockall_nonblock(&self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_lockall_nonblock(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Lock the database for reading
    pub fn lockall_read(&self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_lockall_read(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Lock the database for reading, non-blocking
    pub fn lockall_read_nonblock(&self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_lockall_read_nonblock(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Return the name of the database
    pub fn name(&self) -> &str {
        unsafe { CStr::from_ptr(generated::tdb_name(self.0)) }
            .to_str()
            .unwrap()
    }

    /// Return the hash size used by the database
    pub fn hash_size(&self) -> u32 {
        unsafe { generated::tdb_hash_size(self.0) as u32 }
    }

    /// Return the map size used by the database
    pub fn map_size(&self) -> u32 {
        unsafe { generated::tdb_map_size(self.0) as u32 }
    }

    /// Return the current sequence number
    pub fn get_seqnum(&self) -> u64 {
        unsafe { generated::tdb_get_seqnum(self.0) as u64 }
    }

    /// Return the current flags
    pub fn get_flags(&self) -> Flags {
        Flags::from_bits_truncate(unsafe { generated::tdb_get_flags(self.0) as u32 })
    }

    /// Add a flag
    pub fn add_flags(&mut self, flags: Flags) {
        unsafe { generated::tdb_add_flags(self.0, flags.bits()) };
    }

    /// Remove a flag
    pub fn remove_flags(&mut self, flags: Flags) {
        unsafe { generated::tdb_remove_flags(self.0, flags.bits()) };
    }

    /// Enable sequence numbers
    pub fn enable_seqnum(&mut self) {
        unsafe { generated::tdb_enable_seqnum(self.0) };
    }

    /// Increment the sequence number
    pub fn increment_seqnum_nonblock(&mut self) {
        unsafe { generated::tdb_increment_seqnum_nonblock(self.0) };
    }

    /// Repack the database
    pub fn repack(&mut self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_repack(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Wipe the database
    pub fn wipe_all(&mut self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_wipe_all(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Return a string summarizing the database
    pub fn summary(&self) -> String {
        let buf = unsafe { generated::tdb_summary(self.0) };
        unsafe { CStr::from_ptr(buf) }.to_str().unwrap().to_owned()
    }

    /// Return the freelist size
    pub fn freelist_size(&self) -> u32 {
        unsafe { generated::tdb_freelist_size(self.0) as u32 }
    }

    /// Start a new transaction
    pub fn transaction_start(&mut self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_transaction_start(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Check if a transaction is active
    pub fn transaction_active(&self) -> bool {
        unsafe { generated::tdb_transaction_active(self.0) }
    }

    /// Start a new transaction, non-blocking
    pub fn transaction_start_nonblock(&mut self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_transaction_start_nonblock(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Prepare to commit a transaction
    pub fn transaction_prepare_commit(&mut self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_transaction_prepare_commit(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Commit a transaction
    pub fn transaction_commit(&mut self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_transaction_commit(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    /// Cancel a transaction
    pub fn transaction_cancel(&mut self) -> Result<(), Error> {
        let ret = unsafe { generated::tdb_transaction_cancel(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }
}

impl AsRawFd for Tdb {
    fn as_raw_fd(&self) -> RawFd {
        unsafe { generated::tdb_fd(self.0) }
    }
}

struct TdbKeys<'a>(&'a Tdb, Option<Vec<u8>>);

impl<'a> Iterator for TdbKeys<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        let key = if let Some(prev_key) = self.1.take() {
            unsafe { tdb_nextkey(self.0 .0, prev_key.as_slice().into()) }
        } else {
            unsafe { generated::tdb_firstkey(self.0 .0) }
        };
        if key.dptr.is_null() {
            match self.0.error() {
                Err(Error::NoExist) | Ok(_) => None,
                Err(e) => panic!("error: {}", e),
            }
        } else {
            let ret: Vec<u8> = key.into();
            self.1 = Some(ret.clone());
            Some(ret)
        }
    }
}

struct TdbIter<'a>(&'a Tdb, TdbKeys<'a>);

impl<'a> Iterator for TdbIter<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let key = self.1.next()?;
        let val = self.0.fetch(key.as_slice()).unwrap().unwrap();
        Some((key, val))
    }
}

impl Drop for Tdb {
    fn drop(&mut self) {
        unsafe { generated::tdb_close(self.0) };
    }
}

/// Generate the jenkins hash of a key
pub fn jenkins_hash(key: Vec<u8>) -> u32 {
    let mut key = key.into();
    unsafe { generated::tdb_jenkins_hash(&mut key) }
}

#[cfg(test)]
mod test {
    fn testtdb() -> super::Tdb {
        let tmppath = tempfile::tempdir().unwrap();
        let path = tmppath.path().join("test.tdb");
        super::Tdb::open(
            path.as_path(),
            None,
            super::Flags::empty(),
            libc::O_RDWR | libc::O_CREAT,
            0o600,
        )
        .unwrap()
    }

    #[test]
    fn test_memory() {
        let mut tdb = super::Tdb::memory(None, super::Flags::empty()).unwrap();
        assert!(!tdb.exists(b"foo"));
        tdb.store(b"foo", b"bar", None).unwrap();
        assert!(tdb.exists(b"foo"));
        assert_eq!(tdb.fetch(b"foo").unwrap().unwrap(), b"bar");
        tdb.delete(b"foo").unwrap();
        assert_eq!(tdb.fetch(b"foo").unwrap(), None);
    }

    #[test]
    fn test_simple() {
        let mut tdb = testtdb();
        assert!(!tdb.exists(b"foo"));
        tdb.store(b"foo", b"bar", None).unwrap();
        assert!(tdb.exists(b"foo"));
        assert_eq!(tdb.fetch(b"foo").unwrap().unwrap(), b"bar");
        tdb.delete(b"foo").unwrap();
        assert_eq!(tdb.fetch(b"foo").unwrap(), None);
    }

    #[test]
    fn test_iter() {
        let mut tdb = testtdb();

        tdb.store(b"foo", b"bar", None).unwrap();
        tdb.store(b"blah", b"bloe", None).unwrap();

        let mut iter = tdb.iter();
        assert_eq!(iter.next().unwrap(), (b"foo".to_vec(), b"bar".to_vec()));
        assert_eq!(iter.next().unwrap(), (b"blah".to_vec(), b"bloe".to_vec()));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_keys() {
        let mut tdb = testtdb();

        tdb.store(b"foo", b"bar", None).unwrap();
        tdb.store(b"blah", b"bloe", None).unwrap();

        let mut keys = tdb.keys();
        assert_eq!(keys.next().unwrap(), b"foo");
        assert_eq!(keys.next().unwrap(), b"blah");
        assert_eq!(keys.next(), None);
    }

    #[test]
    fn test_transaction() {
        let mut tdb = testtdb();

        tdb.transaction_start().unwrap();
        tdb.store(b"foo", b"bar", None).unwrap();
        tdb.transaction_cancel().unwrap();
        assert_eq!(tdb.fetch(b"foo").unwrap(), None);

        tdb.transaction_start().unwrap();
        tdb.store(b"foo", b"bar", None).unwrap();
        tdb.transaction_prepare_commit().unwrap();
        tdb.transaction_commit().unwrap();
        assert_eq!(tdb.fetch(b"foo").unwrap().unwrap(), b"bar");
    }

    #[test]
    fn test_fetch_nonexistent() {
        let tdb = testtdb();
        assert_eq!(tdb.fetch(b"foo").unwrap(), None);
    }

    #[test]
    fn test_store_overwrite() {
        let mut tdb = testtdb();
        tdb.store(b"foo", b"bar", None).unwrap();
        tdb.store(b"foo", b"blah", None).unwrap();
        assert_eq!(tdb.fetch(b"foo").unwrap().unwrap(), b"blah");
    }
}
