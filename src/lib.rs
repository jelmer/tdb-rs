#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/tdb_sys.rs"));

use std::ffi::CStr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, RawFd};

pub struct Tdb(*mut tdb_context);

#[derive(Debug)]
pub enum Error {
    Corrupt,
    IO,
    Lock,
    OOM,
    Exists,
    NoLock,
    LockTimeout,
    ReadOnly,
    NoExist,
    Invalid,
    Nesting,
    Unknown(u32),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let msg = match self {
            Error::Corrupt => "Corrupt",
            Error::IO => "IO",
            Error::Lock => "Lock",
            Error::OOM => "OOM",
            Error::Exists => "Exists",
            Error::NoLock => "NoLock",
            Error::LockTimeout => "LockTimeout",
            Error::ReadOnly => "ReadOnly",
            Error::NoExist => "NoExist",
            Error::Invalid => "Invalid",
            Error::Nesting => "Nesting",
            Error::Unknown(e) => return write!(f, "Unknown({})", e),
        };
        write!(f, "{}", msg)
    }
}

impl std::error::Error for Error {}

impl From<u32> for Error {
    fn from(e: u32) -> Self {
        match e {
            TDB_ERROR_TDB_ERR_CORRUPT => Error::Corrupt,
            TDB_ERROR_TDB_ERR_IO => Error::IO,
            TDB_ERROR_TDB_ERR_LOCK => Error::Lock,
            TDB_ERROR_TDB_ERR_OOM => Error::OOM,
            TDB_ERROR_TDB_ERR_EXISTS => Error::Exists,
            TDB_ERROR_TDB_ERR_NOLOCK => Error::NoLock,
            TDB_ERROR_TDB_ERR_LOCK_TIMEOUT => Error::LockTimeout,
            TDB_ERROR_TDB_ERR_RDONLY => Error::ReadOnly,
            TDB_ERROR_TDB_ERR_NOEXIST => Error::NoExist,
            TDB_ERROR_TDB_ERR_EINVAL => Error::Invalid,
            TDB_ERROR_TDB_ERR_NESTING => Error::Nesting,
            _ => Error::Unknown(e),
        }
    }
}

impl From<i32> for Error {
    fn from(e: i32) -> Self {
        From::<u32>::from(e as u32)
    }
}

#[repr(C)]
pub struct TDB_DATA {
    pub dptr: *const std::os::raw::c_uchar,
    pub dsize: usize,
}

impl From<&[u8]> for TDB_DATA {
    fn from(data: &[u8]) -> Self {
        TDB_DATA {
            dptr: data.as_ptr(),
            dsize: data.len(),
        }
    }
}

impl From<TDB_DATA> for &[u8] {
    fn from(data: TDB_DATA) -> Self {
        unsafe { std::slice::from_raw_parts(data.dptr, data.dsize) }
    }
}

impl From<TDB_DATA> for Vec<u8> {
    fn from(data: TDB_DATA) -> Self {
        unsafe { std::slice::from_raw_parts(data.dptr, data.dsize) }.to_vec()
    }
}

impl Tdb {
    /// Open the database and creating it if necessary.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the db to open.
    /// * `hash_size` - The hash size is advisory, leave None for a default.
    /// * `tdb_flags` The flags to use to open the db:
    ///     TDB_CLEAR_IF_FIRST - Clear database if we are the only one with it open
    ///     TDB_NOLOCK - Don't do any locking
    ///     TDB_NOMMAP - Don't use mmap
    ///     TDB_NOSYNC - Don't synchronise transactions to disk
    ///     TDB_SEQNUM - Maintain a sequence number
    ///     TDB_VOLATILE - activate the per-hashchain freelist, default 5.
    ///     TDB_ALLOW_NESTING - Allow transactions to nest.
    ///     TDB_DISALLOW_NESTING - Disallow transactions to nest.
    ///     TDB_INCOMPATIBLE_HASH - Better hashing: can't be opened by tdb < 1.2.6.
    ///     TDB_MUTEX_LOCKING - Optimized locking using robust mutexes if supported, can't be opened by tdb < 1.3.0.
    ///         Only valid in combination with TDB_CLEAR_IF_FIRST after checking tdb_runtime_check_for_robust_mutexes()
    /// * `open_flags` Flags for the open(2) function.
    pub fn open(
        name: &std::path::Path,
        hash_size: Option<u32>,
        tdb_flags: u32,
        open_flags: i32,
    ) -> Option<Tdb> {
        let hash_size = hash_size.unwrap_or(0);
        let ret = unsafe {
            tdb_open(
                name.as_os_str().as_bytes().as_ptr() as *const i8,
                hash_size as i32,
                tdb_flags as i32,
                open_flags,
                0,
            )
        };
        if ret.is_null() {
            None
        } else {
            Some(Tdb(ret))
        }
    }

    pub fn memory(hash_size: Option<u32>, tdb_flags: u32) -> Option<Tdb> {
        let hash_size = hash_size.unwrap_or(0);
        let ret = unsafe {
            tdb_open(
                b":memory:\0".as_ptr() as *const i8,
                hash_size as i32,
                tdb_flags as i32,
                0,
                0,
            )
        };
        if ret.is_null() {
            None
        } else {
            Some(Tdb(ret))
        }
    }

    fn error(&self) -> Result<(), Error> {
        let err = unsafe { tdb_error(self.0) };
        if err == 0 {
            Ok(())
        } else {
            Err(err.into())
        }
    }

    pub fn reopen(&mut self) -> Result<(), Error> {
        let ret = unsafe { tdb_reopen(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn fetch(&self, key: &[u8]) -> Result<Option<&[u8]>, Error> {
        let ret = unsafe { tdb_fetch(self.0, key.into()) };
        if ret.dptr.is_null() {
            match self.error() {
                Err(Error::NoExist) => Ok(None),
                Err(e) => Err(e),
                Ok(_) => panic!("error but no error?"),
            }
        } else {
            Ok(Some(ret.into()))
        }
    }

    pub fn store(&mut self, key: &[u8], val: &[u8], flag: u32) -> Result<(), Error> {
        let ret = unsafe { tdb_store(self.0, key.into(), val.into(), flag as i32) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        let ret = unsafe { tdb_delete(self.0, key.into()) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn append(&mut self, key: &[u8], val: &[u8]) -> Result<(), Error> {
        let ret = unsafe { tdb_append(self.0, key.into(), val.into()) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &[u8]> + '_ {
        TdbKeys(self, None)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        TdbIter(self, TdbKeys(self, None))
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        unsafe { tdb_exists(self.0, key.into()) == 1 }
    }

    pub fn lockall(&self) -> Result<(), Error> {
        let ret = unsafe { tdb_lockall(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn unlockall(&self) -> Result<(), Error> {
        let ret = unsafe { tdb_unlockall(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn lockall_nonblock(&self) -> Result<(), Error> {
        let ret = unsafe { tdb_lockall_nonblock(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn lockall_read(&self) -> Result<(), Error> {
        let ret = unsafe { tdb_lockall_read(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn lockall_read_nonblock(&self) -> Result<(), Error> {
        let ret = unsafe { tdb_lockall_read_nonblock(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn name(&self) -> &str {
        unsafe { CStr::from_ptr(tdb_name(self.0)) }
            .to_str()
            .unwrap()
    }

    pub fn hash_size(&self) -> u32 {
        unsafe { tdb_hash_size(self.0) as u32 }
    }

    pub fn map_size(&self) -> u32 {
        unsafe { tdb_map_size(self.0) as u32 }
    }

    pub fn get_seqnum(&self) -> u64 {
        unsafe { tdb_get_seqnum(self.0) as u64 }
    }

    pub fn get_flags(&self) -> u32 {
        unsafe { tdb_get_flags(self.0) as u32 }
    }

    pub fn add_flags(&mut self, flags: u32) {
        unsafe { tdb_add_flags(self.0, flags) };
    }

    pub fn remove_flags(&mut self, flags: u32) {
        unsafe { tdb_remove_flags(self.0, flags) };
    }

    pub fn enable_seqnum(&mut self) {
        unsafe { tdb_enable_seqnum(self.0) };
    }

    pub fn increment_seqnum_nonblock(&mut self) {
        unsafe { tdb_increment_seqnum_nonblock(self.0) };
    }

    pub fn repack(&mut self) -> Result<(), Error> {
        let ret = unsafe { tdb_repack(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn wipe_all(&mut self) -> Result<(), Error> {
        let ret = unsafe { tdb_wipe_all(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn summary(&self) -> String {
        let buf = unsafe { tdb_summary(self.0) };
        unsafe { CStr::from_ptr(buf) }.to_str().unwrap().to_owned()
    }

    pub fn freelist_size(&self) -> u32 {
        unsafe { tdb_freelist_size(self.0) as u32 }
    }

    pub fn transaction_start(&mut self) -> Result<(), Error> {
        let ret = unsafe { tdb_transaction_start(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn transaction_active(&self) -> bool {
        unsafe { tdb_transaction_active(self.0) }
    }

    pub fn transaction_start_nonblock(&mut self) -> Result<(), Error> {
        let ret = unsafe { tdb_transaction_start_nonblock(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn transaction_prepare_commit(&mut self) -> Result<(), Error> {
        let ret = unsafe { tdb_transaction_prepare_commit(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn transaction_commit(&mut self) -> Result<(), Error> {
        let ret = unsafe { tdb_transaction_commit(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }

    pub fn transaction_cancel(&mut self) -> Result<(), Error> {
        let ret = unsafe { tdb_transaction_cancel(self.0) };
        if ret == -1 {
            self.error()
        } else {
            Ok(())
        }
    }
}

impl AsRawFd for Tdb {
    fn as_raw_fd(&self) -> RawFd {
        unsafe { tdb_fd(self.0) }
    }
}

struct TdbKeys<'a>(&'a Tdb, Option<Vec<u8>>);

impl<'a> Iterator for TdbKeys<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        let key = if let Some(prev_key) = self.1.take() {
            unsafe { tdb_nextkey(self.0 .0, prev_key.as_slice().into()) }
        } else {
            unsafe { tdb_firstkey(self.0 .0) }
        };
        if key.dptr.is_null() {
            match self.0.error() {
                Err(Error::NoExist) | Ok(_) => None,
                Err(e) => panic!("error: {}", e),
            }
        } else {
            let ret: &[u8] = key.into();
            self.1 = Some(ret.to_vec());
            Some(ret)
        }
    }
}

struct TdbIter<'a>(&'a Tdb, TdbKeys<'a>);

impl<'a> Iterator for TdbIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<(&'a [u8], &'a [u8])> {
        let key = self.1.next()?;
        let val = self.0.fetch(key).unwrap().unwrap();
        Some((key, val))
    }
}

impl Drop for Tdb {
    fn drop(&mut self) {
        unsafe { tdb_close(self.0) };
    }
}

pub fn jenkins_hash(key: &[u8]) -> u32 {
    let mut key = key.into();
    unsafe { tdb_jenkins_hash(&mut key) }
}

#[cfg(test)]
mod test {
    fn testtdb() -> super::Tdb {
        let tmppath = tempfile::tempdir().unwrap();
        let path = tmppath.path().join("test.tdb");
        super::Tdb::open(path.as_path(), None, 0, libc::O_RDWR | libc::O_CREAT).unwrap()
    }

    #[test]
    fn test_simple() {
        let mut tdb = testtdb();
        tdb.store(b"foo", b"bar", 0).unwrap();
        assert_eq!(tdb.fetch(b"foo").unwrap().unwrap(), b"bar");
        tdb.delete(b"foo").unwrap();
        assert_eq!(tdb.fetch(b"foo").unwrap(), None);
    }

    #[test]
    fn test_iter() {
        let mut tdb = testtdb();

        tdb.store(b"foo", b"bar", 0).unwrap();
        tdb.store(b"blah", b"bloe", 0).unwrap();

        let mut iter = tdb.iter();
        assert_eq!(iter.next().unwrap(), (&b"foo"[..], &b"bar"[..]));
        assert_eq!(iter.next().unwrap(), (&b"blah"[..], &b"bloe"[..]));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_keys() {
        let mut tdb = testtdb();

        tdb.store(b"foo", b"bar", 0).unwrap();
        tdb.store(b"blah", b"bloe", 0).unwrap();

        let mut keys = tdb.keys();
        assert_eq!(keys.next().unwrap(), b"foo");
        assert_eq!(keys.next().unwrap(), b"blah");
        assert_eq!(keys.next(), None);
    }

    #[test]
    fn test_transaction() {
        let mut tdb = testtdb();

        tdb.transaction_start().unwrap();
        tdb.store(b"foo", b"bar", 0).unwrap();
        tdb.transaction_cancel().unwrap();
        assert_eq!(tdb.fetch(b"foo").unwrap(), None);

        tdb.transaction_start().unwrap();
        tdb.store(b"foo", b"bar", 0).unwrap();
        tdb.transaction_prepare_commit().unwrap();
        tdb.transaction_commit().unwrap();
        assert_eq!(tdb.fetch(b"foo").unwrap().unwrap(), b"bar");
    }

    #[test]
    fn test_fetch_nonexistent() {
        let mut tdb = testtdb();
        assert_eq!(tdb.fetch(b"foo").unwrap(), None);
    }

    #[test]
    fn test_store_overwrite() {
        let mut tdb = testtdb();
        tdb.store(b"foo", b"bar", 0).unwrap();
        tdb.store(b"foo", b"blah", 0).unwrap();
        assert_eq!(tdb.fetch(b"foo").unwrap().unwrap(), b"blah");
    }
}
