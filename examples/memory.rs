use trivialdb::{Tdb,Flags};

fn main() {
    let mut tdb = Tdb::memory(None, Flags::default()).unwrap();

    tdb.store(b"key", b"value", None).unwrap();
    assert_eq!(Some(b"value".to_vec()), tdb.fetch(b"key").unwrap());
}
