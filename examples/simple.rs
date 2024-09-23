use trivialdb::{Flags, Tdb};

fn main() {
    let mut tdb = Tdb::open(
        "simple.db",
        None,
        Flags::empty(),
        trivialdb::O_CREAT | trivialdb::O_RDWR,
        0o644,
    )
    .unwrap();

    tdb.store(b"key", b"value", None).unwrap();
    assert_eq!(Some(b"value".to_vec()), tdb.fetch(b"key").unwrap());
}
