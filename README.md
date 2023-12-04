# Rust bindings for TDB

This rust crate provides idiomatic Rust bindings for the Trivial Database (TDB)
library.

See the [TDB homepage](https://tdb.samba.org/) for more details.

## Example

```rust
use trivialdb::{Tdb,Flags};

let mut tdb = Tdb::memory(None, Flags::empty()).unwrap();

tdb.store(b"key", b"value", None).unwrap();
assert_eq!(Some(b"value".to_vec()), tdb.fetch(b"key").unwrap());
