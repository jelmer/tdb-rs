# Rust bindings for TDB

This rust crate provides idiomatic Rust bindings for the Trivial Database (TDB)
library.

See the [TDB homepage](https://tdb.samba.org/) for more details.

## Example

```rust
use tdb::{Tdb,Flags};

let tdb = Tdb::memory(None, Flags::empty());

tdb.store(b"key", b"value", None).unwrap();
assert_eq!(b"value", tdb.fetch(b"key"));
```
