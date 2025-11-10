//! Example demonstrating iteration over TDB databases
//!
//! This example shows how to iterate over keys and key-value pairs.

use trivialdb::{Flags, Tdb};

fn main() {
    // Create an in-memory database for this example
    let mut tdb = Tdb::memory(None, Flags::empty()).expect("Failed to create database");

    // Store some sample data
    println!("Storing sample data...");
    let data = vec![
        (b"user:1".as_slice(), b"Alice".as_slice()),
        (b"user:2", b"Bob"),
        (b"user:3", b"Charlie"),
        (b"config:timeout", b"30"),
        (b"config:retries", b"3"),
    ];

    for (key, value) in &data {
        tdb.store(key, value, None).expect("Failed to store data");
    }

    // Iterate over keys only
    println!("\nIterating over keys:");
    for key in tdb.keys() {
        println!("  Key: {:?}", String::from_utf8_lossy(&key));
    }

    // Iterate over key-value pairs
    println!("\nIterating over key-value pairs:");
    for (key, value) in tdb.iter() {
        println!(
            "  {:?} => {:?}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Filter keys with a prefix
    println!("\nIterating over keys with 'user:' prefix:");
    for key in tdb.keys() {
        let key_str = String::from_utf8_lossy(&key);
        if key_str.starts_with("user:") {
            let value = tdb.fetch(&key).unwrap().unwrap();
            println!("  {:?} => {:?}", key_str, String::from_utf8_lossy(&value));
        }
    }

    // Count total entries
    let count = tdb.keys().count();
    println!("\nTotal entries: {}", count);
}
