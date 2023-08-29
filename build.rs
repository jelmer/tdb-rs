extern crate bindgen;
extern crate pkg_config;

fn main() {
    system_deps::Config::new().probe().unwrap();

    // Use pkg-config to get the necessary flags for the `tdb` library
    let pc_tdb = pkg_config::Config::new()
        .probe("tdb")
        .unwrap_or_else(|e| panic!("Failed to find tdb library: {}", e));

    if pc_tdb.include_paths.len() != 1 {
        panic!("Expected to find exactly one tdb include path");
    }

    let tdb_header = pc_tdb.include_paths[0].join("tdb.h");

    // Generate bindings using bindgen
    let bindings = bindgen::Builder::default()
        .header("sys/stat.h")
        .header(tdb_header.to_str().unwrap())
        .blocklist_type("TDB_DATA")
        .blocklist_function("tdb_store")
        .blocklist_function("tdb_fetch")
        .blocklist_function("tdb_append")
        .blocklist_function("tdb_delete")
        .blocklist_function("tdb_exists")
        .blocklist_function("tdb_nextkey")
        .clang_args(
            pc_tdb
                .include_paths
                .iter()
                .map(|path| format!("-I{}", path.display())),
        )
        .generate()
        .expect("Failed to generate bindings");

    let out_path = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("tdb_sys.rs"))
        .expect("Failed to write bindings");
}
