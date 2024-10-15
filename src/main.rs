use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;
use std::os::unix::fs::FileExt;
use std::path::Path;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let db_info = dot_dbinfo(&args[1])?;
            println!("database page size: {}", db_info.db_page_size);
            println!("number of tables: {}", db_info.num_tables);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

struct DbInfo {
    db_page_size: u16,
    num_tables: u16,
}

fn dot_dbinfo(db_file: impl AsRef<Path>) -> Result<DbInfo> {
    let mut file = File::open(db_file)?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
    let db_page_size = u16::from_be_bytes([header[16], header[17]]);

    // The b-tree page header is 8 bytes for leaf pages,
    // and 12 bytes for interior pages.
    // TODO: This will break if we are reading a DB file that
    //       contains more pages (i.e., when this page is _not_ a leaf page).
    let mut b_tree_page_header = [0; 8];
    // The b-tree page header directly follows the database header.
    file.read_exact_at(&mut b_tree_page_header, 100)?;
    let page_type = u8::from_be(b_tree_page_header[0]);
    // 0x0a: leaf index b-tree page, 0x0d: leaf table b-tree page
    // TODO: Handle other page types. In particular, we need to actually walk the
    //       b-tree to count the number of tables. Currently we assume that there is
    //       only this one page.
    debug_assert!(page_type == 0x0a || page_type == 0x0d);
    let num_tables = u16::from_be_bytes([b_tree_page_header[3], b_tree_page_header[4]]);

    let db_info = DbInfo {
        db_page_size,
        num_tables,
    };

    Ok(db_info)
}

#[cfg(test)]
mod tests {
    use crate::dot_dbinfo;

    #[test]
    fn dbinfo_outputs_correct_page_size() {
        let test_db_file = "sample.db";

        let db_info = dot_dbinfo(test_db_file).unwrap();

        assert_eq!(db_info.db_page_size, 4096);
    }

    #[test]
    fn dbinfo_outputs_correct_num_tables() {
        let test_db_file = "sample.db";

        let db_info = dot_dbinfo(test_db_file).unwrap();

        assert_eq!(db_info.num_tables, 3);
    }
}
