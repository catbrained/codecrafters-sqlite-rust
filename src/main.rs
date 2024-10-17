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
    /// Database page size in bytes.
    /// Must be a power of two between 512 and 32768 inclusive, or 65536.
    /// Note that in the actual SQlite database format, the page size is
    /// stored as a 2 byte integer, where the value 1 represents a page
    /// size of 65536.
    db_page_size: u32,
    num_tables: u16,
}

fn dot_dbinfo(db_file: impl AsRef<Path>) -> Result<DbInfo> {
    let mut file = File::open(db_file)?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    // Refer to: https://www.sqlite.org/fileformat.html#database_header
    let db_page_size: u32 = {
        // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
        let size = u16::from_be_bytes([header[16], header[17]]);
        if size == 1 {
            65536
        } else {
            assert!((512..=32768).contains(&size));
            assert!(size.is_power_of_two());
            size.into()
        }
    };

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
        let test_db_files = vec![
            ("sample.db", 4096),
        ];

        for (db, expected) in test_db_files {
            let db_info = dot_dbinfo(db).unwrap();

            assert_eq!(db_info.db_page_size, expected, "{db}");
        }
    }

    #[test]
    fn dbinfo_outputs_correct_num_tables() {
        let test_db_files = vec![
            ("sample.db", 3),
        ];

        for (db, expected) in test_db_files {
            let db_info = dot_dbinfo(db).unwrap();

            assert_eq!(db_info.num_tables, expected, "{db}");
        }
    }
}
