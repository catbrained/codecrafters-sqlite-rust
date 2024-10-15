use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;
use std::os::unix::fs::FileExt;

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
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let page_size = u16::from_be_bytes([header[16], header[17]]);

            // The b-tree page header is 8 bytes for leaf pages,
            // and 12 bytes for interior pages.
            let mut b_tree_page_header = [0; 8];
            // The b-tree page header directly follows the database header.
            file.read_exact_at(&mut b_tree_page_header, 100)?;
            let page_type = u8::from_be(b_tree_page_header[0]);
            // 0x0a: leaf index b-tree page, 0x0d: leaf table b-tree page
            debug_assert!(page_type == 0x0a || page_type == 0x0d);
            let num_cells = u16::from_be_bytes([b_tree_page_header[3], b_tree_page_header[4]]);

            println!("database page size: {}", page_size);
            println!("number of tables: {}", num_cells);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
