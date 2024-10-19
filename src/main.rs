use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;
use std::os::unix::fs::FileExt;
use std::path::Path;

use btree_page::*;

mod btree_page;
mod varint;

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
        ".tables" => {
            let tables = dot_tables(&args[1])?;
            println!("{tables}");
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

    let header = DbHeader::parse(&header)?;

    let mut page = vec![0; header.page_size as usize];
    file.read_exact_at(&mut page, 0)?;

    let mut num_tables = 0;
    let mut remaining_pages = Vec::new();

    let mut btree = BTreePage::parse(&page[100..], true)?;

    loop {
        match btree.header.page_type {
            BTreePageType::InteriorIndex => todo!(),
            BTreePageType::InteriorTable => {
                for cell in btree.cells {
                    let Cell::TableInterior { left_child, .. } = cell else {
                        bail!("Unexpected cell type");
                    };
                    remaining_pages.push(left_child - 1);
                }
                let rightmost = btree
                    .header
                    .right_most
                    .expect("Right-most pointer should exist in interior page");
                remaining_pages.push(rightmost - 1);
            }
            BTreePageType::LeafIndex => todo!(),
            BTreePageType::LeafTable => {
                for cell in btree.cells {
                    let Cell::TableLeaf { payload, .. } = cell else {
                        bail!("Unexpected cell type");
                    };
                    let RecordValue::N13AndOdd(ref s) = payload.values[0] else {
                        bail!("Unexpected record value");
                    };
                    if s == "table" {
                        num_tables += 1;
                    }
                }
            }
        }

        if remaining_pages.is_empty() {
            break;
        }

        let next_page = remaining_pages
            .pop()
            .expect("We checked that remaining pages is not empty");
        file.read_exact_at(&mut page, next_page as u64 * header.page_size as u64)?;
        btree = BTreePage::parse(&page[0..], false)?;
    }

    Ok(DbInfo {
        db_page_size: header.page_size,
        num_tables,
    })
}

fn dot_tables(db_file: impl AsRef<Path>) -> Result<String> {
    let mut file = File::open(db_file)?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    let header = DbHeader::parse(&header)?;

    let mut page = vec![0; header.page_size as usize];
    file.read_exact_at(&mut page, 0)?;

    let mut tables = String::new();
    let mut remaining_pages = Vec::new();

    let mut btree = BTreePage::parse(&page[100..], true)?;

    loop {
        match btree.header.page_type {
            BTreePageType::InteriorIndex => todo!(),
            BTreePageType::InteriorTable => {
                for cell in btree.cells {
                    let Cell::TableInterior { left_child, .. } = cell else {
                        bail!("Unexpected cell type");
                    };
                    remaining_pages.push(left_child - 1);
                }
                let rightmost = btree
                    .header
                    .right_most
                    .expect("Right-most pointer should exist in interior page");
                remaining_pages.push(rightmost - 1);
            }
            BTreePageType::LeafIndex => todo!(),
            BTreePageType::LeafTable => {
                for cell in btree.cells {
                    let Cell::TableLeaf { payload, .. } = cell else {
                        bail!("Unexpected cell type");
                    };
                    let RecordValue::N13AndOdd(ref s) = payload.values[0] else {
                        bail!("Unexpected record value");
                    };
                    if s == "table" {
                        let RecordValue::N13AndOdd(ref n) = payload.values[2] else {
                            bail!("Unexpected record value");
                        };
                        if !n.starts_with("sqlite_") {
                            tables.push_str(&format!(" {}", n));
                        }
                    }
                }
            }
        }

        if remaining_pages.is_empty() {
            break;
        }

        let next_page = remaining_pages
            .pop()
            .expect("We checked that remaining pages is not empty");
        file.read_exact_at(&mut page, next_page as u64 * header.page_size as u64)?;
        btree = BTreePage::parse(&page[0..], false)?;
    }

    Ok(tables.trim().to_string())
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn dbinfo_outputs_correct_page_size() {
        let test_db_files = vec![
            ("sample.db", 4096),
            ("superheroes.db", 4096),
            ("companies.db", 4096),
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
            ("superheroes.db", 2),
            ("companies.db", 2),
        ];

        for (db, expected) in test_db_files {
            let db_info = dot_dbinfo(db).unwrap();

            assert_eq!(db_info.num_tables, expected, "{db}");
        }
    }

    #[test]
    fn tables_outputs_correct_table_names() {
        let test_db_files = vec![
            ("sample.db", "apples oranges"),
            ("superheroes.db", "superheroes"),
            ("companies.db", "companies"),
        ];

        for (db, expected) in test_db_files {
            let tables = dot_tables(db).unwrap();

            assert_eq!(tables, expected, "{db}");
        }
    }
}
