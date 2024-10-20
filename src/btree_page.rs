use anyhow::{anyhow, ensure, Result};

use crate::varint::*;

#[expect(dead_code)]
pub struct BTreePage {
    /// The header for this database page.
    pub header: BTreePageHeader,
    /// An array of pointers to cells on this page.
    pub cell_pointer_array: Vec<u16>,
    /// The cells of this page.
    pub cells: Vec<Cell>,
}

impl BTreePage {
    pub fn parse(page: &[u8], first_page: bool) -> Result<Self> {
        let header = BTreePageHeader::parse(&page[0..12])?;
        let mut cell_pointer_array = Vec::with_capacity(header.num_cells as usize);
        for c in 0..header.num_cells {
            let offset = (c * 2) as usize + header.len();
            let mut cp = u16::from_be_bytes([page[offset], page[offset + 1]]);
            if first_page {
                // This is the first page, which includes the database header
                // of 100 bytes.
                cp -= 100;
            }
            cell_pointer_array.push(cp);
        }
        let mut cells = Vec::with_capacity(header.num_cells as usize);
        for &cp in cell_pointer_array.iter() {
            let cell = Cell::parse(header.page_type, &page[cp as usize..])?;
            cells.push(cell);
        }

        Ok(Self {
            header,
            cell_pointer_array,
            cells,
        })
    }
}

/// "SQLite format 3" plus the null terminator character at the end.
const MAGIC: &[u8] = &[
    0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00,
];

/// The database file header.
/// The first 100 bytes of the database file. All multi-byte fields are stored as big-endian
/// in the on-disk format.
#[expect(dead_code)]
pub struct DbHeader<'m> {
    /// Every SQLite database begins with a 16 byte sequence corresponding to
    /// the UTF-8 string "SQLite format 3" including the nul terminator character at the end.
    pub magic: &'m [u8],
    /// The database page size, in bytes. Must be a power of two between 512 and 32768, inclusive.
    /// The page size can also be 65536 bytes
    /// The on-disk format uses a 2 byte value to encode the page size,
    /// and represents the value of 65536 with a 0x00 0x01 (i.e., big-endian 1).
    pub page_size: u32,
    /// In current versions of SQLite, this value is 1 for rollback journalling modes
    /// and 2 for WAL journalling mode.
    pub format_write_version: u8,
    /// In current versions of SQLite, this value is 1 for rollback journalling modes
    /// and 2 for WAL journalling mode.
    pub format_read_version: u8,
    /// The amount of reserved space at the end of each database page, in bytes. Usually 0.
    pub reserved_space: u8,
    /// Must be 64.
    pub max_embedded_payload: u8,
    /// Must be 32.
    pub min_embedded_payload: u8,
    /// Must be 32.
    pub leaf_payload: u8,
    /// This is incremented whenever the database file is unlocked after having been modified.
    /// The counter might not be incremented in WAL mode.
    pub file_change_count: u32,
    /// The size of the database file, in number of pages.
    /// This value may be invalid, in which case the size has to be calculated from the actual
    /// size of the database file.
    /// The counter is only considered valid if it is non-zero and the
    /// [`file_change_count`](DbHeader::file_change_count) matches the [`version_valid_for`](DbHeader::version_valid_for).
    pub page_count: u32,
    /// The page number of the first page in the freelist, or zero if the list is empty.
    /// The list stores unused pages in the database file.
    pub freelist_trunk_head: u32,
    /// The total number of unused pages in the database file.
    pub freelist_page_count: u32,
    /// This value is incremented whenever the database schema changes.
    pub schema_cookie: u32,
    /// The high-level SQL format. Can be 1, 2, 3, 4,
    /// or (if the database is completely empty and has no schema) 0.
    pub schema_format: u32,
    /// The suggested size of the page cache.
    pub default_page_cache_size: u32,
    /// The page number of the largest root page in the database file, or zero if no vacuum mode is supported.
    pub vacuum_root_page: Option<u32>,
    /// The encoding for all text strings in the database.
    /// 1 means UTF-8, 2 means UTF-16le, 3 means UTF-16be.
    pub db_text_encoding: u32,
    /// Not used by SQLite.
    pub user_version: u32,
    /// True for incremental-vacuum mode. Stored as a 4 byte big-endian number, where non-zero means true.
    pub incremental_vacuum: bool,
    /// Can be used to identify the database as belonging to or being associated with a particular application.
    pub application_id: u32,
    /// The value of the change counter when the version number was stored.
    pub version_valid_for: u32,
    /// The SQLite version number of the SQLite library that most recently modified the database file.
    pub sqlite_version: u32,
}

impl<'m> DbHeader<'m> {
    /// Parse a database header from the first 100 bytes of the database file.
    pub fn parse(bytes: &'m [u8]) -> Result<Self> {
        let magic = &bytes[0..16];
        ensure!(
            magic == MAGIC,
            "Magic string at beginning of database file is missing or wrong"
        );

        let page_size: u32 = {
            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let size = u16::from_be_bytes([bytes[16], bytes[17]]);
            if size == 1 {
                65536
            } else {
                ensure!(
                    (512..=32768).contains(&size),
                    "Page size is not in valid range"
                );
                ensure!(size.is_power_of_two(), "Page size is not a power of two");
                size.into()
            }
        };

        let format_write_version = bytes[18];
        ensure!(
            format_write_version == 1 || format_write_version == 2,
            "Format write version is invalid"
        );
        let format_read_version = bytes[19];
        ensure!(
            format_read_version == 1 || format_read_version == 2,
            "Format read version is invalid"
        );

        let reserved_space = bytes[20];

        let max_embedded_payload = bytes[21];
        ensure!(
            max_embedded_payload == 64,
            "Maximum embedded payload size must be 64"
        );
        let min_embedded_payload = bytes[22];
        ensure!(
            min_embedded_payload == 32,
            "Minimum embedded payload size must be 32"
        );
        let leaf_payload = bytes[23];
        ensure!(leaf_payload == 32, "Leaf payload size must be 32");

        let file_change_count = u32::from_be_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]);

        let page_count = u32::from_be_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]);

        let freelist_trunk_head = u32::from_be_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]);
        let freelist_page_count = u32::from_be_bytes([bytes[36], bytes[37], bytes[38], bytes[39]]);
        ensure!(
            (freelist_page_count == 0 && freelist_trunk_head == 0)
                || (freelist_page_count != 0 && freelist_trunk_head != 0),
            "Freelist list trunk head and freelist page count disagree"
        );

        let schema_cookie = u32::from_be_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]);

        let schema_format = u32::from_be_bytes([bytes[44], bytes[45], bytes[46], bytes[47]]);
        ensure!((0..=4).contains(&schema_format), "Schema format is invalid");

        let default_page_cache_size =
            u32::from_be_bytes([bytes[48], bytes[49], bytes[50], bytes[51]]);

        let vacuum_root_page =
            match u32::from_be_bytes([bytes[52], bytes[53], bytes[54], bytes[55]]) {
                0 => None,
                n => Some(n),
            };

        let db_text_encoding = u32::from_be_bytes([bytes[56], bytes[57], bytes[58], bytes[59]]);
        ensure!(
            (1..=3).contains(&db_text_encoding),
            "Invalid database text encoding"
        );

        let user_version = u32::from_be_bytes([bytes[60], bytes[61], bytes[62], bytes[63]]);

        let incremental_vacuum =
            u32::from_be_bytes([bytes[64], bytes[65], bytes[66], bytes[67]]) > 0;

        let application_id = u32::from_be_bytes([bytes[68], bytes[69], bytes[70], bytes[71]]);

        let version_valid_for = u32::from_be_bytes([bytes[92], bytes[93], bytes[94], bytes[95]]);

        let sqlite_version = u32::from_be_bytes([bytes[96], bytes[97], bytes[98], bytes[99]]);

        Ok(Self {
            magic,
            page_size,
            format_write_version,
            format_read_version,
            reserved_space,
            max_embedded_payload,
            min_embedded_payload,
            leaf_payload,
            file_change_count,
            page_count,
            freelist_trunk_head,
            freelist_page_count,
            schema_cookie,
            schema_format,
            default_page_cache_size,
            vacuum_root_page,
            db_text_encoding,
            user_version,
            incremental_vacuum,
            application_id,
            version_valid_for,
            sqlite_version,
        })
    }
}

/// The header of a B-Tree page.
/// 8 bytes for leaf pages and 12 bytes for interior pages.
#[expect(dead_code)]
pub struct BTreePageHeader {
    /// A one byte flag indicating the page type.
    pub page_type: BTreePageType,
    /// The start of the first freeblock on the page, or zero if there are none.
    pub first_freeblock: u16,
    /// The number of cells on the page.
    pub num_cells: u16,
    /// The start of the cell content area.
    /// A zero value in the on-disk format is interpreted as 65536.
    pub cell_content_area: u32,
    /// The number of fragmented free bytes in the cell content area.
    pub fragmented: u8,
    /// The right-most pointer. Only appears in interior B-Tree pages.
    pub right_most: Option<u32>,
}

impl BTreePageHeader {
    fn len(&self) -> usize {
        match self.page_type {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => 12,
            BTreePageType::LeafIndex | BTreePageType::LeafTable => 8,
        }
    }

    fn parse(bytes: &[u8]) -> Result<Self> {
        let page_type = bytes[0].try_into()?;
        let first_freeblock = u16::from_be_bytes([bytes[1], bytes[2]]);
        let num_cells = u16::from_be_bytes([bytes[3], bytes[4]]);
        let cell_content_area: u32 = {
            let cca = u16::from_be_bytes([bytes[5], bytes[6]]);
            if cca == 0 {
                65536
            } else {
                cca.into()
            }
        };
        let fragmented = bytes[7];
        let right_most = match page_type {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => {
                let rm = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
                Some(rm)
            }
            BTreePageType::LeafIndex | BTreePageType::LeafTable => None,
        };

        Ok(Self {
            page_type,
            first_freeblock,
            num_cells,
            cell_content_area,
            fragmented,
            right_most,
        })
    }
}

#[derive(Copy, Clone)]
pub enum BTreePageType {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0a,
    LeafTable = 0x0d,
}

impl TryFrom<u8> for BTreePageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0x02 => Ok(Self::InteriorIndex),
            0x05 => Ok(Self::InteriorTable),
            0x0a => Ok(Self::LeafIndex),
            0x0d => Ok(Self::LeafTable),
            _ => Err(anyhow!("Not a valid B-Tree page type: {:x}", value)),
        }
    }
}

/// A chunk of data in a B-Tree page.
#[expect(dead_code)]
pub enum Cell {
    TableLeaf {
        payload_len: Varint,
        key: Varint,
        payload: Record,
        overflow: Option<u32>,
    },
    TableInterior {
        left_child: u32,
        key: Varint,
    },
    IndexLeaf,
    IndexInterior,
}

impl Cell {
    fn parse(kind: BTreePageType, bytes: &[u8]) -> Result<Self> {
        match kind {
            BTreePageType::InteriorIndex => todo!(),
            BTreePageType::InteriorTable => {
                let left_child = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                let (key, _) = Varint::parse(&bytes[4..]);
                Ok(Self::TableInterior { left_child, key })
            }
            BTreePageType::LeafIndex => todo!(),
            BTreePageType::LeafTable => {
                let mut bytes_read: usize = 0;
                let (payload_len, read) = Varint::parse(&bytes[0..]);
                bytes_read += read;
                let (key, read) = Varint::parse(&bytes[bytes_read..]);
                bytes_read += read;
                let (payload, read) = Record::parse(&bytes[bytes_read..], payload_len.0 as usize);
                bytes_read += read;
                let overflow = if read == payload_len.0 as usize {
                    // No overflow.
                    None
                } else {
                    // TODO: Implement overflow pages.
                    eprintln!("WARNING: Overflow pages are currently unimplemented");
                    // Overflow.
                    Some(u32::from_be_bytes([
                        bytes[bytes_read],
                        bytes[bytes_read + 1],
                        bytes[bytes_read + 2],
                        bytes[bytes_read + 3],
                    ]))
                };

                Ok(Self::TableLeaf {
                    payload_len,
                    key,
                    payload,
                    overflow,
                })
            }
        }
    }
}

#[expect(dead_code)]
pub struct Record {
    pub header_len: Varint,
    pub serial_types: Vec<SerialType>,
    pub values: Vec<RecordValue>,
}

impl Record {
    fn parse(bytes: &[u8], _payload_len: usize) -> (Self, usize) {
        let mut bytes_read = 0;
        let (header_len, read) = Varint::parse(&bytes[0..]);
        bytes_read += read;
        let mut serial_types = Vec::new();
        let mut remaining = header_len.0 as usize - read;
        while remaining > 0 {
            let (st, read) = Varint::parse(&bytes[bytes_read..]);
            bytes_read += read;
            remaining -= read;
            let st: SerialType = st.into();
            serial_types.push(st);
        }

        let mut values = Vec::with_capacity(serial_types.len());
        for st in serial_types.iter() {
            let (rv, read) = RecordValue::parse(st, &bytes[bytes_read..bytes_read + st.len()]);
            bytes_read += read;
            values.push(rv);
        }

        (
            Self {
                header_len,
                serial_types,
                values,
            },
            bytes_read,
        )
    }
}

#[derive(Copy, Clone, Debug)]
pub enum SerialType {
    /// Value is a NULL. Content size 0.
    Null,
    /// Value is an 8 bit twos-complement integer.
    I8,
    /// Value is a big-endian, 16 bit twos-complement integer.
    I16,
    /// Value is a big-endian, 24 bit twos-complement integer.
    I24,
    /// Value is a big-endian, 32 bit twos-complement integer.
    I32,
    /// Value is a big-endian, 48 bit twos-complement integer.
    I48,
    /// Value is a big-endian, 64 bit twos-complement integer.
    I64,
    /// Value is a big-endian, 64 bit IEEE 754-2008 floating point number.
    F64,
    /// Value is the integer 0. Content size 0.
    Zero,
    /// Value is the integer 1. Content size 0.
    One,
    /// Value is a BLOB that is (N-12)/2 bytes in length.
    N12AndEven(Varint),
    /// Value is string in the DB text encoding and (N-13)/2 bytes in length.
    /// The nul terminator is not stored.
    N13AndOdd(Varint),
}

impl SerialType {
    fn len(&self) -> usize {
        match self {
            SerialType::Null => 0,
            SerialType::I8 => 1,
            SerialType::I16 => 2,
            SerialType::I24 => 3,
            SerialType::I32 => 4,
            SerialType::I48 => 6,
            SerialType::I64 => 8,
            SerialType::F64 => 8,
            SerialType::Zero => 0,
            SerialType::One => 0,
            SerialType::N12AndEven(n) => (n.0 as usize - 12) / 2,
            SerialType::N13AndOdd(n) => (n.0 as usize - 13) / 2,
        }
    }
}

impl From<Varint> for SerialType {
    fn from(value: Varint) -> Self {
        match value.0 {
            0 => Self::Null,
            1 => Self::I8,
            2 => Self::I16,
            3 => Self::I24,
            4 => Self::I32,
            5 => Self::I48,
            6 => Self::I64,
            7 => Self::F64,
            8 => Self::Zero,
            9 => Self::One,
            n if n >= 12 && n % 2 == 0 => Self::N12AndEven(value),
            n if n >= 13 && n % 2 != 0 => Self::N13AndOdd(value),
            _ => panic!("Malformed SerialType"),
        }
    }
}

#[expect(dead_code)]
pub enum RecordValue {
    /// Value is a NULL. Content size 0.
    Null,
    /// Value is an 8 bit twos-complement integer.
    I8(i8),
    /// Value is a big-endian, 16 bit twos-complement integer.
    I16(i16),
    /// Value is a big-endian, 24 bit twos-complement integer.
    I24(i32),
    /// Value is a big-endian, 32 bit twos-complement integer.
    I32(i32),
    /// Value is a big-endian, 48 bit twos-complement integer.
    I48(i64),
    /// Value is a big-endian, 64 bit twos-complement integer.
    I64(i64),
    /// Value is a big-endian, 64 bit IEEE 754-2008 floating point number.
    F64(f64),
    /// Value is the integer 0. Content size 0.
    Zero,
    /// Value is the integer 1. Content size 0.
    One,
    /// Value is a BLOB that is (N-12)/2 bytes in length.
    N12AndEven(Vec<u8>),
    /// Value is string in the DB text encoding and (N-13)/2 bytes in length.
    /// The nul terminator is not stored.
    N13AndOdd(String),
}

impl RecordValue {
    fn parse(st: &SerialType, bytes: &[u8]) -> (Self, usize) {
        match st {
            SerialType::Null => (RecordValue::Null, 0),
            SerialType::Zero => (RecordValue::Zero, 0),
            SerialType::One => (RecordValue::One, 0),
            SerialType::I8 => {
                let val = bytes[0] as i8;
                (RecordValue::I8(val), 1)
            }
            SerialType::I16 => {
                let val = i16::from_be_bytes([bytes[0], bytes[1]]);
                (RecordValue::I16(val), 2)
            }
            SerialType::I24 => {
                let val = if bytes[0] < 0b1000_0000 {
                    // Pad with zeroes.
                    i32::from_be_bytes([0x00, bytes[0], bytes[1], bytes[2]])
                } else {
                    // Pad with ones.
                    i32::from_be_bytes([0xFF, bytes[0], bytes[1], bytes[2]])
                };
                (RecordValue::I24(val), 3)
            }
            SerialType::I32 => {
                let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                (RecordValue::I32(val), 4)
            }
            SerialType::I48 => {
                let val = if bytes[0] < 0b1000_0000 {
                    // Pad with zeroes.
                    i64::from_be_bytes([
                        0x00, 0x00, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                    ])
                } else {
                    // Pad with ones.
                    i64::from_be_bytes([
                        0xFF, 0xFF, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                    ])
                };
                (RecordValue::I48(val), 6)
            }
            SerialType::I64 => {
                let val = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                (RecordValue::I64(val), 8)
            }
            SerialType::F64 => {
                let val = f64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                (RecordValue::F64(val), 8)
            }
            SerialType::N12AndEven(n) => {
                let len = (n.0 as usize - 12) / 2;
                let val: Vec<u8> = bytes[0..len].into();
                (RecordValue::N12AndEven(val), len)
            }
            SerialType::N13AndOdd(n) => {
                let len = (n.0 as usize - 12) / 2;
                let val: String =
                    String::from_utf8(bytes[0..len].into()).expect("Should be valid string");
                (RecordValue::N13AndOdd(val), len)
            }
        }
    }
}
