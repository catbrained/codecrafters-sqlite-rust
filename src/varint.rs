pub struct Varint(pub i64);

impl Varint {
    /// Parses a Varint from the provided slice and returns it along with
    /// the amount of bytes read.
    pub fn parse(bytes: &[u8]) -> (Self, usize) {
        let mut varint: i64 = 0;
        let mut bytes_read = 0;
        // A Varint can be between 1 and 9 bytes long.
        for (i, byte) in bytes.iter().enumerate().take(9) {
            bytes_read += 1;
            // We use the lower seven bits of the first 8 bytes.
            if i < 8 {
                varint = (varint << 7) | (*byte & 0b0111_1111) as i64;
                // The most significant bit indicates if we should continue reading.
                if *byte < 0b1000_0000 {
                    break;
                }
            } else {
                // We use all 8 bits of the 9th byte.
                varint = (varint << 8) | *byte as i64;
            }
        }

        (Self(varint), bytes_read)
    }
}
