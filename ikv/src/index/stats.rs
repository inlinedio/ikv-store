#[derive(Debug, Default)]
pub struct CompactionStats {
    pub offset_table_size_bytes: u64,
    pub mmap_file_size_bytes: u64,
}

impl CompactionStats {
    pub fn aggregate(stats: &[CompactionStats]) -> CompactionStats {
        let mut offset_table_size_bytes = 0;
        let mut mmap_file_size_bytes = 0;

        for cs in stats.iter() {
            offset_table_size_bytes += cs.offset_table_size_bytes;
            mmap_file_size_bytes += cs.mmap_file_size_bytes;
        }

        return CompactionStats {
            offset_table_size_bytes,
            mmap_file_size_bytes,
        };
    }
}
