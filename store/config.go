package store

type Config struct {
	StorePath            string
	MaxAccumSizeMiB      int
	MaxCacheSizeKiB      int
	MaxIndexBlockSizeKiB int
	MinSingularSizeKiB   int
	CompressionLevel     int
	MaxGoroutines        int
}
