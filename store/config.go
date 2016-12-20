package store

// Store configuration.
type Config struct {
	StorePath            string
	MaxAccumSizeMiB      int
	MaxCacheSizeKiB      int
	MaxIndexBlockSizeKiB int
	MinSingularSizeKiB   int
	CompressionLevel     int
	MaxGoroutines        int
}
