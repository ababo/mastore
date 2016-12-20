package store

// Config is a store configuration.
type Config struct {
	StorePath            string
	MaxAccumSizeMiB      int
	MaxCacheSizeKiB      int
	MaxIndexBlockSizeKiB int
	MinSingularSizeKiB   int
	CompressionLevel     int
	MaxGoroutines        int
}
