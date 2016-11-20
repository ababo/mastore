package store

type Config struct {
	StorePath        string
	AccumSizeMiB     int
	MaxFileSizeKiB   int
	CompressionLevel int
}
