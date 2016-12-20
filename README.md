## Key to (multiple) values storage

### Features:
1. Capable to easily store billions of records.
2. Supports record addition only.
3. Support for concurrency.
4. Highly tunable.
5. Very simple.

### How to use:
1. Build the `mastore` binary.
2. Create a store directory (e.g. 'store') in your $HOME.
3. Create `mastore.config` in your $HOME:

	```json
	{
		"StorePath": "$HOME/store",
		"MaxAccumSizeMiB": 1024,
		"MaxCacheSizeKiB": 1024,
		"MaxIndexBlockSizeKiB": 8192,
		"MinSingularSizeKiB": 8192,
		"CompressionLevel": -1,
		"MaxGoroutines": 1
	}
	```
4. Feed standard input of the binary with strings of form `<key>\t<value>`:

	```bash
	mastore write < some_records.txt
	```
5. Read all keys for the stored records:

	```bash
	mastore read -keys
	```
6. Read records for a specified key:

	```bash
	mastore read -key=<key>
	```
7. Enjoy!
