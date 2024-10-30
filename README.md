LocalKVStore is a simple key-value store implemented in Node.js, designed to save key-value pairs in a JSON file with support for time-to-live (TTL) functionality. This store allows you to create, read, and delete key-value pairs, while automatically managing expired entries.

Features
Persistent Storage: Saves key-value pairs in a local JSON file.
TTL Support: Allows setting an expiration time for each key.
Automatic Cleanup: Periodically removes expired keys.
Concurrent Access: Uses file locking to prevent concurrent writes.
Validation: Ensures keys do not exceed a specified length and values do not exceed a specified size.
