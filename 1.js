const fs = require('fs');
const path = require('path');
const lockfile = require('proper-lockfile');
const { promisify } = require('util');

class LocalKVStore {
  constructor(filePath = 'local_kv_store.json') {
    this.filePath = filePath; // Path to the JSON file for storing key-value data
    this.store = {}; // In-memory store for key-value pairs
    this.maxKeyLength = 32; // Maximum allowed length for keys
    this.maxValueSize = 16 * 1024; // Maximum size for value (16KB)
    this.ttlQueue = []; // Queue to track entries with TTL (time-to-live)
    this.lock = false; // Lock flag for preventing concurrent writes

    this._loadData(); // Load existing data from file
    this._startCleanup(); // Start the cleanup interval for expired keys
  }

  // Load data from the JSON file if it exists
  async _loadData() {
    if (fs.existsSync(this.filePath)) {
      try {
        const data = await fs.promises.readFile(this.filePath, 'utf-8');
        this.store = JSON.parse(data); // Parse and load data into the store
        this._rebuildTTLQueue(); // Rebuild the TTL queue for loaded data
      } catch (error) {
        console.error("Error loading data:", error);
      }
    }
  }

  // Rebuild the TTL queue based on current store entries
  _rebuildTTLQueue() {
    Object.entries(this.store).forEach(([key, entry]) => {
      if (entry.ttl && Date.now() < entry.ttl) {
        this._addTTL(key, entry.ttl); // Add valid TTL entries to the queue
      } else if (entry.ttl) {
        delete this.store[key]; // Remove expired entries from the store
      }
    });
  }

  // Save the current store data to the JSON file with locking to prevent concurrent writes
  async _saveData() {
    try {
      await lockfile.lock(this.filePath); // Lock file to ensure exclusive access
      await fs.promises.writeFile(this.filePath, JSON.stringify(this.store, null, 2));
      lockfile.unlock(this.filePath); // Unlock file after writing
    } catch (error) {
      console.error("Error saving data:", error);
    }
  }

  // Check if a store entry is expired
  _isExpired(entry) {
    return entry.ttl && Date.now() > entry.ttl;
  }

  // Add a key to the TTL queue and sort it based on expiration time
  _addTTL(key, ttl) {
    this.ttlQueue.push({ ttl, key });
    this.ttlQueue.sort((a, b) => a.ttl - b.ttl); // Sort to keep the nearest expiration first
  }

  // Start a cleanup interval to remove expired keys periodically
  _startCleanup() {
    setInterval(() => {
      while (this.ttlQueue.length && this.ttlQueue[0].ttl <= Date.now()) {
        const { key } = this.ttlQueue.shift();
        if (this._isExpired(this.store[key])) {
          delete this.store[key]; // Remove expired entries from the store
        }
      }
      this._saveData(); // Save data after each cleanup
    }, 60000); // Run cleanup every 60 seconds
  }

  // Create a new key-value pair with optional TTL
  async create(key, value, ttl) {
    if (key.length > this.maxKeyLength) throw new Error("Key length exceeds 32 characters.");
    if (Buffer.byteLength(JSON.stringify(value), 'utf8') > this.maxValueSize) throw new Error("Value size exceeds 16KB.");
    if (this.store[key] && !this._isExpired(this.store[key])) throw new Error("Key already exists.");

    const entry = { value };
    if (ttl) {
      entry.ttl = Date.now() + ttl; // Set TTL if provided
      this._addTTL(key, entry.ttl); // Add entry to TTL queue
    }
    this.store[key] = entry;
    await this._saveData(); // Save data to file
    return "Success: Key created.";
  }

  // Read a value by key, throwing an error if the key is expired or missing
  async read(key) {
    const entry = this.store[key];
    if (!entry || this._isExpired(entry)) throw new Error("Key not found or expired.");
    return entry.value; // Return the value if it exists and is not expired
  }

  // Delete a key from the store, throwing an error if it’s missing or expired
  async delete(key) {
    if (!this.store[key] || this._isExpired(this.store[key])) throw new Error("Key not found or expired.");
    delete this.store[key]; // Remove key from the store
    await this._saveData(); // Save changes to file
    return "Success: Key deleted.";
  }
}

module.exports = LocalKVStore; // Export the LocalKVStore class
