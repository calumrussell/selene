package com.selene;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class DB {

    // This is a off-heap data structure that stores a list of variable-length strings as bytes.
    private final KeyStore keyStore;
    private final Arena arena;
    private final MemorySegment valueSegment;
    private long buckets;

    private long getHashBucket(String key) {
        long hash = key.hashCode();
        long alwaysPositive = hash ^ (hash >>> 32);
        return Math.abs(alwaysPositive) % (this.buckets - 1);
    }

    int getInt(String key) {
        long hash = this.getHashBucket(key);
        long valuePosition = hash * 4;
        return this.valueSegment.get(ValueLayout.JAVA_INT, valuePosition + 4);
    }

    void putInt(String key, Integer value) {
        long keyPosition = this.keyStore.add(key);
        long hash = this.getHashBucket(key);

        long valuePosition = hash * 4;
        this.valueSegment.set(ValueLayout.JAVA_INT, valuePosition, (int) keyPosition);
        this.valueSegment.set(ValueLayout.JAVA_INT, valuePosition + 4, value);
    }

    public DB() {
        this.arena = Arena.ofAuto();
        this.keyStore = new KeyStore(this.arena);

        long bytes = 100000;
        this.valueSegment = this.arena.allocate(bytes, 4);
        //Number of buckets is the total size on bytes / alignment
        this.buckets = bytes/4;
    }
}
