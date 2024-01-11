package com.selene;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class DB {

    private final Arena arena;
    private final MemorySegment valueSegment;
    private final MemorySegment keySegment;
    private long position;
    private ValueLayout layout;
    private final long dataSize = 4;
    private long loadFactor;
    private long buckets;

    private long getHashBucket(Integer key) {
        long hash = key.hashCode();
        long alwaysPositive = hash ^ (hash >>> 32);
        return Math.abs(hash) % (this.buckets - 1);
    }

    void putInt(Integer key) {

        this.valueSegment.set((ValueLayout.OfInt) layout, this.position, key);
        this.position += dataSize;
    }

    Integer getInt(long pos) {
        return this.valueSegment.get((ValueLayout.OfInt) this.layout, pos * dataSize);
    }

    public DB(long size) {
        this.arena = Arena.ofAuto();
        this.keySegment = this.arena.allocate(size * dataSize, dataSize);
        this.valueSegment = this.arena.allocate(size * dataSize, dataSize);
        this.position = 0;
        this.loadFactor = 0;
        this.buckets = size;
        this.layout = ValueLayout.JAVA_INT;
    }
}
