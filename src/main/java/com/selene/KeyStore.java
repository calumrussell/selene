package com.selene;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

public class KeyStore {

    private static final long INT_SIZE = 4;
    private static final long INITIAL_SIZE = 100;
    private long currentSize;
    private Arena arena;
    private MemorySegment keySegment;
    private long writePosition;

    private void resize() {
        Arena newArena = Arena.ofConfined();
        MemorySegment newSegment = newArena.allocate(this.currentSize * 2, 1);

        //MemorySegment.copy(this.keySegment, 0, newSegment, 0, this.currentSize);
        newSegment.copyFrom(this.keySegment);
        this.arena = newArena;
        this.keySegment = newSegment;
        this.currentSize = this.currentSize * 2;
    }

    //Only supports indexed queries
    public byte[] get(long offset) {
        long length = this.keySegment.get(ValueLayout.JAVA_INT, offset);
        byte[] result = new byte[(int) length];
        for (int i = 0; i < length; i++) {
            result[i] = this.keySegment.get(ValueLayout.JAVA_BYTE, offset + 4 + i);
        }
        return result;
    }

    public long add(String key) {
        int len = key.length();
        if (len + INT_SIZE > this.currentSize - this.writePosition) {
            this.resize();
        }

        long startPosition = this.writePosition;
        this.keySegment.set(ValueLayout.JAVA_INT, this.writePosition, len);
        this.writePosition += INT_SIZE;
        for (byte b: key.getBytes(StandardCharsets.UTF_8)) {
            this.keySegment.set(ValueLayout.JAVA_BYTE, this.writePosition, b);
            this.writePosition++;
        }
        return startPosition;
    }

    public KeyStore() {
        this.arena = Arena.ofConfined();
        this.currentSize = INITIAL_SIZE;
        this.keySegment = this.arena.allocate(this.currentSize, 1);
        this.writePosition = 0;
    }
}
