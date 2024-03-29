package com.selene;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

public class KeyStore {
    /*
        One of the potential problems here is that we are writing an INT and BYTE which are potentially misaligned
        if we are writing to the same MemorySegment when writing BYTE of length that aren't % 4. We can use
        UNALIGNED ValueLayout but the docs seem to say this isn't a good idea and might not be portable (for example,
        long seems to have a size that can change between machines, so it makes sense not to use UNALIGNED ValueLayout
        that will become unaligned on new machine).

        So the solution is to hold two MemorySegments, one that holds the INT length and this is used to index into the
        BYTE MemorySegment that is holding the actual key strings.
     */

    private long currentValueSize;
    private long currentLengthSize;
    private Arena arena;
    // This is poorly named but it is [int, int] with the length of the string and the starting position of that string
    // within the valueSegment. This is used because it makes sense to keep segments of single type. When you mix,
    // it can lead to alignment problems.
    private MemorySegment lengthSegment;
    private MemorySegment valueSegment;
    private long valueWritePosition;
    private long lengthWritePosition;

    private void resizeLengthSegment() {
        MemorySegment newSegment = this.arena.allocate(this.currentLengthSize * 2 * 4, 4);
        newSegment.copyFrom(this.lengthSegment);
        this.lengthSegment = newSegment;
        this.currentLengthSize = this.currentLengthSize * 2;
    }

    private void resizeValueSegment(long min) {
        var minimumValueSize = Long.max(min + 1, this.currentValueSize * 2);

        MemorySegment newSegment = this.arena.allocate(minimumValueSize, 1);
        newSegment.copyFrom(this.valueSegment);
        this.valueSegment = newSegment;
        this.currentValueSize = this.currentValueSize * 2;
    }

    public byte[] get(long offset) {
        // offset is the real starting position within the memory segment, not an index into array-like
        long length = this.lengthSegment.get(ValueLayout.JAVA_INT, offset);
        long startingPosition = this.lengthSegment.get(ValueLayout.JAVA_INT, offset+ 4);
        return this.valueSegment.asSlice(startingPosition, length).toArray(ValueLayout.JAVA_BYTE);
    }

    private boolean checkForValueResize(int newStringLength) {
        return newStringLength + 4 > this.currentValueSize - this.valueWritePosition;
    }

    private boolean checkForLengthResize() {
        return this.lengthWritePosition + 8 > this.currentLengthSize;
    }

    public long add(String key) {
        int len = key.length();
        if (checkForValueResize(len)) {
            this.resizeValueSegment(len);
        }

        if (checkForLengthResize()) {
            this.resizeLengthSegment();
        }

        // Write length
        this.lengthSegment.set(ValueLayout.JAVA_INT, this.lengthWritePosition, len);
        // Write starting position of value in valueSegment
        this.lengthWritePosition+=4;
        this.lengthSegment.set(ValueLayout.JAVA_INT, this.lengthWritePosition, (int) this.valueWritePosition);
        this.lengthWritePosition+=4;

        for (byte b: key.getBytes(StandardCharsets.UTF_8)) {
            this.valueSegment.set(ValueLayout.JAVA_BYTE, this.valueWritePosition, b);
            this.valueWritePosition++;
        }
        // We return the index into the lengthSegment, which can then retrieve from the valueSegment
        return this.lengthWritePosition-8;
    }

    public KeyStore(Arena arena, long size) {
        // The arena for this comes from another owned data structure so KeyStore shouldn't worry about this reference
        // becoming invalidated as KeyStore should be only created by a DB, and has lifetime at least as long as DB.
        this.arena = arena;
        this.currentLengthSize = size+1;
        this.currentValueSize = size+1;
        this.lengthSegment = arena.allocate(this.currentLengthSize * 4, 4);
        this.valueSegment = arena.allocate(this.currentValueSize, 1);
        this.valueWritePosition = 1;
        this.lengthWritePosition = 4;
    }
}
