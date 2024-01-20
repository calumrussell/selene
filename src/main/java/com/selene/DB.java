package com.selene;

import java.lang.foreign.*;
import java.lang.invoke.VarHandle;
import java.util.Optional;

public class DB {
    /*
        Off-heap data structure that stores String keys and Integer values.

        The String keys are offloaded to the KeyStore. The layout of memory is [int, int] representing the position of
        the key in KeyStore and the integer stored at this value.

        We need to store the key so it can be re-hashed if we need to resize.
     */

    private final KeyStore keyStore;
    private final Arena arena;
    private MemorySegment valueSegment;
    private long buckets;
    private long filledBuckets;
    private final MemoryLayout memoryLayout;
    private SequenceLayout sequenceLayout;
    private VarHandle keyStorePositionHandle;
    private VarHandle valueHandle;

    private void resizeValueSegment() {
        // When resizing we need to go through all the values that currently exist and re-insert them as the hash might
        // have changed. We only need to move the valueSegment, the values in the KeyStore can stay in the same place,
        // we just retrieve the value for rehashing.

        long newBucketSize = this.buckets * 2;
        long newFilledBuckets = 0;

        SequenceLayout newSequenceLayout = MemoryLayout.sequenceLayout(newBucketSize, this.memoryLayout);
        VarHandle newKeyStorePositionHandle = newSequenceLayout.varHandle(MemoryLayout.PathElement.sequenceElement(),
                MemoryLayout.PathElement.groupElement("keyStorePosition"));
        VarHandle newValueHandle = newSequenceLayout.varHandle(MemoryLayout.PathElement.sequenceElement(),
                MemoryLayout.PathElement.groupElement("value"));
        MemorySegment newSegment = this.arena.allocate(newSequenceLayout);

        for (int i = 0; i < this.sequenceLayout.elementCount(); i++) {
            int bucketKey = (int) this.keyStorePositionHandle.get(this.valueSegment, i);
            int bucketValue = (int) this.valueHandle.get(this.valueSegment, i);
            if (bucketKey!=0 && bucketValue!=0) {
                // This doesn't work if we have a collision

                String key = new String(this.keyStore.get(bucketKey));
                long newHash = this.getHashBucket(key, newBucketSize);

                newKeyStorePositionHandle.set(newSegment, newHash, bucketKey);
                newValueHandle.set(newSegment, newHash, bucketValue);

                newFilledBuckets++;
            }
        }
        this.buckets = newBucketSize;
        this.filledBuckets = newFilledBuckets;
        this.sequenceLayout = newSequenceLayout;
        this.keyStorePositionHandle = newKeyStorePositionHandle;
        this.valueHandle = newValueHandle;
        this.valueSegment = newSegment;

    }

    private long getHashBucket(String key, long buckets) {
        // buckets is a param because we want to calculate hash with a different value when we are resizing
        long hash = key.hashCode();
        return Math.abs(hash) % buckets;
    }

    Optional<Integer> getInt(String key) {
        long hash = this.getHashBucket(key, this.buckets);

        // We will eventually find the value or an empty bucket
        while (true) {
            int bucketKey = (int) this.keyStorePositionHandle.get(this.valueSegment, hash);
            int bucketValue = (int) this.valueHandle.get(this.valueSegment, hash);
            if (bucketKey == 0 && bucketValue == 0) {
                // Value doesn't exist
                return Optional.empty();
            } else  {
                // Value does exist
                String bucketKeyString = new String(this.keyStore.get(bucketKey));
                // Check if it matches
                if (bucketKeyString.equals(key)) {
                    // Found a match, return the value
                    return Optional.of((int) this.valueHandle.get(this.valueSegment, hash));
                } else {
                    // Doesn't match, look in next bucket
                    if (hash >= this.buckets - 1) {
                        hash = 0;
                    } else {
                        hash++;
                    }
                }
            }
        }
    }

    private double getLoadFactor() {
        return (double) this.filledBuckets/ (double) this.buckets;
    }

    void putInt(String key, Integer value) {
        if (this.getLoadFactor() > 0.55) {
            this.resizeValueSegment();
        }

        long hash = this.getHashBucket(key, buckets);

        while (true) {
            int bucketKey = (int) this.keyStorePositionHandle.get(this.valueSegment, hash);
            int bucketValue = (int) this.valueHandle.get(this.valueSegment, hash);
            if (bucketKey != 0 && bucketValue != 0) {
                // If there is already a value here then we need to check whether the key already exists. If it does
                // then we update the keyPosition and the value
                String bucketKeyString = new String(this.keyStore.get(bucketKey));
                if (bucketKeyString.equals(key)) {
                    break;
                } else {
                    if (hash >= this.buckets - 1) {
                        hash = 0;
                    } else {
                        hash++;
                    }
                }
            } else {
                break;
            }
        }

        long keyPosition = this.keyStore.add(key);
        this.keyStorePositionHandle.set(this.valueSegment, hash, (int) keyPosition);
        this.valueHandle.set(this.valueSegment, hash, value);
        this.filledBuckets++;
    }

    public DB(long size) {

        this.memoryLayout = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("keyStorePosition"),
                ValueLayout.JAVA_INT.withName("value")
        );
        this.sequenceLayout = MemoryLayout.sequenceLayout(size, this.memoryLayout);
        this.keyStorePositionHandle = this.sequenceLayout.varHandle(MemoryLayout.PathElement.sequenceElement(),
                MemoryLayout.PathElement.groupElement("keyStorePosition"));
        this.valueHandle = this.sequenceLayout.varHandle(MemoryLayout.PathElement.sequenceElement(),
                MemoryLayout.PathElement.groupElement("value"));

        this.arena = Arena.ofConfined();
        this.keyStore = new KeyStore(this.arena, size);

        this.buckets = size;
        this.filledBuckets = 0;

        this.valueSegment = this.arena.allocate(this.sequenceLayout);
    }
}
