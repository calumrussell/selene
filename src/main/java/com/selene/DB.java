package com.selene;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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

    private void resizeValueSegment() {
        // When resizing we need to go through all the values that currently exist and re-insert them as the hash might
        // have changed. We only need to move the valueSegment, the values in the KeyStore can stay in the same place,
        // we just retrieve the value for rehashing.

        long newLoadFactor = 0;
        long newBucketSize = this.buckets * 2;
        long newFilledBuckets = 0;
        MemorySegment newSegment = this.arena.allocate(newBucketSize * 8, 4);

        for (int i = 0; i < this.buckets; i++) {
            int position = i * 8;
            int bucketKey = this.valueSegment.get(ValueLayout.JAVA_INT, position);
            int bucketValue = this.valueSegment.get(ValueLayout.JAVA_INT, position + 4);
            if (bucketKey!=0 && bucketValue!=0) {
                // This doesn't work if we have a collision

                String key = new String(this.keyStore.get(bucketKey));
                long newHash = this.getHashBucket(key, newBucketSize);
                long valuePosition = newHash * 8;

                newSegment.set(ValueLayout.JAVA_INT, valuePosition, bucketKey);
                newSegment.set(ValueLayout.JAVA_INT, valuePosition + 4, bucketValue);

                newLoadFactor = newLoadFactor + 1 / newBucketSize;
                newFilledBuckets++;
            }
        }
        this.buckets = newBucketSize;
        this.filledBuckets = newFilledBuckets;
        this.valueSegment = newSegment;

    }

    private long getHashBucket(String key, long buckets) {
        // buckets is a param because we want to calculate hash with a different value when we are resizing
        long hash = key.hashCode();
        return Math.abs(hash) % buckets;
    }

    Optional<Integer> getInt(String key) {
        long hash = this.getHashBucket(key, this.buckets);
        long valuePosition = hash * 8;

        // We will eventually find the value or an empty bucket
        while (true) {
            int bucketKey = this.valueSegment.get(ValueLayout.JAVA_INT, valuePosition);
            int bucketValue = this.valueSegment.get(ValueLayout.JAVA_INT, valuePosition + 4);
            if (bucketKey == 0 && bucketValue == 0) {
                // Value doesn't exist
                return Optional.empty();
            } else  {
                // Value does exist
                String bucketKeyString = new String(this.keyStore.get(bucketKey));
                // Check if it matches
                if (bucketKeyString.equals(key)) {
                    // Found a match, return the value
                    return Optional.of(this.valueSegment.get(ValueLayout.JAVA_INT, valuePosition + 4));
                } else {
                    // Doesn't match, look in next bucket
                    if (valuePosition + 8 >= this.buckets * 8) {
                        valuePosition = 0;
                    } else {
                        valuePosition += 8;
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

        // Check if bucket has value, if there is a value move forward 8 bytes until we find a free bucket
        boolean foundEmptyPosition = false;
        long valuePosition = hash * 8;
        while (!foundEmptyPosition) {
            int bucketKey = this.valueSegment.get(ValueLayout.JAVA_INT, valuePosition);
            int bucketValue = this.valueSegment.get(ValueLayout.JAVA_INT, valuePosition + 4);
            if (bucketKey != 0 && bucketValue != 0) {
                if (valuePosition + 8 > this.buckets * 8) {
                    valuePosition = 0;
                } else {
                    valuePosition += 8;
                }
            } else {
                foundEmptyPosition = true;
            }
        }

        long keyPosition = this.keyStore.add(key);
        this.valueSegment.set(ValueLayout.JAVA_INT, valuePosition, (int) keyPosition);
        this.valueSegment.set(ValueLayout.JAVA_INT, valuePosition + 4, value);
        this.filledBuckets++;
    }

    public DB(long size) {
        this.arena = Arena.ofConfined();
        this.keyStore = new KeyStore(this.arena, size);

        //Each bucket is 8 bytes long, we multiply the number of buckets by 8 to allocate the memory.
        this.buckets = size;
        this.filledBuckets = 0;
        this.valueSegment = this.arena.allocate(buckets * 8, 4);
    }
}
