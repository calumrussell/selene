package com.selene;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Optional;

public class DB {
  /*
   * Off-heap data structure that stores String keys and Integer values.
   * 
   * The String keys are offloaded to the KeyStore. The layout of memory is [int,
   * int] representing the position of
   * the key in KeyStore and the integer stored at this value.
   * 
   * We need to store the key so it can be re-hashed if we need to resize.
   */

  private final KeyStore keyStore;
  private final Arena arena;
  private MemorySegment valueSegment;
  private long buckets;
  private long filledBuckets;
  private final MemoryLayout memoryLayout;

  private static int getKeyStorePosition(MemorySegment segment, long hash) {
    return segment.getAtIndex(ValueLayout.JAVA_INT, hash * 2);
  }

  private static void setKeyStorePosition(MemorySegment segment, long hash, int value) {
    segment.setAtIndex(ValueLayout.JAVA_INT, hash * 2, value);
  }

  private static int getValue(MemorySegment segment, long hash) {
    return segment.getAtIndex(ValueLayout.JAVA_INT, (hash * 2) + 1);
  }

  private static void setValue(MemorySegment segment, long hash, int value) {
    segment.setAtIndex(ValueLayout.JAVA_INT, (hash * 2) + 1, value);
  }

  private void resizeValueSegment() {
    // When resizing we need to go through all the values that currently exist and
    // re-insert them as the hash might
    // have changed. We only need to move the valueSegment, the values in the
    // KeyStore can stay in the same place,
    // we just retrieve the value for rehashing.

    long newBucketSize = this.buckets * 2;
    long newFilledBuckets = 0;
    var sequenceLayout = MemoryLayout.sequenceLayout(newBucketSize, this.memoryLayout);
    var newSegment = this.arena.allocate(sequenceLayout);

    for (int i = 0; i < this.buckets; i++) {
      int bucketKey = DB.getKeyStorePosition(this.valueSegment, i);
      int bucketValue = DB.getValue(this.valueSegment, i);
      if (bucketKey != 0 && bucketValue != 0) {
        byte[] key = this.keyStore.get(bucketKey);
        long newHash = this.getHashBucket(key, newBucketSize);

        DB.setKeyStorePosition(newSegment, newHash, bucketKey);
        DB.setValue(newSegment, newHash, bucketValue);
        newFilledBuckets++;
      }
    }
    this.buckets = newBucketSize;
    this.filledBuckets = newFilledBuckets;
    this.valueSegment = newSegment;
  }

  private long getHashBucket(byte[] key, long buckets) {
    // buckets is a param because we want to calculate hash with a different value
    // when we are resizing
    long hash = Arrays.hashCode(key);
    return Math.abs(hash) % buckets;
  }

  Optional<Integer> getInt(String key) {
    byte[] keyBytes = key.getBytes();
    long hash = this.getHashBucket(keyBytes, this.buckets);

    // We will eventually find the value or an empty bucket
    while (true) {
      int bucketKey = DB.getKeyStorePosition(this.valueSegment, hash);
      int bucketValue = DB.getValue(this.valueSegment, hash);
      if (bucketKey == 0 && bucketValue == 0) {
        // Value doesn't exist
        return Optional.empty();
      } else {
        // Value does exist
        byte[] bucketKeyString = this.keyStore.get(bucketKey);
        // Check if it matches
        if (Arrays.equals(keyBytes, bucketKeyString)) {
          // Found a match, return the value
          int value = DB.getValue(this.valueSegment, hash);
          return Optional.of(value);
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
    return (double) this.filledBuckets / (double) this.buckets;
  }

  void putInt(String key, Integer value) {
    if (this.getLoadFactor() > 0.55) {
      this.resizeValueSegment();
    }
    byte[] keyBytes = key.getBytes();
    long hash = this.getHashBucket(keyBytes, buckets);

    while (true) {
      int bucketKey = DB.getKeyStorePosition(this.valueSegment, hash);
      int bucketValue = DB.getValue(this.valueSegment, hash);
      if (bucketKey != 0 && bucketValue != 0) {
        // If there is already a value here then we need to check whether the key
        // already exists. If it does
        // then we update the keyPosition and the value
        byte[] bucketKeyString = this.keyStore.get(bucketKey);
        if (Arrays.equals(keyBytes, bucketKeyString)) {
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
    DB.setKeyStorePosition(this.valueSegment, hash, (int) keyPosition);
    DB.setValue(this.valueSegment, hash, value);
    this.filledBuckets++;
  }

  public DB(long size) {

    this.memoryLayout = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("keyStorePosition"),
        ValueLayout.JAVA_INT.withName("value"));
    var sequenceLayout = MemoryLayout.sequenceLayout(size, this.memoryLayout);

    this.arena = Arena.global();
    this.keyStore = new KeyStore(this.arena, size);

    this.buckets = size;
    this.filledBuckets = 0;

    this.valueSegment = this.arena.allocate(sequenceLayout);
  }
}
