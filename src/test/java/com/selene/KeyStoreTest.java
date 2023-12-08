package com.selene;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KeyStoreTest {

    @Test
    void testInsertingRetrieving() {
        KeyStore store = new KeyStore();
        long idx0 = store.add("test");
        long idx1 = store.add("test2");
        long idx2 = store.add("test22");
        long idx3 = store.add("test222");
        long idx4 = store.add("test2222");
        long idx5 = store.add("test22222");
        assertEquals(new String(store.get(idx0)), "test");
        assertEquals(new String(store.get(idx1)), "test2");
        assertEquals(new String(store.get(idx2)), "test22");
        assertEquals(new String(store.get(idx3)), "test222");
        assertEquals(new String(store.get(idx4)), "test2222");
        assertEquals(new String(store.get(idx5)), "test22222");
    }

    @Test
    void testInsertingLongString() {
        //Insert value that will overflow initial size immediately
        KeyStore store = new KeyStore();
        String longString = "gbmgvyroigvourovsgbjsemtraojcstceznwgytcfgemkteuyvcpfkdfyxfblgehneoxxssttwbjgykyaelyknxjmousjlmqtmghpg";
        long idx0 = store.add(longString);
        assertEquals(new String(store.get(idx0)), longString);
    }

    @Test
    void testInsertThenOverflow() {
        //Insert shorter value, then a longer one that should overflow
        String longString = "gbmgvyroigvourovsgbjsemtraojcstceznwgytcfgemkteuyvcpfkdfyxfblgehneoxxssttwbjgykyaelyknxjmousjlmqtmghpg";
        KeyStore store = new KeyStore();
        long idx0 = store.add(longString);
        store.add("test");
        assertEquals(new String(store.get(idx0)), longString);
    }
}