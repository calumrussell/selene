package com.selene;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class KeyStoreTest {

    @Test
    void testInsertingRetrieving() {
        Arena arena = Arena.ofAuto();
        KeyStore store = new KeyStore(arena);
        long idx0 = store.add("test");
        assertEquals(new String(store.get(idx0)), "test");
        long idx1 = store.add("test2");
        assertEquals(new String(store.get(idx1)), "test2");
        long idx2 = store.add("test22");
        assertEquals(new String(store.get(idx2)), "test22");
        long idx3 = store.add("test222");
        assertEquals(new String(store.get(idx3)), "test222");
        long idx4 = store.add("test2222");
        assertEquals(new String(store.get(idx4)), "test2222");
        long idx5 = store.add("test22222");
        assertEquals(new String(store.get(idx5)), "test22222");
    }

    @Test
    void testInsertingLongString() {
        Arena arena = Arena.ofAuto();
        //Insert value that will overflow initial size immediately
        KeyStore store = new KeyStore(arena);
        String longString = "gbmgvyroigvourovsgbjsemtraojcstceznwgytcfgemkteuyvcpfkdfyxfblgehneoxxssttwbjgykyaelyknxjmousjlmqtmghpg";

        long idx0 = store.add(longString);
        assertEquals(new String(store.get(idx0)), longString);
    }

    @Test
    void testInsertThenOverflow() {
        Arena arena = Arena.ofAuto();
        //Insert shorter value, then a longer one that should overflow
        String longString = "gbmgvyroigvourovsgbjsemtraojcstceznwgytcfgemkteuyvcpfkdfyxfblgehneoxxssttwbjgykyaelyknxjmousjlmqtmghpg";
        KeyStore store = new KeyStore(arena);
        long idx0 = store.add(longString);
        store.add("test");
        assertEquals(new String(store.get(idx0)), longString);
    }
}
