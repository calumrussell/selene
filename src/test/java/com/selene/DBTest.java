package com.selene;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class DBTest {

    @Test
    void testHashMap() {
        HashMap<String, Integer> map = new HashMap<>(10000);
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 5;

        for (int i = 1; i < 100000; i++) {
            Random random = new Random();
            String generatedString = random.ints(leftLimit, rightLimit + 1)
                    .limit(targetStringLength)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            map.put(generatedString, i);
            assertEquals(i, map.get(generatedString));
        }
    }

    @Test
    void testRepeatedValues() {
        DB db = new DB(10);

        db.putInt("fake", 100);
        db.putInt("another", 100);
        db.putInt("fake", 200);
        assertEquals(200, db.getInt("fake").get());
    }

    @Test
    void testInsert() {
        DB db = new DB(100000);
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 5;

        for (int i = 1; i < 100000; i++) {
            Random random = new Random();
            String generatedString = random.ints(leftLimit, rightLimit + 1)
                    .limit(targetStringLength)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            db.putInt(generatedString, i);
            assertEquals(i, db.getInt(generatedString).get());
        }
    }

}