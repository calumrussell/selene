package com.selene;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class DBTest {

    @Test
    void testHashMap() {
        HashMap<String, Integer> map = new HashMap<>(100);
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 5;

        for (int i = 1; i < 1000; i++) {
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
    void testInsert() {
        DB db = new DB(100);
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 5;

        for (int i = 1; i < 10000; i++) {

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