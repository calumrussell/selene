package com.selene;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class DBTest {

    @Test
    void testInsert() {
        DB db = new DB();
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 5;

        for (int i = 0; i < 100; i++) {

            Random random = new Random();
            String generatedString = random.ints(leftLimit, rightLimit + 1)
                    .limit(targetStringLength)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            System.out.println(generatedString);
            db.putInt(generatedString, i * 50);
            assertEquals(i*50, db.getInt(generatedString));
        }
    }

}