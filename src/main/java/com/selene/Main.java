package com.selene;

public class Main {
    public static void main(String[] args) {
        KeyStore store = new KeyStore();
        long idx0 = store.add("test");
        long idx1 = store.add("vals");
        long idx2 = store.add("antiquarian");

        System.out.println(new String(store.get(idx0)));
        System.out.println(new String(store.get(idx1)));
        System.out.println(new String(store.get(idx2)));
    }
}
