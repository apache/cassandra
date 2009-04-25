package org.apache.cassandra.utils;

import java.io.*;
import java.util.Random;

public class KeyGenerator {
    private static String randomKey(Random r) {
        StringBuffer buffer = new StringBuffer();
        for (int j = 0; j < 16; j++) {
            buffer.append((char)r.nextInt());
        }
        return buffer.toString();
    }

    static class RandomStringGenerator implements ResetableIterator<String> {
        int i, n, seed;
        Random random;

        RandomStringGenerator(int seed, int n) {
            i = 0;
            this.seed = seed;
            this.n = n;
            reset();
        }

        public int size() {
            return n;
        }

        public void reset() {
            random = new Random(seed);
        }

        public boolean hasNext() {
            return i < n;
        }

        public String next() {
            i++;
            return randomKey(random);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static class IntGenerator implements ResetableIterator<String> {
        private int i, start, n;

        IntGenerator(int n) {
            this(0, n);
        }

        IntGenerator(int start, int n) {
            this.start = start;
            this.n = n;
            reset();
        }

        public int size() {
            return n - start;
        }

        public void reset() {
            i = start;
        }

        public boolean hasNext() {
            return i < n;
        }

        public String next() {
            return Integer.toString(i++);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static class WordGenerator implements ResetableIterator<String> {
        static int WORDS;

        static {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/usr/share/dict/words")));
                while (br.ready()) {
                    br.readLine();
                    WORDS++;
                }
            } catch (IOException e) {
                WORDS = 0;
            }
        }

        BufferedReader reader;
        private int modulo;
        private int skip;
        String next;

        WordGenerator(int skip, int modulo) {
            this.skip = skip;
            this.modulo = modulo;
            reset();
        }

        public int size() {
            return (1 + WORDS - skip) / modulo;
        }

        public void reset() {
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream("/usr/share/dict/words")));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < skip; i++) {
                try {
                    reader.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            next();
        }

        public boolean hasNext() {
            return next != null;
        }

        public String next() {
            try {
                String s = next;
                for (int i = 0; i < modulo; i++) {
                    next = reader.readLine();
                }
                return s;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
