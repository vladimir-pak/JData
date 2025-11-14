package com.gpb.jdata.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class IteratorStringInputStream extends InputStream {
        private final Iterator<String> it;
        private byte[] buffer = new byte[0];
        private int pos = 0;

        public IteratorStringInputStream(Iterator<String> it) {
            this.it = it;
        }

        @Override
        public int read() throws IOException {
            if (pos >= buffer.length) {
                if (!it.hasNext()) return -1;
                String next = it.next();
                if (next == null) return -1;
                buffer = next.getBytes(StandardCharsets.UTF_8);
                pos = 0;
            }
            return buffer[pos++] & 0xFF;
        }

        @Override
        public void close() throws IOException {
            // nothing special — underlying resources (ResultSet/Statement) closed by caller
            super.close();
        }
    }
