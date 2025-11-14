package com.gpb.jdata.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ResultSetIterator implements Iterator<ResultSet> {
        private final ResultSet rs;
        private Boolean hasNextCache = null;

        public ResultSetIterator(ResultSet rs) {
            this.rs = rs;
        }

        @Override
        public boolean hasNext() {
            if (hasNextCache != null) return hasNextCache;
            try {
                boolean hn = rs.next();
                hasNextCache = hn;
                return hn;
            } catch (SQLException e) {
                throw new RuntimeException("Error advancing ResultSet", e);
            }
        }

        @Override
        public ResultSet next() {
            if (hasNextCache != null) {
                if (!hasNextCache) throw new NoSuchElementException();
                // hasNextCache == true, текущая строка уже готова в rs (we advanced in hasNext)
                hasNextCache = null; // сбросим — следующий hasNext вызовет rs.next()
                return rs;
            } else {
                // hasNext() не вызывался, попробуем продвинуться прямо
                try {
                    if (!rs.next()) throw new NoSuchElementException();
                    return rs;
                } catch (SQLException e) {
                    throw new RuntimeException("Error reading ResultSet", e);
                }
            }
        }
    }