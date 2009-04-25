package org.apache.cassandra.utils;

import java.util.Iterator;

public interface ResetableIterator<T> extends Iterator<T> {
    public void reset();

    int size();
}
