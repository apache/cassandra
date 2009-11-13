package org.apache.cassandra.utils;

import java.io.Closeable;
import java.util.Iterator;

// so we can instantiate anonymous classes implementing both interfaces
public interface CloseableIterator<T> extends Iterator<T>, Closeable
{
}
