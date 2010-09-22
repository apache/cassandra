package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.util.Iterator;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.ICompactionInfo;

public interface IKeyIterator extends Iterator<DecoratedKey>, ICompactionInfo, Closeable
{
}
