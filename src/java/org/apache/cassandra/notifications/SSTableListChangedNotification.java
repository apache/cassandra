package org.apache.cassandra.notifications;

import org.apache.cassandra.io.sstable.SSTableReader;

import java.util.List;

public class SSTableListChangedNotification implements INotification
{
    public Iterable<SSTableReader> removed;
    public Iterable<SSTableReader> added;
    public SSTableListChangedNotification(Iterable<SSTableReader> added, Iterable<SSTableReader> removed)
    {
        this.removed = removed;
        this.added = added;
    }
}
