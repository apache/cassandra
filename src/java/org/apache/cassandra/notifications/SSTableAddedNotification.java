package org.apache.cassandra.notifications;

import org.apache.cassandra.io.sstable.SSTableReader;

import java.util.List;

public class SSTableAddedNotification implements INotification
{
    public SSTableReader added;
    public SSTableAddedNotification(SSTableReader added)
    {
        this.added = added;
    }
}
