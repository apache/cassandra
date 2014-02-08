package org.apache.cassandra.notifications;

import java.util.Collection;

import org.apache.cassandra.io.sstable.SSTableReader;

public class SSTableRepairStatusChanged implements INotification
{
    public final Collection<SSTableReader> sstable;

    public SSTableRepairStatusChanged(Collection<SSTableReader> repairStatusChanged)
    {
        this.sstable = repairStatusChanged;
    }
}
