package org.apache.cassandra.db;

import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;

import org.apache.cassandra.io.AbstractCompactedRow;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;

/**
 * A CompactedRow implementation that just echos the original row bytes without deserializing.
 * Currently only used by cleanup.
 */
public class EchoedRow extends AbstractCompactedRow
{
    private final SSTableIdentityIterator row;

    public EchoedRow(SSTableIdentityIterator row)
    {
        super(row.getKey());
        this.row = row;
        // Reset SSTableIdentityIterator because we have not guarantee the filePointer hasn't moved since the Iterator was built
        row.reset();
    }

    public void write(DataOutput out) throws IOException
    {
        assert row.dataSize > 0;
        out.writeLong(row.dataSize);
        row.echoData(out);
    }

    public void update(MessageDigest digest)
    {
        // EchoedRow is not used in anti-entropy validation
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty()
    {
        return !row.hasNext();
    }

    public int columnCount()
    {
        return row.columnCount;
    }
}
