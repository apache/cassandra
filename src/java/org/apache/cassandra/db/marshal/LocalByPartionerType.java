package org.apache.cassandra.db.marshal;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

/** for sorting columns representing row keys in the row ordering as determined by a partitioner.
 * Not intended for user-defined CFs, and will in fact error out if used with such. */
public class LocalByPartionerType<T extends Token> extends AbstractType
{
    private final IPartitioner<T> partitioner;

    public LocalByPartionerType(IPartitioner<T> partitioner)
    {
        this.partitioner = partitioner;
    }

    public String getString(byte[] bytes)
    {
        return null;
    }

    public int compare(byte[] o1, byte[] o2)
    {
        return partitioner.decorateKey(o1).compareTo(partitioner.decorateKey(o2));
    }
}
