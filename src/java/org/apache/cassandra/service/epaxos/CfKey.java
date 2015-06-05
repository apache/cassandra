package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.Schema;

import java.nio.ByteBuffer;
import java.util.UUID;

public class CfKey
{
    public final ByteBuffer key;
    public final UUID cfId;

    public CfKey(ByteBuffer key, UUID cfId)
    {
        assert key != null;
        assert cfId != null;

        this.key = key;
        this.cfId = cfId;
    }

    public CfKey(ByteBuffer key, String ks, String cf)
    {
        this(key, Schema.instance.getId(ks, cf));
    }

    @Override
    public String toString()
    {
        return "CfKey{" +
                "key=" + key +
                ", cfId=" + cfId +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CfKey cfKey = (CfKey) o;

        if (!cfId.equals(cfKey.cfId)) return false;
        if (!key.equals(cfKey.key)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = key.hashCode();
        result = 31 * result + cfId.hashCode();
        return result;
    }
}
