package org.apache.cassandra.dht;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;

public class LocalPartitioner implements IPartitioner<LocalToken>
{
    private final AbstractType comparator;

    public LocalPartitioner(AbstractType comparator)
    {
        this.comparator = comparator;
    }

    public DecoratedKey<LocalToken> convertFromDiskFormat(byte[] key)
    {
        return decorateKey(key);
    }

    public DecoratedKey<LocalToken> decorateKey(byte[] key)
    {
        return new DecoratedKey<LocalToken>(getToken(key), key);
    }

    public LocalToken midpoint(LocalToken left, LocalToken right)
    {
        throw new UnsupportedOperationException();
    }

    public LocalToken getMinimumToken()
    {
        return new LocalToken(comparator, ArrayUtils.EMPTY_BYTE_ARRAY);
    }

    public LocalToken getToken(byte[] key)
    {
        return new LocalToken(comparator, key);
    }

    public LocalToken getRandomToken()
    {
        throw new UnsupportedOperationException();
    }

    public Token.TokenFactory getTokenFactory()
    {
        throw new UnsupportedOperationException();
    }

    public boolean preservesOrder()
    {
        return true;
    }
}
