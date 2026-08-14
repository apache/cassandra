/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.index.sai.disk.v2;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public final class TokenOnlyPrimaryKey implements PrimaryKey
{
    private final Token token;

    public TokenOnlyPrimaryKey(Token token)
    {
        this.token = token;
    }

    @Override
    public boolean isTokenOnly()
    {
        return true;
    }

    @Override
    public Token token()
    {
        return token;
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return null;
    }

    @Override
    public Clustering<?> clustering()
    {
        return null;
    }

    @Override
    public ByteSource asComparableBytes(Version version)
    {
        return asComparableBytes(version == ByteComparable.Version.LEGACY ? ByteSource.END_OF_STREAM : ByteSource.TERMINATOR, version, false);
    }

    @Override
    public ByteSource asComparableBytesMinPrefix(Version version)
    {
        return asComparableBytes(ByteSource.LT_NEXT_COMPONENT, version, true);
    }

    @Override
    public ByteSource asComparableBytesMaxPrefix(Version version)
    {
        return asComparableBytes(ByteSource.GT_NEXT_COMPONENT, version, true);
    }

    private ByteSource asComparableBytes(int terminator, ByteComparable.Version version, boolean isPrefix)
    {
        ByteSource tokenComparable = token.asComparableBytes(version);
        // prefix doesn't include null components
        if (isPrefix)
            return ByteSource.withTerminator(terminator, tokenComparable);
        else
            return ByteSource.withTerminator(terminator, tokenComparable, null, null);
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        return token().compareTo(o.token());
    }

    @Override
    public long ramBytesUsed()
    {
        // Object header + 1 reference (token) + implicit outer reference + token size
        return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF + token.getHeapSize();
    }

    @Override
    public PrimaryKey forStaticRow()
    {
        return this;
    }

    @Override
    public PrimaryKey loadDeferred()
    {
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof PrimaryKey)
            return compareTo((PrimaryKey) o) == 0;
        return false;
    }

    @Override
    public int hashCode()
    {
        return token().hashCode();
    }

    @Override
    public String toString()
    {
        return String.format("TokenOnlyPrimaryKey: { token: %s }", token());
    }
}
