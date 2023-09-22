/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.utils;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class ScoredPrimaryKey implements PrimaryKey
{
    public final PrimaryKey primaryKey;
    public final float score;

    public ScoredPrimaryKey(PrimaryKey primaryKey, float score)
    {
        this.primaryKey = primaryKey;
        this.score = score;
    }

    public static ScoredPrimaryKey create(PrimaryKey key, float score)
    {
        return new ScoredPrimaryKey(key, score);
    }

    @Override
    public Token token()
    {
        return primaryKey.token();
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return primaryKey.partitionKey();
    }

    @Override
    public Clustering clustering()
    {
        return primaryKey.clustering();
    }

    @Override
    public PrimaryKey loadDeferred()
    {
        return primaryKey.loadDeferred();
    }

    @Override
    public ByteSource asComparableBytes(ByteComparable.Version version)
    {
        return primaryKey.asComparableBytes(version);
    }

    @Override
    public ByteSource asComparableBytesMinPrefix(ByteComparable.Version version)
    {
        return primaryKey.asComparableBytesMinPrefix(version);
    }

    @Override
    public ByteSource asComparableBytesMaxPrefix(ByteComparable.Version version)
    {
        return primaryKey.asComparableBytesMaxPrefix(version);
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        return primaryKey.compareTo(o);
    }
}
