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

package org.apache.cassandra.service.accord;

import java.io.IOException;

import com.google.common.base.Preconditions;

import accord.api.KeyRange;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.api.AccordKey.SentinelKey;
import org.apache.cassandra.service.accord.api.AccordKey.TokenKey;

public class TokenRange extends KeyRange.EndInclusive<AccordKey>
{
    public TokenRange(AccordKey start, AccordKey end)
    {
        super(start, end);
        Preconditions.checkArgument(!(start instanceof AccordKey.PartitionKey));
        Preconditions.checkArgument(!(end instanceof AccordKey.PartitionKey));
    }

    private static AccordKey toAccordTokenOrSentinel(AccordKey key)
    {
        if (key instanceof TokenKey || key instanceof SentinelKey)
            return key;
        return new TokenKey(key.tableId(), key.partitionKey().getToken().maxKeyBound());
    }

    public static TokenRange fullRange(TableId tableId)
    {
        return new TokenRange(SentinelKey.min(tableId), SentinelKey.max(tableId));
    }

    @Override
    public TokenRange subRange(AccordKey start, AccordKey end)
    {
        return new TokenRange(toAccordTokenOrSentinel(start), toAccordTokenOrSentinel(end));
    }

    // FIXME: serialize sentinel keys
    public static final IVersionedSerializer<TokenRange> serializer = new IVersionedSerializer<TokenRange>()
    {
        @Override
        public void serialize(TokenRange range, DataOutputPlus out, int version) throws IOException
        {
            TokenKey.serializer.serialize((TokenKey) range.start(), out, version);
            TokenKey.serializer.serialize((TokenKey) range.end(), out, version);
        }

        @Override
        public TokenRange deserialize(DataInputPlus in, int version) throws IOException
        {
            return new TokenRange(TokenKey.serializer.deserialize(in, version),
                                  TokenKey.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(TokenRange range, int version)
        {
            return TokenKey.serializer.serializedSize((TokenKey) range.start(), version)
                 + TokenKey.serializer.serializedSize((TokenKey) range.end(), version);
        }
    };
}
