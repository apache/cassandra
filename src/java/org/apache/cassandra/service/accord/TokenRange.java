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

import accord.api.Key;
import accord.primitives.KeyRange;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.api.AccordKey.SentinelKey;
import org.apache.cassandra.service.accord.api.AccordKey.TokenKey;

public class TokenRange extends KeyRange.EndInclusive
{
    public TokenRange(AccordKey start, AccordKey end)
    {
        super(start, end);
        Preconditions.checkArgument(start.kind().supportsRanges());
        Preconditions.checkArgument(end.kind().supportsRanges());
    }

    private static AccordKey toAccordTokenOrSentinel(Key key)
    {
        AccordKey aKey = (AccordKey) key;
        if (aKey instanceof TokenKey || aKey instanceof SentinelKey)
            return aKey;
        return new TokenKey(aKey.tableId(), aKey.partitionKey().getToken().maxKeyBound());
    }

    public static TokenRange fullRange(TableId tableId)
    {
        return new TokenRange(SentinelKey.min(tableId), SentinelKey.max(tableId));
    }

    @Override
    public TokenRange subRange(Key start, Key end)
    {
        return new TokenRange(toAccordTokenOrSentinel(start), toAccordTokenOrSentinel(end));
    }

    public static final IVersionedSerializer<TokenRange> serializer = new IVersionedSerializer<TokenRange>()
    {
        @Override
        public void serialize(TokenRange range, DataOutputPlus out, int version) throws IOException
        {
            AccordKey.serializer.serialize((AccordKey) range.start(), out, version);
            AccordKey.serializer.serialize((AccordKey) range.end(), out, version);
        }

        @Override
        public TokenRange deserialize(DataInputPlus in, int version) throws IOException
        {
            return new TokenRange(AccordKey.serializer.deserialize(in, version),
                                  AccordKey.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(TokenRange range, int version)
        {
            return AccordKey.serializer.serializedSize((AccordKey) range.start(), version)
                 + AccordKey.serializer.serializedSize((AccordKey) range.end(), version);
        }
    };
}
