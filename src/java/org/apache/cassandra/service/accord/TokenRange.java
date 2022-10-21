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

import accord.api.RoutingKey;
import accord.primitives.Range;
import accord.primitives.Ranges;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.SentinelKey;

public class TokenRange extends Range.EndInclusive
{
    public TokenRange(AccordRoutingKey start, AccordRoutingKey end)
    {
        super(start, end);
    }

    public static TokenRange fullRange(String keyspace)
    {
        return new TokenRange(SentinelKey.min(keyspace), SentinelKey.max(keyspace));
    }

    @Override
    public TokenRange newRange(RoutingKey start, RoutingKey end)
    {
        return new TokenRange((AccordRoutingKey) start, (AccordRoutingKey) end);
    }

    @Override
    public RoutingKey someIntersectingRoutingKey(Ranges ranges)
    {
        RoutingKey pick = super.someIntersectingRoutingKey(ranges);
        if (pick instanceof SentinelKey)
            pick = ((SentinelKey) pick).toTokenKey();
        return pick;
    }

    public static final IVersionedSerializer<TokenRange> serializer = new IVersionedSerializer<TokenRange>()
    {
        @Override
        public void serialize(TokenRange range, DataOutputPlus out, int version) throws IOException
        {
            AccordRoutingKey.serializer.serialize((AccordRoutingKey) range.start(), out, version);
            AccordRoutingKey.serializer.serialize((AccordRoutingKey) range.end(), out, version);
        }

        @Override
        public TokenRange deserialize(DataInputPlus in, int version) throws IOException
        {
            return new TokenRange(AccordRoutingKey.serializer.deserialize(in, version),
                                  AccordRoutingKey.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(TokenRange range, int version)
        {
            return AccordRoutingKey.serializer.serializedSize((AccordRoutingKey) range.start(), version)
                 + AccordRoutingKey.serializer.serializedSize((AccordRoutingKey) range.end(), version);
        }
    };
}
