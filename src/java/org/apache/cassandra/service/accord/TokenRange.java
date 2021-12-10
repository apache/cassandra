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

import com.google.common.base.Preconditions;

import accord.api.KeyRange;
import accord.topology.KeyRanges;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordKey;

public class TokenRange extends KeyRange.EndInclusive<AccordKey>
{
    public TokenRange(TableId tableId, Range<Token> range)
    {
        this(tableId, range.left, range.right);
    }

    public TokenRange(TableId tableId, Token start, Token end)
    {
        this(new AccordKey.TokenKey(tableId, start.maxKeyBound()),
             new AccordKey.TokenKey(tableId, end.maxKeyBound()));
        Preconditions.checkArgument(start().tableId().equals(end().tableId()));
    }

    public TokenRange(AccordKey.TokenKey start, AccordKey.TokenKey end)
    {
        super(start, end);
    }

    private static AccordKey.TokenKey toAccordToken(AccordKey key)
    {
        if (key instanceof AccordKey.TokenKey)
            return (AccordKey.TokenKey) key;
        return new AccordKey.TokenKey(key.tableId(),
                                      key.partitionKey().getToken().maxKeyBound());
    }

    @Override
    public TokenRange subRange(AccordKey start, AccordKey end)
    {
        return new TokenRange(toAccordToken(start), toAccordToken(end));
    }

    @Override
    public KeyRanges split(int count)
    {
        KeyRange[] ranges = new KeyRange[count];
        TableId tableId = start().tableId();
        Token left = start().partitionKey().getToken();
        Token right = end().partitionKey().getToken();
        while (count > 1)
        {
            double ratio = 1.0f / (count - 1);
            Token midpoint = DatabaseDescriptor.getPartitioner().split(left, right, ratio);
            if (midpoint.equals(left) || midpoint.equals(right))
                return new KeyRanges(new KeyRange[]{this});
            ranges[ranges.length - count] = new TokenRange(tableId, left, midpoint);
            left = midpoint;
            count--;
        }

        ranges[ranges.length - 1] = new TokenRange(tableId, left, right);

        return new KeyRanges(ranges);
    }
}
