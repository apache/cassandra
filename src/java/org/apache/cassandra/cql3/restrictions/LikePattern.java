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

package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public final class LikePattern
{
    public enum Kind
    {
        PREFIX(Operator.LIKE_PREFIX),
        SUFFIX(Operator.LIKE_SUFFIX),
        CONTAINS(Operator.LIKE_CONTAINS),
        MATCHES(Operator.LIKE_MATCHES);

        private final Operator operator;

        Kind(Operator operator)
        {
            this.operator = operator;
        }

        public Operator operator()
        {
            return operator;
        }
    }

    private static final ByteBuffer WILDCARD = ByteBufferUtil.bytes("%");

    private final Kind kind;

    private final ByteBuffer value;

    private LikePattern(Kind kind, ByteBuffer value)
    {
        this.kind = kind;
        this.value = value;
    }

    public Kind kind()
    {
        return kind;
    }

    public ByteBuffer value()
    {
        return value;
    }

    /**
     * As the specific subtype of LIKE (LIKE_PREFIX, LIKE_SUFFIX, LIKE_CONTAINS, LIKE_MATCHES) can only be
     * determined by examining the value, which in turn can only be known after binding, all LIKE restrictions
     * are initially created with the generic LIKE operator. This function takes the bound value, trims the
     * wildcard '%' chars from it and returns a tuple of the inferred operator subtype and the final value
     * @param value the bound value for the LIKE operation
     * @return  Pair containing the inferred LIKE subtype and the value with wildcards removed
     */
    public static LikePattern parse(ByteBuffer value)
    {
        Kind kind;
        int beginIndex = value.position();
        int endIndex = value.limit() - 1;
        if (ByteBufferUtil.endsWith(value, WILDCARD))
        {
            if (ByteBufferUtil.startsWith(value, WILDCARD))
            {
                kind = Kind.CONTAINS;
                beginIndex =+ 1;
            }
            else
            {
                kind = Kind.PREFIX;
            }
        }
        else if (ByteBufferUtil.startsWith(value, WILDCARD))
        {
            kind = Kind.SUFFIX;
            beginIndex += 1;
            endIndex += 1;
        }
        else
        {
            kind = Kind.MATCHES;
            endIndex += 1;
        }

        if (endIndex == 0 || beginIndex == endIndex)
            throw invalidRequest("LIKE value can't be empty.");

        ByteBuffer newValue = value.duplicate();
        newValue.position(beginIndex);
        newValue.limit(endIndex);
        return new LikePattern(kind, newValue);
    }
}
