/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import com.google.common.base.Objects;

import org.apache.cassandra.utils.ByteBufferUtil;

public class IndexExpression
{
    public final ByteBuffer column;
    public final Operator operator;
    public final ByteBuffer value;

    public IndexExpression(ByteBuffer column, Operator operator, ByteBuffer value)
    {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }

    public enum Operator
    {
        EQ, GTE, GT, LTE, LT;

        public static Operator findByOrdinal(int ordinal)
        {
            switch (ordinal) {
                case 0:
                    return EQ;
                case 1:
                    return GTE;
                case 2:
                    return GT;
                case 3:
                    return LTE;
                case 4:
                    return LT;
                default:
                    throw new AssertionError();
            }
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s %s %s", ByteBufferUtil.bytesToHex(column), operator, ByteBufferUtil.bytesToHex(value));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof IndexExpression))
            return false;

        IndexExpression ie = (IndexExpression) o;

        return Objects.equal(this.column, ie.column)
            && Objects.equal(this.operator, ie.operator)
            && Objects.equal(this.value, ie.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(column, operator, value);
    }
}
