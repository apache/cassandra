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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public final class IndexExpression
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

    /**
     * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code> operator.
     *
     * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code>
     * operator, <code>false</code> otherwise.
     */
    public boolean isContains()
    {
        return Operator.CONTAINS == operator;
    }

    /**
     * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code> operator.
     *
     * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code>
     * operator, <code>false</code> otherwise.
     */
    public boolean isContainsKey()
    {
        return Operator.CONTAINS_KEY == operator;
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

    /**
     * Write the serialized version of this <code>IndexExpression</code> to the specified output.
     *
     * @param output the output to write to
     * @throws IOException if an I/O problem occurs while writing to the specified output
     */
    public void writeTo(DataOutputPlus output) throws IOException
    {
        ByteBufferUtil.writeWithShortLength(column, output);
        operator.writeTo(output);
        ByteBufferUtil.writeWithShortLength(value, output);
    }

    /**
     * Deserializes an <code>IndexExpression</code> instance from the specified input. 
     *
     * @param input the input to read from 
     * @return the <code>IndexExpression</code> instance deserialized
     * @throws IOException if a problem occurs while deserializing the <code>IndexExpression</code> instance.
     */
    public static IndexExpression readFrom(DataInput input) throws IOException
    {
        return new IndexExpression(ByteBufferUtil.readWithShortLength(input),
                                   Operator.readFrom(input),
                                   ByteBufferUtil.readWithShortLength(input));
    }
}
