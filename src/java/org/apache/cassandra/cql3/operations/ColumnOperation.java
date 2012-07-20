/**
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
package org.apache.cassandra.cql3.operations;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class ColumnOperation implements Operation
{
    enum Kind { SET, COUNTER_INC, COUNTER_DEC }

    private final Term value;
    private final Kind kind;

    private ColumnOperation(Term value, Kind kind)
    {
        this.value = value;
        this.kind = kind;
    }

    public void execute(ColumnFamily cf,
                        ColumnNameBuilder builder,
                        AbstractType<?> validator,
                        UpdateParameters params) throws InvalidRequestException
    {
        switch (kind)
        {
            case SET:
                doSet(cf, builder, validator, params);
                break;
            case COUNTER_INC:
            case COUNTER_DEC:
                doCounter(cf, builder, params);
                break;
            default:
                throw new AssertionError("Unsupported operation: " + kind);
        }
    }

    public void execute(ColumnFamily cf, ColumnNameBuilder builder, CollectionType validator, UpdateParameters params, List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        throw new InvalidRequestException("Column operations are only supported on simple types, but " + validator + " given.");
    }

    private void doSet(ColumnFamily cf, ColumnNameBuilder builder, AbstractType<?> validator, UpdateParameters params) throws InvalidRequestException
    {
        ByteBuffer colName = builder.build();
        QueryProcessor.validateColumnName(colName);

        ByteBuffer valueBytes = value.getByteBuffer(validator, params.variables);
        cf.addColumn(params.makeColumn(colName, valueBytes));
    }

    private void doCounter(ColumnFamily cf, ColumnNameBuilder builder, UpdateParameters params) throws InvalidRequestException
    {
        long val;

        try
        {
            val = ByteBufferUtil.toLong(value.getByteBuffer(LongType.instance, params.variables));
        }
        catch (NumberFormatException e)
        {
            throw new InvalidRequestException(String.format("'%s' is an invalid value, should be a long.", value.getText()));
        }

        if (kind == Kind.COUNTER_DEC)
        {
            if (val == Long.MIN_VALUE)
                throw new InvalidRequestException("The negation of " + val + " overflows supported integer precision (signed 8 bytes integer)");
            else
                val = -val;
        }

        cf.addCounter(new QueryPath(cf.metadata().cfName, null, builder.build()), val);
    }

    public List<Term> getValues()
    {
        return Collections.singletonList(value);
    }

    public boolean requiresRead()
    {
        return false;
    }

    public Type getType()
    {
        return kind == Kind.COUNTER_DEC || kind == Kind.COUNTER_INC ? Type.COUNTER : Type.COLUMN;
    }

    /* Utility methods */

    public static Operation Set(Term value)
    {
        return new ColumnOperation(value, Kind.SET);
    }

    public static Operation CounterInc(Term value)
    {
        return new ColumnOperation(value, Kind.COUNTER_INC);
    }

    public static Operation CounterDec(Term value)
    {
        return new ColumnOperation(value, Kind.COUNTER_DEC);
    }
}
