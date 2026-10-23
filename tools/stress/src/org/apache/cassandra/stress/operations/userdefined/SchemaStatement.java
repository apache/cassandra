package org.apache.cassandra.stress.operations.userdefined;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.TypeCodec;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.stress.generate.Row;
import org.apache.cassandra.stress.operations.PartitionOperation;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;

public abstract class SchemaStatement extends PartitionOperation
{
    public enum ArgSelect
    {
        MULTIROW, SAMEROW;
        //TODO: FIRSTROW, LASTROW
    }

    final PreparedStatement statement;
    final ConsistencyLevel cl;
    final int[] argumentIndex;
    final Object[] bindBuffer;
    final Binder binder;

    public SchemaStatement(Timer timer, StressSettings settings, DataSpec spec,
                           PreparedStatement statement, List<String> bindNames, ConsistencyLevel cl)
    {
        super(timer, settings, spec);
        this.statement = statement;
        this.cl = cl;
        argumentIndex = new int[bindNames.size()];
        bindBuffer = new Object[argumentIndex.length];
        binder = statement != null ? new Binder(statement) : null;
        int i = 0;
        for (String name : bindNames)
            argumentIndex[i++] = spec.partitionGenerator.indexOf(name);

        if (statement != null)
        {
            if (cl.isSerialConsistency())
                statement.setSerialConsistencyLevel(JavaDriverClient.from(cl));
            else
                statement.setConsistencyLevel(JavaDriverClient.from(cl));
        }
    }

    BoundStatement bindRow(Row row)
    {
        assert statement != null;

        for (int i = 0 ; i < argumentIndex.length ; i++)
        {
            bindBuffer[i] = row.get(argumentIndex[i]);
            if (bindBuffer[i] == null && !spec.partitionGenerator.permitNulls(argumentIndex[i]))
                throw new IllegalStateException();
        }
        return binder.bind(bindBuffer);
    }

    List<ByteBuffer> rowArgs(Row row)
    {
        List<ByteBuffer> args = new ArrayList<>();
        for (int i : argumentIndex)
            args.add(spec.partitionGenerator.convert(i,
                        row.get(i)));
        return args;
    }

    /**
     * Binds the values of a request to a prepared statement, using the codecs of its bind variables, which are
     * resolved once, when the operation is created.
     *
     * Handing the values over to {@link PreparedStatement#bind(Object...)} instead makes the driver search its
     * codec registry for every value of every request: {@code CodecRegistry.codecFor(DataType, Object)} is a
     * linear scan over all the codecs it knows about and, unlike its type only counterparts, its result is not
     * cached.
     */
    static final class Binder
    {
        private final PreparedStatement statement;
        private final TypeCodec<Object>[] codecs;
        private final boolean[] isDate;

        @SuppressWarnings("unchecked")
        Binder(PreparedStatement statement)
        {
            ColumnDefinitions variables = statement.getVariables();
            CodecRegistry codecRegistry = statement.getCodecRegistry();
            this.statement = statement;
            this.codecs = new TypeCodec[variables.size()];
            this.isDate = new boolean[variables.size()];
            for (int i = 0 ; i < codecs.length ; i++)
            {
                DataType type = variables.getType(i);
                codecs[i] = codecRegistry.codecFor(type);
                isDate[i] = type.getName() == DataType.Name.DATE;
            }
        }

        BoundStatement bind(Object[] values)
        {
            BoundStatement bound = statement.bind();
            for (int i = 0 ; i < values.length ; i++)
            {
                Object value = values[i];
                if (value == null)
                {
                    bound.setBytesUnsafe(i, null);
                    continue;
                }
                if (isDate[i] && value instanceof Integer)
                {
                    // the java driver only accepts com.datastax.driver.core.LocalDate for CQL type "DATE"
                    value = LocalDate.fromDaysSinceEpoch((Integer) value);
                }
                bound.set(i, value, codecs[i]);
            }
            return bound;
        }
    }

    abstract class Runner implements RunOp
    {
        int partitionCount;
        int rowCount;

        @Override
        public int partitionCount()
        {
            return partitionCount;
        }

        @Override
        public int rowCount()
        {
            return rowCount;
        }
    }

}
