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
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
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
    final ColumnDefinitions definitions;

    public SchemaStatement(Timer timer, StressSettings settings, DataSpec spec,
                           PreparedStatement statement, List<String> bindNames, ConsistencyLevel cl)
    {
        super(timer, settings, spec);
        this.statement = statement;
        this.cl = cl;
        argumentIndex = new int[bindNames.size()];
        bindBuffer = new Object[argumentIndex.length];
        definitions = statement != null ? statement.getVariables() : null;
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
            Object value = row.get(argumentIndex[i]);
            if (definitions.getType(i).getName().equals(DataType.date().getName()))
            {
                // the java driver only accepts com.datastax.driver.core.LocalDate for CQL type "DATE"
                value= LocalDate.fromDaysSinceEpoch((Integer) value);
            }
            bindBuffer[i] = value;
            if (bindBuffer[i] == null && !spec.partitionGenerator.permitNulls(argumentIndex[i]))
                throw new IllegalStateException();
        }
        return statement.bind(bindBuffer);
    }

    List<ByteBuffer> rowArgs(Row row)
    {
        List<ByteBuffer> args = new ArrayList<>();
        for (int i : argumentIndex)
            args.add(spec.partitionGenerator.convert(i,
                        row.get(i)));
        return args;
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
