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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.generate.Row;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.settings.ValidationType;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.transport.SimpleClient;

public abstract class SchemaStatement extends Operation
{

    final PreparedStatement statement;
    final Integer thriftId;
    final ConsistencyLevel cl;
    final ValidationType validationType;
    final int[] argumentIndex;
    final Object[] bindBuffer;

    public SchemaStatement(Timer timer, StressSettings settings, DataSpec spec,
                           PreparedStatement statement, Integer thriftId, ConsistencyLevel cl, ValidationType validationType)
    {
        super(timer, settings, spec);
        this.statement = statement;
        this.thriftId = thriftId;
        this.cl = cl;
        this.validationType = validationType;
        argumentIndex = new int[statement.getVariables().size()];
        bindBuffer = new Object[argumentIndex.length];
        int i = 0;
        for (ColumnDefinitions.Definition definition : statement.getVariables())
            argumentIndex[i++] = spec.partitionGenerator.indexOf(definition.getName());

        statement.setConsistencyLevel(JavaDriverClient.from(cl));
    }

    BoundStatement bindRow(Row row)
    {
        for (int i = 0 ; i < argumentIndex.length ; i++)
        {
            bindBuffer[i] = row.get(argumentIndex[i]);
            if (bindBuffer[i] == null && !spec.partitionGenerator.permitNulls(argumentIndex[i]))
                throw new IllegalStateException();
        }
        return statement.bind(bindBuffer);
    }

    List<ByteBuffer> thriftRowArgs(Row row)
    {
        List<ByteBuffer> args = new ArrayList<>();
        for (int i : argumentIndex)
            args.add(spec.partitionGenerator.convert(i, row.get(i)));
        return args;
    }

    void validate(ResultSet rs)
    {
        switch (validationType)
        {
            case NOT_FAIL:
                return;
            case NON_ZERO:
                if (rs.all().size() == 0)
                    throw new IllegalStateException("Expected non-zero results");
                break;
            default:
                throw new IllegalStateException("Unsupported validation type");
        }
    }

    void validate(CqlResult rs)
    {
        switch (validationType)
        {
            case NOT_FAIL:
                return;
            case NON_ZERO:
                if (rs.getRowsSize() == 0)
                    throw new IllegalStateException("Expected non-zero results");
                break;
            default:
                throw new IllegalStateException("Unsupported validation type");
        }
    }

    @Override
    public void run(SimpleClient client) throws IOException
    {
        throw new UnsupportedOperationException();
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
