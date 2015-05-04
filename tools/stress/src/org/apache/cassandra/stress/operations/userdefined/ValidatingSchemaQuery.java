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
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.datastax.driver.core.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.generate.*;
import org.apache.cassandra.stress.generate.Row;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.utils.Pair;
import org.apache.thrift.TException;

public class ValidatingSchemaQuery extends Operation
{
    final Random random = new Random();
    private Pair<Row, Row> bounds;

    final int clusteringComponents;
    final ValidatingStatement[] statements;
    final ConsistencyLevel cl;
    final int[] argumentIndex;
    final Object[] bindBuffer;

    @Override
    public void run(SimpleClient client) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    private ValidatingSchemaQuery(Timer timer, StressSettings settings, PartitionGenerator generator, SeedManager seedManager, ValidatingStatement[] statements, ConsistencyLevel cl, int clusteringComponents)
    {
        super(timer, settings, new DataSpec(generator, seedManager, new DistributionFixed(1), 1));
        this.statements = statements;
        this.cl = cl;
        argumentIndex = new int[statements[0].statement.getVariables().size()];
        bindBuffer = new Object[argumentIndex.length];
        int i = 0;
        for (ColumnDefinitions.Definition definition : statements[0].statement.getVariables())
            argumentIndex[i++] = spec.partitionGenerator.indexOf(definition.getName());

        for (ValidatingStatement statement : statements)
            statement.statement.setConsistencyLevel(JavaDriverClient.from(cl));
        this.clusteringComponents = clusteringComponents;
    }

    protected boolean reset(Seed seed, PartitionIterator iterator)
    {
        bounds = iterator.resetToBounds(seed, clusteringComponents);
        return true;
    }

    abstract class Runner implements RunOp
    {
        int partitionCount;
        int rowCount;
        final PartitionIterator iter;
        final int statementIndex;

        protected Runner(PartitionIterator iter)
        {
            this.iter = iter;
            statementIndex = ThreadLocalRandom.current().nextInt(statements.length);
        }

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

    private class JavaDriverRun extends Runner
    {
        final JavaDriverClient client;

        private JavaDriverRun(JavaDriverClient client, PartitionIterator iter)
        {
            super(iter);
            this.client = client;
        }

        public boolean run() throws Exception
        {
            ResultSet rs = client.getSession().execute(bind(statementIndex));
            int[] valueIndex = new int[rs.getColumnDefinitions().size()];
            {
                int i = 0;
                for (ColumnDefinitions.Definition definition : rs.getColumnDefinitions())
                    valueIndex[i++] = spec.partitionGenerator.indexOf(definition.getName());
            }

            List<Object[]> prev1 = new ArrayList<>();
            List<Object[]> prev2 = new ArrayList<>();
            rowCount = 0;
            Iterator<com.datastax.driver.core.Row> results = rs.iterator();
            if (!statements[statementIndex].inclusiveStart && iter.hasNext())
                iter.next();
            while (iter.hasNext())
            {
                Row expectedRow = iter.next();
                if (!statements[statementIndex].inclusiveEnd && !iter.hasNext())
                    break;

                if (!results.hasNext())
                    return false;

                rowCount++;
                com.datastax.driver.core.Row actualRow = results.next();
                Object[] vs1 = new Object[actualRow.getColumnDefinitions().size()];
                Object[] vs2 = vs1.clone();
                for (int i = 0 ; i < actualRow.getColumnDefinitions().size() ; i++)
                {
                    Object expectedValue = expectedRow.get(valueIndex[i]);
                    Object actualValue = spec.partitionGenerator.convert(valueIndex[i], actualRow.getBytesUnsafe(i));
                    vs1[i] = expectedValue;
                    vs2[i] = actualValue;
                    if (!expectedValue.equals(actualValue))
                        return false;
                }
                prev1.add(vs1);
                prev2.add(vs2);
            }
            partitionCount = Math.min(1, rowCount);
            if (!rs.isExhausted())
                return false;
            return true;
        }
    }

    private class ThriftRun extends Runner
    {
        final ThriftClient client;

        private ThriftRun(ThriftClient client, PartitionIterator iter)
        {
            super(iter);
            this.client = client;
        }

        public boolean run() throws Exception
        {
            CqlResult rs = client.execute_prepared_cql3_query(statements[statementIndex].thriftId, partitions.get(0).getToken(), thriftArgs(), ThriftConversion.toThrift(cl));
            int[] valueIndex = new int[rs.getSchema().name_types.size()];
                for (int i = 0 ; i < valueIndex.length ; i++)
                    valueIndex[i] = spec.partitionGenerator.indexOf(rs.fieldForId(i).getFieldName());
            int r = 0;
            if (!statements[statementIndex].inclusiveStart && iter.hasNext())
                iter.next();
            while (iter.hasNext())
            {
                Row expectedRow = iter.next();
                if (!statements[statementIndex].inclusiveEnd && !iter.hasNext())
                    break;

                if (r == rs.num)
                    return false;

                rowCount++;
                CqlRow actualRow = rs.getRows().get(r++);
                for (int i = 0 ; i < actualRow.getColumnsSize() ; i++)
                {
                    ByteBuffer expectedValue = spec.partitionGenerator.convert(valueIndex[i], expectedRow.get(valueIndex[i]));
                    ByteBuffer actualValue = actualRow.getColumns().get(i).value;
                    if (!expectedValue.equals(actualValue))
                        return false;
                }
            }
            assert r == rs.num;
            partitionCount = Math.min(1, rowCount);
            return true;
        }
    }

    BoundStatement bind(int statementIndex)
    {
        int pkc = bounds.left.partitionKey.length;
        System.arraycopy(bounds.left.partitionKey, 0, bindBuffer, 0, pkc);
        int ccc = bounds.left.row.length;
        System.arraycopy(bounds.left.row, 0, bindBuffer, pkc, ccc);
        System.arraycopy(bounds.right.row, 0, bindBuffer, pkc + ccc, ccc);
        return statements[statementIndex].statement.bind(bindBuffer);
    }

    List<ByteBuffer> thriftArgs()
    {
        List<ByteBuffer> args = new ArrayList<>();
        int pkc = bounds.left.partitionKey.length;
        for (int i = 0 ; i < pkc ; i++)
            args.add(spec.partitionGenerator.convert(-i, bounds.left.partitionKey[i]));
        int ccc = bounds.left.row.length;
        for (int i = 0 ; i < ccc ; i++)
            args.add(spec.partitionGenerator.convert(i, bounds.left.get(i)));
        for (int i = 0 ; i < ccc ; i++)
            args.add(spec.partitionGenerator.convert(i, bounds.right.get(i)));
        return args;
    }

    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        timeWithRetry(new JavaDriverRun(client, partitions.get(0)));
    }

    @Override
    public void run(ThriftClient client) throws IOException
    {
        timeWithRetry(new ThriftRun(client, partitions.get(0)));
    }

    public static class Factory
    {
        final ValidatingStatement[] statements;
        final int clusteringComponents;

        public Factory(ValidatingStatement[] statements, int clusteringComponents)
        {
            this.statements = statements;
            this.clusteringComponents = clusteringComponents;
        }

        public ValidatingSchemaQuery create(Timer timer, StressSettings settings, PartitionGenerator generator, SeedManager seedManager, ConsistencyLevel cl)
        {
            return new ValidatingSchemaQuery(timer, settings, generator, seedManager, statements, cl, clusteringComponents);
        }
    }

    public static List<Factory> create(TableMetadata metadata, StressSettings settings)
    {
        List<Factory> factories = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        sb.append("SELECT * FROM ");
        sb.append(metadata.getName());
        sb.append(" WHERE");
        for (ColumnMetadata pk : metadata.getPartitionKey())
        {
            sb.append(first ? " " : " AND ");
            sb.append(pk.getName());
            sb.append(" = ?");
            first = false;
        }
        String base = sb.toString();

        factories.add(new Factory(new ValidatingStatement[] { prepare(settings, base, true, true) }, 0));

        int maxDepth = metadata.getClusteringColumns().size() - 1;
        for (int depth = 0 ; depth <= maxDepth  ; depth++)
        {
            StringBuilder cc = new StringBuilder();
            StringBuilder arg = new StringBuilder();
            cc.append("("); arg.append("(");
            for (int d = 0 ; d <= depth ; d++)
            {
                if (d > 0) { cc.append(","); arg.append(","); }
                cc.append(metadata.getClusteringColumns().get(d).getName());
                arg.append("?");
            }
            cc.append(")"); arg.append(")");

            ValidatingStatement[] statements = new ValidatingStatement[depth < maxDepth ? 1 : 4];
            int i = 0;
            for (boolean incLb : depth < maxDepth ? new boolean[] { true } : new boolean[] { true, false } )
            {
                for (boolean incUb : depth < maxDepth ? new boolean[] { false } : new boolean[] { true, false } )
                {
                    String lb = incLb ? ">=" : ">";
                    String ub = incUb ? "<=" : "<";
                    sb.setLength(0);
                    sb.append(base);
                    sb.append(" AND ");
                    sb.append(cc);
                    sb.append(lb);
                    sb.append(arg);
                    sb.append(" AND ");
                    sb.append(cc);
                    sb.append(ub);
                    sb.append(arg);
                    statements[i++] = prepare(settings, sb.toString(), incLb, incUb);
                }
            }
            factories.add(new Factory(statements, depth + 1));
        }

        return factories;
    }

    private static class ValidatingStatement
    {
        final PreparedStatement statement;
        final Integer thriftId;
        final boolean inclusiveStart;
        final boolean inclusiveEnd;
        private ValidatingStatement(PreparedStatement statement, Integer thriftId, boolean inclusiveStart, boolean inclusiveEnd)
        {
            this.statement = statement;
            this.thriftId = thriftId;
            this.inclusiveStart = inclusiveStart;
            this.inclusiveEnd = inclusiveEnd;
        }
    }

    private static ValidatingStatement prepare(StressSettings settings, String cql, boolean incLb, boolean incUb)
    {
        JavaDriverClient jclient = settings.getJavaDriverClient();
        ThriftClient tclient = settings.getThriftClient();
        PreparedStatement statement = jclient.prepare(cql);
        try
        {
            Integer thriftId = tclient.prepare_cql3_query(cql, Compression.NONE);
            return new ValidatingStatement(statement, thriftId, incLb, incUb);
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
    }
}
