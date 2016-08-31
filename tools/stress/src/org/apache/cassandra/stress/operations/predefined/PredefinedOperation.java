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
package org.apache.cassandra.stress.operations.predefined;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.generate.*;
import org.apache.cassandra.stress.operations.PartitionOperation;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.CqlVersion;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

public abstract class PredefinedOperation extends PartitionOperation
{
    public static final byte[] EMPTY_BYTE_ARRAY = {};
    public final Command type;
    private final Distribution columnCount;
    private Object cqlCache;

    public PredefinedOperation(Command type, Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        super(timer, settings, spec(generator, seedManager, settings.insert.rowPopulationRatio.get()));
        this.type = type;
        this.columnCount = settings.columns.countDistribution.get();
    }

    private static DataSpec spec(PartitionGenerator generator, SeedManager seedManager, RatioDistribution rowPopulationCount)
    {
        return new DataSpec(generator, seedManager, new DistributionFixed(1), rowPopulationCount, 1);
    }

    public boolean isCql3()
    {
        return settings.mode.cqlVersion == CqlVersion.CQL3;
    }
    public Object getCqlCache()
    {
        return cqlCache;
    }
    public void storeCqlCache(Object val)
    {
        cqlCache = val;
    }

    protected ByteBuffer getKey()
    {
        return (ByteBuffer) partitions.get(0).getPartitionKey(0);
    }

    final class ColumnSelection
    {
        final int[] indices;
        final int lb, ub;
        private ColumnSelection(int[] indices, int lb, int ub)
        {
            this.indices = indices;
            this.lb = lb;
            this.ub = ub;
        }

        public <V> List<V> select(List<V> in)
        {
            List<V> out = new ArrayList<>();
            if (indices != null)
            {
                for (int i : indices)
                    out.add(in.get(i));
            }
            else
            {
                out.addAll(in.subList(lb, ub));
            }
            return out;
        }

        int count()
        {
            return indices != null ? indices.length : ub - lb;
        }

        SlicePredicate predicate()
        {
            final SlicePredicate predicate = new SlicePredicate();
            if (indices == null)
            {
                predicate.setSlice_range(new SliceRange()
                                         .setStart(settings.columns.names.get(lb))
                                         .setFinish(EMPTY_BYTE_ARRAY)
                                         .setReversed(false)
                                         .setCount(count())
                );
            }
            else
                predicate.setColumn_names(select(settings.columns.names));
            return predicate;

        }
    }

    public String toString()
    {
        return type.toString();
    }

    ColumnSelection select()
    {
        if (settings.columns.slice)
        {
            int count = (int) columnCount.next();
            int start;
            if (count == settings.columns.maxColumnsPerKey)
                start = 0;
            else
                start = 1 + ThreadLocalRandom.current().nextInt(settings.columns.maxColumnsPerKey - count);
            return new ColumnSelection(null, start, start + count);
        }

        int count = (int) columnCount.next();
        int totalCount = settings.columns.names.size();
        if (count == settings.columns.names.size())
            return new ColumnSelection(null, 0, count);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int[] indices = new int[count];
        int c = 0, o = 0;
        while (c < count && count + o < totalCount)
        {
            int leeway = totalCount - (count + o);
            int spreadover = count - c;
            o += Math.round(rnd.nextDouble() * (leeway / (double) spreadover));
            indices[c] = o + c;
            c++;
        }
        while (c < count)
        {
            indices[c] = o + c;
            c++;
        }
        return new ColumnSelection(indices, 0, 0);
    }

    protected List<ByteBuffer> getColumnValues()
    {
        return getColumnValues(new ColumnSelection(null, 0, settings.columns.names.size()));
    }

    protected List<ByteBuffer> getColumnValues(ColumnSelection columns)
    {
        Row row = partitions.get(0).next();
        ByteBuffer[] r = new ByteBuffer[columns.count()];
        int c = 0;
        if (columns.indices != null)
            for (int i : columns.indices)
                r[c++] = (ByteBuffer) row.get(i);
        else
            for (int i = columns.lb ; i < columns.ub ; i++)
                r[c++] = (ByteBuffer) row.get(i);
        return Arrays.asList(r);
    }

    public static Operation operation(Command type, Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings, DistributionFactory counteradd)
    {
        switch (type)
        {
            case READ:
                switch(settings.mode.style)
                {
                    case THRIFT:
                        return new ThriftReader(timer, generator, seedManager, settings);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlReader(timer, generator, seedManager, settings);
                    default:
                        throw new UnsupportedOperationException();
                }


            case COUNTER_READ:
                switch(settings.mode.style)
                {
                    case THRIFT:
                        return new ThriftCounterGetter(timer, generator, seedManager, settings);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlCounterGetter(timer, generator, seedManager, settings);
                    default:
                        throw new UnsupportedOperationException();
                }

            case WRITE:

                switch(settings.mode.style)
                {
                    case THRIFT:
                        return new ThriftInserter(timer, generator, seedManager, settings);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlInserter(timer, generator, seedManager, settings);
                    default:
                        throw new UnsupportedOperationException();
                }

            case COUNTER_WRITE:
                switch(settings.mode.style)
                {
                    case THRIFT:
                        return new ThriftCounterAdder(counteradd, timer, generator, seedManager, settings);
                    case CQL:
                    case CQL_PREPARED:
                        return new CqlCounterAdder(counteradd, timer, generator, seedManager, settings);
                    default:
                        throw new UnsupportedOperationException();
                }

        }

        throw new UnsupportedOperationException();
    }

}
