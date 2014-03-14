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
package org.apache.cassandra.stress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.stress.generatedata.Distribution;
import org.apache.cassandra.stress.generatedata.KeyGen;
import org.apache.cassandra.stress.generatedata.RowGen;
import org.apache.cassandra.stress.settings.*;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class Operation
{
    public final long index;
    protected final State state;

    public Operation(State state, long idx)
    {
        index = idx;
        this.state = state;
    }

    public static interface RunOp
    {
        public boolean run() throws Exception;
        public String key();
        public int keyCount();
    }

    // one per thread!
    public static final class State
    {

        public final StressSettings settings;
        public final Timer timer;
        public final Command type;
        public final KeyGen keyGen;
        public final RowGen rowGen;
        public final Distribution counteradd;
        public final List<ColumnParent> columnParents;
        public final StressMetrics metrics;
        public final SettingsCommandMixed.CommandSelector commandSelector;
        private final EnumMap<Command, State> substates;
        private Object cqlCache;

        public State(Command type, StressSettings settings, StressMetrics metrics)
        {
            this.type = type;
            this.timer = metrics.getTiming().newTimer();
            if (type == Command.MIXED)
            {
                commandSelector = ((SettingsCommandMixed) settings.command).selector();
                substates = new EnumMap<>(Command.class);
            }
            else
            {
                commandSelector = null;
                substates = null;
            }
            counteradd = settings.command.add.get();
            this.settings = settings;
            this.keyGen = settings.keys.newKeyGen();
            this.rowGen = settings.columns.newRowGen();
            this.metrics = metrics;
            this.columnParents = columnParents(type, settings);
        }

        private State(Command type, State copy)
        {
            this.type = type;
            this.timer = copy.timer;
            this.rowGen = copy.rowGen;
            this.keyGen = copy.keyGen;
            this.columnParents = columnParents(type, copy.settings);
            this.metrics = copy.metrics;
            this.settings = copy.settings;
            this.counteradd = copy.counteradd;
            this.substates = null;
            this.commandSelector = null;
        }

        private List<ColumnParent> columnParents(Command type, StressSettings settings)
        {
            if (!settings.columns.useSuperColumns)
                return Collections.singletonList(new ColumnParent(type.table));
            else
            {
                ColumnParent[] cp = new ColumnParent[settings.columns.superColumns];
                for (int i = 0 ; i < cp.length ; i++)
                    cp[i] = new ColumnParent(type.supertable).setSuper_column(ByteBufferUtil.bytes("S" + i));
                return Arrays.asList(cp);
            }
        }



        public boolean isCql3()
        {
            return settings.mode.cqlVersion == CqlVersion.CQL3;
        }
        public boolean isCql2()
        {
            return settings.mode.cqlVersion == CqlVersion.CQL2;
        }
        public Object getCqlCache()
        {
            return cqlCache;
        }
        public void storeCqlCache(Object val)
        {
            cqlCache = val;
        }

        public State substate(Command command)
        {
            assert type == Command.MIXED;
            State substate = substates.get(command);
            if (substate == null)
            {
                substates.put(command, substate = new State(command, this));
            }
            return substate;
        }

    }

    protected ByteBuffer getKey()
    {
        return state.keyGen.getKeys(1, index).get(0);
    }

    protected List<ByteBuffer> getKeys(int count)
    {
        return state.keyGen.getKeys(count, index);
    }

    protected List<ByteBuffer> generateColumnValues(ByteBuffer key)
    {
        return state.rowGen.generate(index, key);
    }

    private int sliceStart(int count)
    {
        if (count == state.settings.columns.maxColumnsPerKey)
            return 0;
        return 1 + ThreadLocalRandom.current().nextInt(state.settings.columns.maxColumnsPerKey - count);
    }

    protected SlicePredicate slicePredicate()
    {
        final SlicePredicate predicate = new SlicePredicate();
        if (state.settings.columns.slice)
        {
            int count = state.rowGen.count(index);
            int start = sliceStart(count);
            predicate.setSlice_range(new SliceRange()
                                     .setStart(state.settings.columns.names.get(start))
                                     .setFinish(new byte[] {})
                                     .setReversed(false)
                                     .setCount(count)
            );
        }
        else
            predicate.setColumn_names(randomNames());
        return predicate;
    }

    protected List<ByteBuffer> randomNames()
    {
        int count = state.rowGen.count(index);
        List<ByteBuffer> src = state.settings.columns.names;
        if (count == src.size())
            return src;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        List<ByteBuffer> r = new ArrayList<>();
        int c = 0, o = 0;
        while (c < count && count + o < src.size())
        {
            int leeway = src.size() - (count + o);
            int spreadover = count - c;
            o += Math.round(rnd.nextDouble() * (leeway / (double) spreadover));
            r.add(src.get(o + c++));
        }
        while (c < count)
            r.add(src.get(o + c++));
        return r;
    }

    /**
     * Run operation
     * @param client Cassandra Thrift client connection
     * @throws IOException on any I/O error.
     */
    public abstract void run(ThriftClient client) throws IOException;

    public void run(SimpleClient client) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void run(JavaDriverClient client) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void timeWithRetry(RunOp run) throws IOException
    {
        state.timer.start();

        boolean success = false;
        String exceptionMessage = null;

        int tries = 0;
        for (; tries < state.settings.command.tries; tries++)
        {
            try
            {
                success = run.run();
                break;
            }
            catch (Exception e)
            {
                switch (state.settings.log.level)
                {
                    case MINIMAL:
                        break;

                    case NORMAL:
                        System.err.println(e);
                        break;

                    case VERBOSE:
                        e.printStackTrace(System.err);
                        break;

                    default:
                        throw new AssertionError();
                }
                exceptionMessage = getExceptionMessage(e);
            }
        }

        state.timer.stop(run.keyCount());

        if (!success)
        {
            error(String.format("Operation [%d] x%d key %s (0x%s) %s%n",
                    index,
                    tries,
                    run.key(),
                    ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(run.key())),
                    (exceptionMessage == null)
                        ? "Data returned was not validated"
                        : "Error executing: " + exceptionMessage));
        }

    }

    protected String getExceptionMessage(Exception e)
    {
        String className = e.getClass().getSimpleName();
        String message = (e instanceof InvalidRequestException) ? ((InvalidRequestException) e).getWhy() : e.getMessage();
        return (message == null) ? "(" + className + ")" : String.format("(%s): %s", className, message);
    }

    protected void error(String message) throws IOException
    {
        if (!state.settings.command.ignoreErrors)
            throw new IOException(message);
        else if (state.settings.log.level.compareTo(SettingsLog.Level.MINIMAL) > 0)
            System.err.println(message);
    }

}
