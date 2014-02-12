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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;

import org.apache.cassandra.stress.generatedata.KeyGen;
import org.apache.cassandra.stress.generatedata.RowGen;
import org.apache.cassandra.stress.operations.CqlCounterAdder;
import org.apache.cassandra.stress.operations.CqlCounterGetter;
import org.apache.cassandra.stress.operations.CqlIndexedRangeSlicer;
import org.apache.cassandra.stress.operations.CqlInserter;
import org.apache.cassandra.stress.operations.CqlMultiGetter;
import org.apache.cassandra.stress.operations.CqlRangeSlicer;
import org.apache.cassandra.stress.operations.CqlReader;
import org.apache.cassandra.stress.operations.ThriftCounterAdder;
import org.apache.cassandra.stress.operations.ThriftCounterGetter;
import org.apache.cassandra.stress.operations.ThriftIndexedRangeSlicer;
import org.apache.cassandra.stress.operations.ThriftInserter;
import org.apache.cassandra.stress.operations.ThriftMultiGetter;
import org.apache.cassandra.stress.operations.ThriftRangeSlicer;
import org.apache.cassandra.stress.operations.ThriftReader;
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.CqlVersion;
import org.apache.cassandra.stress.settings.SettingsCommandMixed;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.InvalidRequestException;
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
            this.settings = settings;
            this.keyGen = settings.keys.newKeyGen();
            this.rowGen = settings.columns.newRowGen();
            this.metrics = metrics;
            if (!settings.columns.useSuperColumns)
                columnParents = Collections.singletonList(new ColumnParent(settings.schema.columnFamily));
            else
            {
                ColumnParent[] cp = new ColumnParent[settings.columns.superColumns];
                for (int i = 0 ; i < cp.length ; i++)
                    cp[i] = new ColumnParent("Super1").setSuper_column(ByteBufferUtil.bytes("S" + i));
                columnParents = Arrays.asList(cp);
            }
        }

        private State(Command type, State copy)
        {
            this.type = type;
            this.timer = copy.timer;
            this.rowGen = copy.rowGen;
            this.keyGen = copy.keyGen;
            this.columnParents = copy.columnParents;
            this.metrics = copy.metrics;
            this.settings = copy.settings;
            this.substates = null;
            this.commandSelector = null;
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
                System.err.println(e);
                exceptionMessage = getExceptionMessage(e);
            }
        }

        state.timer.stop(run.keyCount());

        if (!success)
        {
            error(String.format("Operation [%d] x%d key %s %s%n",
                    index,
                    tries,
                    run.key(),
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
        else
            System.err.println(message);
    }

    public static ByteBuffer getColumnNameBytes(int i)
    {
        return ByteBufferUtil.bytes("C" + i);
    }

    public static String getColumnName(int i)
    {
        return "C" + i;
    }

}
