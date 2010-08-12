/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.cassandra.hadoop.pig;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * A LoadFunc wrapping ColumnFamilyInputFormat.
 *
 * A row from a standard CF will be returned as nested tuples: (key, ((name1, val1), (name2, val2))).
 */
public class CassandraStorage extends LoadFunc
{
    // system environment variables that can be set to configure connection info:
    // alternatively, Hadoop JobConf variables can be set using keys from ConfigHelper
    public final static String PIG_RPC_PORT = "PIG_RPC_PORT";
    public final static String PIG_INITIAL_ADDRESS = "PIG_INITIAL_ADDRESS";
    public final static String PIG_PARTITIONER = "PIG_PARTITIONER";

    private final static byte[] BOUND = new byte[0];
    private final static int LIMIT = 1024;

    private Configuration conf;
    private RecordReader reader;

    @Override
    public Tuple getNext() throws IOException
    {
        try
        {
            // load the next pair
            if (!reader.nextKeyValue())
                return null;
            byte[] key = (byte[])reader.getCurrentKey();
            SortedMap<byte[],IColumn> cf = (SortedMap<byte[],IColumn>)reader.getCurrentValue();
            assert key != null && cf != null;
            
            // and wrap it in a tuple
		    Tuple tuple = TupleFactory.getInstance().newTuple(2);
            ArrayList<Tuple> columns = new ArrayList<Tuple>();
            tuple.set(0, new DataByteArray(key));
            for (Map.Entry<byte[], IColumn> entry : cf.entrySet())
                columns.add(columnToTuple(entry.getKey(), entry.getValue()));
            tuple.set(1, new DefaultDataBag(columns));
            return tuple;
        }
        catch (InterruptedException e)
        {
            throw new IOException(e.getMessage());
        }
    }

    private Tuple columnToTuple(byte[] name, IColumn col) throws IOException
    {
        Tuple pair = TupleFactory.getInstance().newTuple(2);
        pair.set(0, new DataByteArray(name));
        if (col instanceof Column)
        {
            // standard
            pair.set(1, new DataByteArray(col.value()));
            return pair;
        }

        // super
        ArrayList<Tuple> subcols = new ArrayList<Tuple>();
        for (IColumn subcol : ((SuperColumn)col).getSubColumns())
            subcols.add(columnToTuple(subcol.name(), subcol));
        pair.set(1, new DefaultDataBag(subcols));
        return pair;
    }

    @Override
    public InputFormat getInputFormat()
    {
        ColumnFamilyInputFormat inputFormat = new ColumnFamilyInputFormat();
        return inputFormat;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
    {
        this.reader = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException
    {
        // parse uri into keyspace and columnfamily
        String ksname, cfname;
        try
        {
            if (!location.startsWith("cassandra://"))
                throw new Exception("Bad scheme.");
            String[] parts = location.split("/+");
            ksname = parts[1];
            cfname = parts[2];
        }
        catch (Exception e)
        {
            throw new IOException("Expected 'cassandra://<keyspace>/<columnfamily>': " + e.getMessage());
        }

        // and configure
        SliceRange range = new SliceRange(BOUND, BOUND, false, LIMIT);
        SlicePredicate predicate = new SlicePredicate().setSlice_range(range);
        conf = job.getConfiguration();
        ConfigHelper.setInputSlicePredicate(conf, predicate);
        ConfigHelper.setInputColumnFamily(conf, ksname, cfname);

        // check the environment for connection information
        if (System.getenv(PIG_RPC_PORT) != null)
            ConfigHelper.setRpcPort(conf, System.getenv(PIG_RPC_PORT));
        if (System.getenv(PIG_INITIAL_ADDRESS) != null)
            ConfigHelper.setInitialAddress(conf, System.getenv(PIG_INITIAL_ADDRESS));
        if (System.getenv(PIG_PARTITIONER) != null)
            ConfigHelper.setPartitioner(conf, System.getenv(PIG_PARTITIONER));
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException
    {
        return location;
    }
}
