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
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.avro.Mutation;
import org.apache.cassandra.avro.Deletion;
import org.apache.cassandra.avro.ColumnOrSuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;

import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * A LoadFunc wrapping ColumnFamilyInputFormat.
 *
 * A row from a standard CF will be returned as nested tuples: (key, ((name1, val1), (name2, val2))).
 */
public class CassandraStorage extends LoadFunc implements StoreFuncInterface, LoadPushDown
{
    // system environment variables that can be set to configure connection info:
    // alternatively, Hadoop JobConf variables can be set using keys from ConfigHelper
    public final static String PIG_RPC_PORT = "PIG_RPC_PORT";
    public final static String PIG_INITIAL_ADDRESS = "PIG_INITIAL_ADDRESS";
    public final static String PIG_PARTITIONER = "PIG_PARTITIONER";

    private final static ByteBuffer BOUND = FBUtilities.EMPTY_BYTE_BUFFER;
    private final static int LIMIT = 1024;
    private static final Log logger = LogFactory.getLog(CassandraStorage.class);

    private Configuration conf;
    private RecordReader reader;
    private RecordWriter writer;

    @Override
    public Tuple getNext() throws IOException
    {
        try
        {
            // load the next pair
            if (!reader.nextKeyValue())
                return null;

            ByteBuffer key = (ByteBuffer)reader.getCurrentKey();
            SortedMap<ByteBuffer,IColumn> cf = (SortedMap<ByteBuffer,IColumn>)reader.getCurrentValue();
            assert key != null && cf != null;
            
            // and wrap it in a tuple
	    Tuple tuple = TupleFactory.getInstance().newTuple(2);
            ArrayList<Tuple> columns = new ArrayList<Tuple>();
            tuple.set(0, new DataByteArray(key.array(), key.position()+key.arrayOffset(), key.limit()+key.arrayOffset()));
            for (Map.Entry<ByteBuffer, IColumn> entry : cf.entrySet())
            {                    
                columns.add(columnToTuple(entry.getKey(), entry.getValue()));
            }
         
            tuple.set(1, new DefaultDataBag(columns));
            return tuple;
        }
        catch (InterruptedException e)
        {
            throw new IOException(e.getMessage());
        }
    }

    private Tuple columnToTuple(ByteBuffer name, IColumn col) throws IOException
    {
        Tuple pair = TupleFactory.getInstance().newTuple(2);
        pair.set(0, new DataByteArray(name.array(), name.position()+name.arrayOffset(), name.limit()+name.arrayOffset()));
        if (col instanceof Column)
        {
            // standard
            pair.set(1, new DataByteArray(col.value().array(), 
                                          col.value().position()+col.value().arrayOffset(),
                                          col.value().limit()+col.value().arrayOffset()));
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
        return new ColumnFamilyInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
    {
        this.reader = reader;
    }

    private String[] parseLocation(String location) throws IOException
    {
        // parse uri into keyspace and columnfamily
        String names[];
        try
        {
            if (!location.startsWith("cassandra://"))
                throw new Exception("Bad scheme.");
            String[] parts = location.split("/+");
            names = new String[]{ parts[1], parts[2] };
        }
        catch (Exception e)
        {
            throw new IOException("Expected 'cassandra://<keyspace>/<columnfamily>': " + e.getMessage());
        }
       return names;
    }

    private void setConnectionInformation() throws IOException
    {
        if (System.getenv(PIG_RPC_PORT) != null)
            ConfigHelper.setRpcPort(conf, System.getenv(PIG_RPC_PORT));
        else
            throw new IOException("PIG_RPC_PORT environment variable not set");
        if (System.getenv(PIG_INITIAL_ADDRESS) != null)
            ConfigHelper.setInitialAddress(conf, System.getenv(PIG_INITIAL_ADDRESS));
        else
            throw new IOException("PIG_INITIAL_ADDRESS environment variable not set");
        if (System.getenv(PIG_PARTITIONER) != null)
            ConfigHelper.setPartitioner(conf, System.getenv(PIG_PARTITIONER));
        else
            throw new IOException("PIG_PARTITIONER environment variable not set");
    }

    @Override
    public void setLocation(String location, Job job) throws IOException
    {
        SliceRange range = new SliceRange(BOUND, BOUND, false, LIMIT);
        SlicePredicate predicate = new SlicePredicate().setSlice_range(range);
        conf = job.getConfiguration();
        ConfigHelper.setInputSlicePredicate(conf, predicate);
        String[] names = parseLocation(location);
        ConfigHelper.setInputColumnFamily(conf, names[0], names[1]);
        setConnectionInformation();
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException
    {
        return location;
    }

    /* StoreFunc methods */
    public void setStoreFuncUDFContextSignature(String signature)
    {
    }

    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException
    {
        return relativeToAbsolutePath(location, curDir);
    }

    public void setStoreLocation(String location, Job job) throws IOException
    {
        conf = job.getConfiguration();
        String[] names = parseLocation(location);
        ConfigHelper.setOutputColumnFamily(conf, names[0], names[1]);
        setConnectionInformation();
    }

    public OutputFormat getOutputFormat()
    {
        return new ColumnFamilyOutputFormat();
    }

    public void checkSchema(ResourceSchema schema) throws IOException
    {
        // we don't care about types, they all get casted to ByteBuffers
    }

    public void prepareToWrite(RecordWriter writer)
    {
        this.writer = writer;
    }

    private ByteBuffer objToBB(Object o)
    {
        if (o == null)
            return (ByteBuffer)o;
        if (o instanceof java.lang.String)
            o = new DataByteArray((String)o);
        return ByteBuffer.wrap(((DataByteArray) o).get());
    }

    public void putNext(Tuple t) throws ExecException, IOException
    {
        ByteBuffer key = objToBB(t.get(0));
        DefaultDataBag pairs = (DefaultDataBag) t.get(1);
        ArrayList<Mutation> mutationList = new ArrayList<Mutation>();

        try
        {
            for (Tuple pair : pairs)
            {
               Mutation mutation = new Mutation();
               if (DataType.findType(pair.get(1)) == DataType.BAG) // supercolumn
               {
                   org.apache.cassandra.avro.SuperColumn sc = new org.apache.cassandra.avro.SuperColumn();
                   sc.name = objToBB(pair.get(0));
                   ArrayList<org.apache.cassandra.avro.Column> columns = new ArrayList<org.apache.cassandra.avro.Column>();
                   for (Tuple subcol : (DefaultDataBag) pair.get(1))
                   {
                       org.apache.cassandra.avro.Column column = new org.apache.cassandra.avro.Column();
                       column.name = objToBB(subcol.get(0));
                       column.value = objToBB(subcol.get(1));
                       column.timestamp = System.currentTimeMillis() * 1000;
                       columns.add(column);
                   }
                   if (columns.isEmpty()) // a deletion
                   {
                       mutation.deletion = new Deletion();
                       mutation.deletion.super_column = objToBB(pair.get(0));
                       mutation.deletion.timestamp = System.currentTimeMillis() * 1000;
                   }
                   else
                   {
                       sc.columns = columns;
                       mutation.column_or_supercolumn = new ColumnOrSuperColumn();
                       mutation.column_or_supercolumn.super_column = sc;
                   }
               }
               else // assume column since it could be anything else
               {
                   if (pair.get(1) == null)
                   {
                       mutation.deletion = new Deletion();
                       mutation.deletion.predicate = new org.apache.cassandra.avro.SlicePredicate();
                       mutation.deletion.predicate.column_names = Arrays.asList(objToBB(pair.get(0)));
                       mutation.deletion.timestamp = System.currentTimeMillis() * 1000;
                   }
                   else
                   {
                       org.apache.cassandra.avro.Column column = new org.apache.cassandra.avro.Column();
                       column.name = objToBB(pair.get(0));
                       column.value = objToBB(pair.get(1));
                       column.timestamp = System.currentTimeMillis() * 1000;
                       mutation.column_or_supercolumn = new ColumnOrSuperColumn();
                       mutation.column_or_supercolumn.column = column;
                       mutationList.add(mutation);
                   }
               }
               mutationList.add(mutation);
            }
        }
        catch (ClassCastException e)
        {
            throw new IOException(e + " Output must be (key, {(column,value)...}) for ColumnFamily or (key, {supercolumn:{(column,value)...}...}) for SuperColumnFamily");
        }
        try
        {
            writer.write(key, mutationList);
        }
        catch (InterruptedException e)
        {
           throw new IOException(e);
        }
    }

    public void cleanupOnFailure(String failure, Job job)
    {
    }

    /* LoadPushDown methods */

    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException
    {
        return new RequiredFieldResponse(true);
    }

}
