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

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;

import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * A LoadStoreFunc for retrieving data from and storing data to Cassandra
 *
 * A row from a standard CF will be returned as nested tuples: (key, ((name1, val1), (name2, val2))).
 */
public class CassandraStorage extends LoadFunc implements StoreFuncInterface
{
    // system environment variables that can be set to configure connection info:
    // alternatively, Hadoop JobConf variables can be set using keys from ConfigHelper
    public final static String PIG_RPC_PORT = "PIG_RPC_PORT";
    public final static String PIG_INITIAL_ADDRESS = "PIG_INITIAL_ADDRESS";
    public final static String PIG_PARTITIONER = "PIG_PARTITIONER";

    private final static ByteBuffer BOUND = ByteBufferUtil.EMPTY_BYTE_BUFFER;
    private static final Log logger = LogFactory.getLog(CassandraStorage.class);

    private ByteBuffer slice_start = BOUND;
    private ByteBuffer slice_end = BOUND;
    private boolean slice_reverse = false;
    private String keyspace;
    private String column_family;
    private String loadSignature;
    private String storeSignature;

    private Configuration conf;
    private RecordReader reader;
    private RecordWriter writer;
    private int limit;

    public CassandraStorage()
    {
        this(1024);
    }

    /**
     * @param limit: number of columns to fetch in a slice
     */
    public CassandraStorage(int limit)
    {
        super();
        this.limit = limit;
    }

    public int getLimit() 
    {
        return limit;
    }

	@Override
    public Tuple getNext() throws IOException
    {
        try
        {
            // load the next pair
            if (!reader.nextKeyValue())
                return null;

            CfDef cfDef = getCfDef(loadSignature);
            ByteBuffer key = (ByteBuffer)reader.getCurrentKey();
            SortedMap<ByteBuffer,IColumn> cf = (SortedMap<ByteBuffer,IColumn>)reader.getCurrentValue();
            assert key != null && cf != null;
            
            // and wrap it in a tuple
	        Tuple tuple = TupleFactory.getInstance().newTuple(2);
            ArrayList<Tuple> columns = new ArrayList<Tuple>();
            tuple.set(0, new DataByteArray(key.array(), key.position()+key.arrayOffset(), key.limit()+key.arrayOffset()));
            for (Map.Entry<ByteBuffer, IColumn> entry : cf.entrySet())
            {
                columns.add(columnToTuple(entry.getKey(), entry.getValue(), cfDef));
            }

            tuple.set(1, new DefaultDataBag(columns));
            return tuple;
        }
        catch (InterruptedException e)
        {
            throw new IOException(e.getMessage());
        }
    }

    private Tuple columnToTuple(ByteBuffer name, IColumn col, CfDef cfDef) throws IOException
    {
        Tuple pair = TupleFactory.getInstance().newTuple(2);
        List<AbstractType> marshallers = getDefaultMarshallers(cfDef);
        Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);

        if (col instanceof Column)
        {
            // standard
            pair.set(0, marshallers.get(0).compose(name));
            if (validators.get(name) == null)
                // Have to special case BytesType because compose returns a ByteBuffer
                if (marshallers.get(1) instanceof BytesType)
                    pair.set(1, new DataByteArray(ByteBufferUtil.getArray(col.value())));
                else
                    pair.set(1, marshallers.get(1).compose(col.value()));
            else
                pair.set(1, validators.get(name).compose(col.value()));
            return pair;
        }

        // super
        ArrayList<Tuple> subcols = new ArrayList<Tuple>();
        for (IColumn subcol : col.getSubColumns())
            subcols.add(columnToTuple(subcol.name(), subcol, cfDef));
        
        pair.set(1, new DefaultDataBag(subcols));
        return pair;
    }

    private CfDef getCfDef(String signature)
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(CassandraStorage.class);
        return cfdefFromString(property.getProperty(signature));
    }

    private List<AbstractType> getDefaultMarshallers(CfDef cfDef) throws IOException
    {
        ArrayList<AbstractType> marshallers = new ArrayList<AbstractType>();
        AbstractType comparator = null;
        AbstractType default_validator = null;
        try
        {
            comparator = TypeParser.parse(cfDef.comparator_type);
            default_validator = TypeParser.parse(cfDef.default_validation_class);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }

        marshallers.add(comparator);
        marshallers.add(default_validator);
        return marshallers;
    }

    private Map<ByteBuffer,AbstractType> getValidatorMap(CfDef cfDef) throws  IOException
    {
        Map<ByteBuffer, AbstractType> validators = new HashMap<ByteBuffer, AbstractType>();
        for (ColumnDef cd : cfDef.column_metadata)
        {
            if (cd.getValidation_class() != null && !cd.getValidation_class().isEmpty())
            {
                AbstractType validator = null;
                try
                {
                    validator = TypeParser.parse(cd.getValidation_class());
                    validators.put(cd.name, validator);
                }
                catch (ConfigurationException e)
                {
                    throw new IOException(e);
                }
            }
        }
        return validators;
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

    private void setLocationFromUri(String location) throws IOException
    {
        // parse uri into keyspace and columnfamily
        String names[];
        try
        {
            if (!location.startsWith("cassandra://"))
                throw new Exception("Bad scheme.");
            String[] urlParts = location.split("\\?");
            if (urlParts.length > 1)
            {
                for (String param : urlParts[1].split("&"))
                {
                    String[] pair = param.split("=");
                    if (pair[0].equals("slice_start"))
                        slice_start = ByteBufferUtil.bytes(pair[1]);
                    else if (pair[0].equals("slice_end"))
                        slice_end = ByteBufferUtil.bytes(pair[1]);
                    else if (pair[0].equals("reversed"))
                        slice_reverse = Boolean.parseBoolean(pair[1]);
                    else if (pair[0].equals("limit"))
                        limit = Integer.parseInt(pair[1]);
                }
            }
            String[] parts = urlParts[0].split("/+");
            keyspace = parts[1];
            column_family = parts[2];
        }
        catch (Exception e)
        {
            throw new IOException("Expected 'cassandra://<keyspace>/<columnfamily>[?slice_start=<start>&slice_end=<end>[&reversed=true][&limit=1]]': " + e.getMessage());
        }
    }

    private void setConnectionInformation() throws IOException
    {
        if (System.getenv(PIG_RPC_PORT) != null)
            ConfigHelper.setRpcPort(conf, System.getenv(PIG_RPC_PORT));
        else if (ConfigHelper.getRpcPort(conf) == 0) 
            throw new IOException("PIG_RPC_PORT environment variable not set");
        if (System.getenv(PIG_INITIAL_ADDRESS) != null)
            ConfigHelper.setInitialAddress(conf, System.getenv(PIG_INITIAL_ADDRESS));
        else if (ConfigHelper.getInitialAddress(conf) == null) 
            throw new IOException("PIG_INITIAL_ADDRESS environment variable not set");
        if (System.getenv(PIG_PARTITIONER) != null)
            ConfigHelper.setPartitioner(conf, System.getenv(PIG_PARTITIONER));
        else if (ConfigHelper.getPartitioner(conf) == null) 
            throw new IOException("PIG_PARTITIONER environment variable not set");
    }

    @Override
    public void setLocation(String location, Job job) throws IOException
    {
        conf = job.getConfiguration();
        setLocationFromUri(location);
        if (ConfigHelper.getRawInputSlicePredicate(conf) == null)
        {
            SliceRange range = new SliceRange(slice_start, slice_end, slice_reverse, limit);
            SlicePredicate predicate = new SlicePredicate().setSlice_range(range);
            ConfigHelper.setInputSlicePredicate(conf, predicate);
        }
        ConfigHelper.setInputColumnFamily(conf, keyspace, column_family);
        setConnectionInformation();
        initSchema(loadSignature);
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException
    {
        return location;
    }

    @Override
    public void setUDFContextSignature(String signature)
    {
        this.loadSignature = signature;
    }

    /* StoreFunc methods */
    public void setStoreFuncUDFContextSignature(String signature)
    {
        this.storeSignature = signature;
    }

    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException
    {
        return relativeToAbsolutePath(location, curDir);
    }

    public void setStoreLocation(String location, Job job) throws IOException
    {
        conf = job.getConfiguration();
        setLocationFromUri(location);
        ConfigHelper.setOutputColumnFamily(conf, keyspace, column_family);
        setConnectionInformation();
        initSchema(storeSignature);
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
        CfDef cfDef = getCfDef(storeSignature);
        List<AbstractType> marshallers = getDefaultMarshallers(cfDef);
        Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);
        try
        {
            for (Tuple pair : pairs)
            {
               Mutation mutation = new Mutation();
               if (DataType.findType(pair.get(1)) == DataType.BAG) // supercolumn
               {
                   org.apache.cassandra.thrift.SuperColumn sc = new org.apache.cassandra.thrift.SuperColumn();
                   sc.name = objToBB(pair.get(0));
                   ArrayList<org.apache.cassandra.thrift.Column> columns = new ArrayList<org.apache.cassandra.thrift.Column>();
                   for (Tuple subcol : (DefaultDataBag) pair.get(1))
                   {
                       org.apache.cassandra.thrift.Column column = new org.apache.cassandra.thrift.Column();
                       column.name = objToBB(subcol.get(0));
                       column.value = objToBB(subcol.get(1));
                       column.setTimestamp(System.currentTimeMillis() * 1000);
                       columns.add(column);
                   }
                   if (columns.isEmpty()) // a deletion
                   {
                       mutation.deletion = new Deletion();
                       mutation.deletion.super_column = objToBB(pair.get(0));
                       mutation.deletion.setTimestamp(System.currentTimeMillis() * 1000);
                   }
                   else
                   {
                       sc.columns = columns;
                       mutation.column_or_supercolumn = new ColumnOrSuperColumn();
                       mutation.column_or_supercolumn.super_column = sc;
                   }
               }
               else // assume column since it couldn't be anything else
               {
                   if (pair.get(1) == null)
                   {
                       mutation.deletion = new Deletion();
                       mutation.deletion.predicate = new org.apache.cassandra.thrift.SlicePredicate();
                       mutation.deletion.predicate.column_names = Arrays.asList(objToBB(pair.get(0)));
                       mutation.deletion.setTimestamp(System.currentTimeMillis() * 1000);
                   }
                   else
                   {
                       org.apache.cassandra.thrift.Column column = new org.apache.cassandra.thrift.Column();
                       column.name = marshallers.get(0).decompose((pair.get(0)));
                       if (validators.get(column.name) == null)
                           // Have to special case BytesType to convert DataByteArray into ByteBuffer
                           if (marshallers.get(1) instanceof BytesType)
                               column.value = objToBB(pair.get(1));
                           else
                               column.value = marshallers.get(1).decompose(pair.get(1));
                       else
                           column.value = validators.get(column.name).decompose(pair.get(1));
                       column.setTimestamp(System.currentTimeMillis() * 1000);
                       mutation.column_or_supercolumn = new ColumnOrSuperColumn();
                       mutation.column_or_supercolumn.column = column;
                   }
               }
               mutationList.add(mutation);
            }
        }
        catch (ClassCastException e)
        {
            throw new IOException(e + " Output must be (key, {(column,value)...}) for ColumnFamily or (key, {supercolumn:{(column,value)...}...}) for SuperColumnFamily", e);
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

    /* Methods to get the column family schema from Cassandra */

    private void initSchema(String signature)
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(CassandraStorage.class);

        // Only get the schema if we haven't already gotten it
        if (!property.containsKey(signature))
        {
            Cassandra.Client client = null;
            try
            {
                client = createConnection(ConfigHelper.getInitialAddress(conf), ConfigHelper.getRpcPort(conf), true);
                CfDef cfDef = null;
                client.set_keyspace(keyspace);
                KsDef ksDef = client.describe_keyspace(keyspace);
                List<CfDef> defs = ksDef.getCf_defs();
                for (CfDef def : defs)
                {
                    if (column_family.equalsIgnoreCase(def.getName()))
                    {
                        cfDef = def;
                        break;
                    }
                }
                property.setProperty(signature, cfdefToString(cfDef));
            }
            catch (TException e)
            {
                throw new RuntimeException(e);
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException(e);
            }
            catch (NotFoundException e)
            {
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws IOException
    {
        TSocket socket = new TSocket(host, port);
        TTransport trans = framed ? new TFramedTransport(socket) : socket;
        try
        {
            trans.open();
        }
        catch (TTransportException e)
        {
            throw new IOException("unable to connect to server", e);
        }
        return new Cassandra.Client(new TBinaryProtocol(trans));
    }

    private static String cfdefToString(CfDef cfDef)
    {
        assert cfDef != null;
        // this is so awful it's kind of cool!
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        try
        {
            return FBUtilities.bytesToHex(serializer.serialize(cfDef));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static CfDef cfdefFromString(String st)
    {
        assert st != null;
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        CfDef cfDef = new CfDef();
        try
        {
            deserializer.deserialize(cfDef, FBUtilities.hexToBytes(st));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return cfDef;
    }
}
