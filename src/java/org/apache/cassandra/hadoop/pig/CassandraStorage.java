/*
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
package org.apache.cassandra.hadoop.pig;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 * A LoadStoreFunc for retrieving data from and storing data to Cassandra
 *
 * A row from a standard CF will be returned as nested tuples: (key, ((name1, val1), (name2, val2))).
 */
@Deprecated
public class CassandraStorage extends LoadFunc implements StoreFuncInterface, LoadMetadata
{
    public final static String PIG_ALLOW_DELETES = "PIG_ALLOW_DELETES";
    public final static String PIG_WIDEROW_INPUT = "PIG_WIDEROW_INPUT";
    public final static String PIG_USE_SECONDARY = "PIG_USE_SECONDARY";

    private final static ByteBuffer BOUND = ByteBufferUtil.EMPTY_BYTE_BUFFER;
    private static final Logger logger = LoggerFactory.getLogger(CassandraStorage.class);

    private ByteBuffer slice_start = BOUND;
    private ByteBuffer slice_end = BOUND;
    private boolean slice_reverse = false;
    private boolean allow_deletes = false;

    private RecordReader<ByteBuffer, Map<ByteBuffer, Cell>> reader;
    private RecordWriter<ByteBuffer, List<Mutation>> writer;

    private boolean widerows = false;
    private int limit;

    protected String DEFAULT_INPUT_FORMAT;
    protected String DEFAULT_OUTPUT_FORMAT;

    protected enum MarshallerType { COMPARATOR, DEFAULT_VALIDATOR, KEY_VALIDATOR, SUBCOMPARATOR };

    protected String username;
    protected String password;
    protected String keyspace;
    protected String column_family;
    protected String loadSignature;
    protected String storeSignature;

    protected Configuration conf;
    protected String inputFormatClass;
    protected String outputFormatClass;
    protected int splitSize = 64 * 1024;
    protected String partitionerClass;
    protected boolean usePartitionFilter = false;
    protected String initHostAddress;
    protected String rpcPort;
    protected int nativeProtocolVersion = 1;
    
    // wide row hacks
    private ByteBuffer lastKey;
    private Map<ByteBuffer, Cell> lastRow;
    private boolean hasNext = true;

    public CassandraStorage()
    {
        this(1024);
    }

    /**@param limit number of columns to fetch in a slice */
    public CassandraStorage(int limit)
    {
        super();
        this.limit = limit;
        DEFAULT_INPUT_FORMAT = "org.apache.cassandra.hadoop.ColumnFamilyInputFormat";
        DEFAULT_OUTPUT_FORMAT = "org.apache.cassandra.hadoop.ColumnFamilyOutputFormat";
    }

    public int getLimit()
    {
        return limit;
    }

    public void prepareToRead(RecordReader reader, PigSplit split)
    {
        this.reader = reader;
    }

    /** read wide row*/
    public Tuple getNextWide() throws IOException
    {
        CfDef cfDef = getCfDef(loadSignature);
        ByteBuffer key = null;
        Tuple tuple = null; 
        DefaultDataBag bag = new DefaultDataBag();
        try
        {
            while(true)
            {
                hasNext = reader.nextKeyValue();
                if (!hasNext)
                {
                    if (tuple == null)
                        tuple = TupleFactory.getInstance().newTuple();

                    if (lastRow != null)
                    {
                        if (tuple.size() == 0) // lastRow is a new one
                        {
                            key = reader.getCurrentKey();
                            tuple = keyToTuple(key, cfDef, parseType(cfDef.getKey_validation_class()));
                        }
                        for (Map.Entry<ByteBuffer, Cell> entry : lastRow.entrySet())
                        {
                            bag.add(columnToTuple(entry.getValue(), cfDef, parseType(cfDef.getComparator_type())));
                        }
                        lastKey = null;
                        lastRow = null;
                        tuple.append(bag);
                        return tuple;
                    }
                    else
                    {
                        if (tuple.size() == 1) // rare case of just one wide row, key already set
                        {
                            tuple.append(bag);
                            return tuple;
                        }
                        else
                            return null;
                    }
                }
                if (key != null && !(reader.getCurrentKey()).equals(key)) // key changed
                {
                    // read too much, hold on to it for next time
                    lastKey = reader.getCurrentKey();
                    lastRow = reader.getCurrentValue();
                    // but return what we have so far
                    tuple.append(bag);
                    return tuple;
                }
                if (key == null) // only set the key on the first iteration
                {
                    key = reader.getCurrentKey();
                    if (lastKey != null && !(key.equals(lastKey))) // last key only had one value
                    {
                        if (tuple == null)
                            tuple = keyToTuple(lastKey, cfDef, parseType(cfDef.getKey_validation_class()));
                        else
                            addKeyToTuple(tuple, lastKey, cfDef, parseType(cfDef.getKey_validation_class()));
                        for (Map.Entry<ByteBuffer, Cell> entry : lastRow.entrySet())
                        {
                            bag.add(columnToTuple(entry.getValue(), cfDef, parseType(cfDef.getComparator_type())));
                        }
                        tuple.append(bag);
                        lastKey = key;
                        lastRow = reader.getCurrentValue();
                        return tuple;
                    }
                    if (tuple == null)
                        tuple = keyToTuple(key, cfDef, parseType(cfDef.getKey_validation_class()));
                    else
                        addKeyToTuple(tuple, lastKey, cfDef, parseType(cfDef.getKey_validation_class()));
                }
                SortedMap<ByteBuffer, Cell> row = (SortedMap<ByteBuffer, Cell>)reader.getCurrentValue();
                if (lastRow != null) // prepend what was read last time
                {
                    for (Map.Entry<ByteBuffer, Cell> entry : lastRow.entrySet())
                    {
                        bag.add(columnToTuple(entry.getValue(), cfDef, parseType(cfDef.getComparator_type())));
                    }
                    lastKey = null;
                    lastRow = null;
                }
                for (Map.Entry<ByteBuffer, Cell> entry : row.entrySet())
                {
                    bag.add(columnToTuple(entry.getValue(), cfDef, parseType(cfDef.getComparator_type())));
                }
            }
        }
        catch (InterruptedException e)
        {
            throw new IOException(e.getMessage());
        }
    }

    /** read next row */
    public Tuple getNext() throws IOException
    {
        if (widerows)
            return getNextWide();
        try
        {
            // load the next pair
            if (!reader.nextKeyValue())
                return null;

            CfDef cfDef = getCfDef(loadSignature);
            ByteBuffer key = reader.getCurrentKey();
            Map<ByteBuffer, Cell> cf = reader.getCurrentValue();
            assert key != null && cf != null;

            // output tuple, will hold the key, each indexed column in a tuple, then a bag of the rest
            // NOTE: we're setting the tuple size here only for the key so we can use setTupleValue on it

            Tuple tuple = keyToTuple(key, cfDef, parseType(cfDef.getKey_validation_class()));
            DefaultDataBag bag = new DefaultDataBag();
            // we must add all the indexed columns first to match the schema
            Map<ByteBuffer, Boolean> added = new HashMap<ByteBuffer, Boolean>(cfDef.column_metadata.size());
            // take care to iterate these in the same order as the schema does
            for (ColumnDef cdef : cfDef.column_metadata)
            {
                boolean hasColumn = false;
                boolean cql3Table = false;
                try
                {
                    hasColumn = cf.containsKey(cdef.name);
                }
                catch (Exception e)
                {
                    cql3Table = true;
                }
                if (hasColumn)
                {
                    tuple.append(columnToTuple(cf.get(cdef.name), cfDef, parseType(cfDef.getComparator_type())));
                }
                else if (!cql3Table)
                {   // otherwise, we need to add an empty tuple to take its place
                    tuple.append(TupleFactory.getInstance().newTuple());
                }
                added.put(cdef.name, true);
            }
            // now add all the other columns
            for (Map.Entry<ByteBuffer, Cell> entry : cf.entrySet())
            {
                if (!added.containsKey(entry.getKey()))
                    bag.add(columnToTuple(entry.getValue(), cfDef, parseType(cfDef.getComparator_type())));
            }
            tuple.append(bag);
            // finally, special top-level indexes if needed
            if (usePartitionFilter)
            {
                for (ColumnDef cdef : getIndexes())
                {
                    Tuple throwaway = columnToTuple(cf.get(cdef.name), cfDef, parseType(cfDef.getComparator_type()));
                    tuple.append(throwaway.get(1));
                }
            }
            return tuple;
        }
        catch (InterruptedException e)
        {
            throw new IOException(e.getMessage());
        }
    }

    /** write next row */
    public void putNext(Tuple t) throws IOException
    {
        /*
        We support two cases for output:
        First, the original output:
            (key, (name, value), (name,value), {(name,value)}) (tuples or bag is optional)
        For supers, we only accept the original output.
        */

        if (t.size() < 1)
        {
            // simply nothing here, we can't even delete without a key
            logger.warn("Empty output skipped, filter empty tuples to suppress this warning");
            return;
        }
        ByteBuffer key = objToBB(t.get(0));
        if (t.getType(1) == DataType.TUPLE)
            writeColumnsFromTuple(key, t, 1);
        else if (t.getType(1) == DataType.BAG)
        {
            if (t.size() > 2)
                throw new IOException("No arguments allowed after bag");
            writeColumnsFromBag(key, (DataBag) t.get(1));
        }
        else
            throw new IOException("Second argument in output must be a tuple or bag");
    }

    /** set hadoop cassandra connection settings */
    protected void setConnectionInformation() throws IOException
    {
        StorageHelper.setConnectionInformation(conf);
        if (System.getenv(StorageHelper.PIG_INPUT_FORMAT) != null)
            inputFormatClass = getFullyQualifiedClassName(System.getenv(StorageHelper.PIG_INPUT_FORMAT));
        else
            inputFormatClass = DEFAULT_INPUT_FORMAT;
        if (System.getenv(StorageHelper.PIG_OUTPUT_FORMAT) != null)
            outputFormatClass = getFullyQualifiedClassName(System.getenv(StorageHelper.PIG_OUTPUT_FORMAT));
        else
            outputFormatClass = DEFAULT_OUTPUT_FORMAT;
        if (System.getenv(PIG_ALLOW_DELETES) != null)
            allow_deletes = Boolean.parseBoolean(System.getenv(PIG_ALLOW_DELETES));
    }

    /** get the full class name */
    protected String getFullyQualifiedClassName(String classname)
    {
        return classname.contains(".") ? classname : "org.apache.cassandra.hadoop." + classname;
    }

    /** set read configuration settings */
    public void setLocation(String location, Job job) throws IOException
    {
        conf = HadoopCompat.getConfiguration(job);
        setLocationFromUri(location);

        if (ConfigHelper.getInputSlicePredicate(conf) == null)
        {
            SliceRange range = new SliceRange(slice_start, slice_end, slice_reverse, limit);
            SlicePredicate predicate = new SlicePredicate().setSlice_range(range);
            ConfigHelper.setInputSlicePredicate(conf, predicate);
        }
        if (System.getenv(PIG_WIDEROW_INPUT) != null)
            widerows = Boolean.parseBoolean(System.getenv(PIG_WIDEROW_INPUT));
        if (System.getenv(PIG_USE_SECONDARY) != null)
            usePartitionFilter = Boolean.parseBoolean(System.getenv(PIG_USE_SECONDARY));
        if (System.getenv(StorageHelper.PIG_INPUT_SPLIT_SIZE) != null)
        {
            try
            {
                ConfigHelper.setInputSplitSize(conf, Integer.parseInt(System.getenv(StorageHelper.PIG_INPUT_SPLIT_SIZE)));
            }
            catch (NumberFormatException e)
            {
                throw new IOException("PIG_INPUT_SPLIT_SIZE is not a number", e);
            }           
        } 

        if (usePartitionFilter && getIndexExpressions() != null)
            ConfigHelper.setInputRange(conf, getIndexExpressions());

        if (username != null && password != null)
            ConfigHelper.setInputKeyspaceUserNameAndPassword(conf, username, password);

        if (splitSize > 0)
            ConfigHelper.setInputSplitSize(conf, splitSize);
        if (partitionerClass!= null)
            ConfigHelper.setInputPartitioner(conf, partitionerClass);
        if (rpcPort != null)
            ConfigHelper.setInputRpcPort(conf, rpcPort);
        if (initHostAddress != null)
            ConfigHelper.setInputInitialAddress(conf, initHostAddress);

        ConfigHelper.setInputColumnFamily(conf, keyspace, column_family, widerows);
        setConnectionInformation();

        if (ConfigHelper.getInputRpcPort(conf) == 0)
            throw new IOException("PIG_INPUT_RPC_PORT or PIG_RPC_PORT environment variable not set");
        if (ConfigHelper.getInputInitialAddress(conf) == null)
            throw new IOException("PIG_INPUT_INITIAL_ADDRESS or PIG_INITIAL_ADDRESS environment variable not set");
        if (ConfigHelper.getInputPartitioner(conf) == null)
            throw new IOException("PIG_INPUT_PARTITIONER or PIG_PARTITIONER environment variable not set");
        if (loadSignature == null)
            loadSignature = location;
        initSchema(loadSignature);
    }

    /** set store configuration settings */
    public void setStoreLocation(String location, Job job) throws IOException
    {
        conf = HadoopCompat.getConfiguration(job);
        
        // don't combine mappers to a single mapper per node
        conf.setBoolean("pig.noSplitCombination", true);
        setLocationFromUri(location);

        if (username != null && password != null)
            ConfigHelper.setOutputKeyspaceUserNameAndPassword(conf, username, password);
        if (splitSize > 0)
            ConfigHelper.setInputSplitSize(conf, splitSize);
        if (partitionerClass!= null)
            ConfigHelper.setOutputPartitioner(conf, partitionerClass);
        if (rpcPort != null)
        {
            ConfigHelper.setOutputRpcPort(conf, rpcPort);
            ConfigHelper.setInputRpcPort(conf, rpcPort);
        }
        if (initHostAddress != null)
        {
            ConfigHelper.setOutputInitialAddress(conf, initHostAddress);
            ConfigHelper.setInputInitialAddress(conf, initHostAddress);
        }

        ConfigHelper.setOutputColumnFamily(conf, keyspace, column_family);
        setConnectionInformation();

        if (ConfigHelper.getOutputRpcPort(conf) == 0)
            throw new IOException("PIG_OUTPUT_RPC_PORT or PIG_RPC_PORT environment variable not set");
        if (ConfigHelper.getOutputInitialAddress(conf) == null)
            throw new IOException("PIG_OUTPUT_INITIAL_ADDRESS or PIG_INITIAL_ADDRESS environment variable not set");
        if (ConfigHelper.getOutputPartitioner(conf) == null)
            throw new IOException("PIG_OUTPUT_PARTITIONER or PIG_PARTITIONER environment variable not set");

        // we have to do this again here for the check in writeColumnsFromTuple
        if (System.getenv(PIG_USE_SECONDARY) != null)
            usePartitionFilter = Boolean.parseBoolean(System.getenv(PIG_USE_SECONDARY));

        initSchema(storeSignature);
    }

    /** Methods to get the column family schema from Cassandra */
    protected void initSchema(String signature) throws IOException
    {
        Properties properties = UDFContext.getUDFContext().getUDFProperties(CassandraStorage.class);

        // Only get the schema if we haven't already gotten it
        if (!properties.containsKey(signature))
        {
            try
            {
                Cassandra.Client client = ConfigHelper.getClientFromInputAddressList(conf);
                client.set_keyspace(keyspace);

                if (username != null && password != null)
                {
                    Map<String, String> credentials = new HashMap<String, String>(2);
                    credentials.put(PasswordAuthenticator.USERNAME_KEY, username);
                    credentials.put(PasswordAuthenticator.PASSWORD_KEY, password);

                    try
                    {
                        client.login(new AuthenticationRequest(credentials));
                    }
                    catch (AuthenticationException e)
                    {
                        logger.error("Authentication exception: invalid username and/or password");
                        throw new IOException(e);
                    }
                }

                // compose the CfDef for the columfamily
                CfDef cfDef = getCfDef(client);

                if (cfDef != null)
                {
                    StringBuilder sb = new StringBuilder();
                    sb.append(cfdefToString(cfDef));
                    properties.setProperty(signature, sb.toString());
                }
                else
                    throw new IOException(String.format("Table '%s' not found in keyspace '%s'",
                            column_family,
                            keyspace));
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }
        }
    }

    public void checkSchema(ResourceSchema schema) throws IOException
    {
        // we don't care about types, they all get casted to ByteBuffers
    }

    /** define the schema */
    public ResourceSchema getSchema(String location, Job job) throws IOException
    {
        setLocation(location, job);
        CfDef cfDef = getCfDef(loadSignature);
        if (cfDef.column_type.equals("Super"))
            return null;
        /*
        Our returned schema should look like this:
        (key, index1:(name, value), index2:(name, value), columns:{(name, value)})
        Which is to say, columns that have metadata will be returned as named tuples, but unknown columns will go into a bag.
        This way, wide rows can still be handled by the bag, but known columns can easily be referenced.
         */

        // top-level schema, no type
        ResourceSchema schema = new ResourceSchema();

        // get default marshallers and validators
        Map<MarshallerType, AbstractType> marshallers = getDefaultMarshallers(cfDef);
        Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);

        // add key
        ResourceFieldSchema keyFieldSchema = new ResourceFieldSchema();
        keyFieldSchema.setName("key");
        keyFieldSchema.setType(StorageHelper.getPigType(marshallers.get(MarshallerType.KEY_VALIDATOR)));

        ResourceSchema bagSchema = new ResourceSchema();
        ResourceFieldSchema bagField = new ResourceFieldSchema();
        bagField.setType(DataType.BAG);
        bagField.setName("columns");
        // inside the bag, place one tuple with the default comparator/validator schema
        ResourceSchema bagTupleSchema = new ResourceSchema();
        ResourceFieldSchema bagTupleField = new ResourceFieldSchema();
        bagTupleField.setType(DataType.TUPLE);
        ResourceFieldSchema bagcolSchema = new ResourceFieldSchema();
        ResourceFieldSchema bagvalSchema = new ResourceFieldSchema();
        bagcolSchema.setName("name");
        bagvalSchema.setName("value");
        bagcolSchema.setType(StorageHelper.getPigType(marshallers.get(MarshallerType.COMPARATOR)));
        bagvalSchema.setType(StorageHelper.getPigType(marshallers.get(MarshallerType.DEFAULT_VALIDATOR)));
        bagTupleSchema.setFields(new ResourceFieldSchema[] { bagcolSchema, bagvalSchema });
        bagTupleField.setSchema(bagTupleSchema);
        bagSchema.setFields(new ResourceFieldSchema[] { bagTupleField });
        bagField.setSchema(bagSchema);

        // will contain all fields for this schema
        List<ResourceFieldSchema> allSchemaFields = new ArrayList<ResourceFieldSchema>();
        // add the key first, then the indexed columns, and finally the bag
        allSchemaFields.add(keyFieldSchema);

        if (!widerows)
        {
            // defined validators/indexes
            for (ColumnDef cdef : cfDef.column_metadata)
            {
                // make a new tuple for each col/val pair
                ResourceSchema innerTupleSchema = new ResourceSchema();
                ResourceFieldSchema innerTupleField = new ResourceFieldSchema();
                innerTupleField.setType(DataType.TUPLE);
                innerTupleField.setSchema(innerTupleSchema);
                innerTupleField.setName(new String(cdef.getName()));

                ResourceFieldSchema idxColSchema = new ResourceFieldSchema();
                idxColSchema.setName("name");
                idxColSchema.setType(StorageHelper.getPigType(marshallers.get(MarshallerType.COMPARATOR)));

                ResourceFieldSchema valSchema = new ResourceFieldSchema();
                AbstractType validator = validators.get(cdef.name);
                if (validator == null)
                    validator = marshallers.get(MarshallerType.DEFAULT_VALIDATOR);
                valSchema.setName("value");
                valSchema.setType(StorageHelper.getPigType(validator));

                innerTupleSchema.setFields(new ResourceFieldSchema[] { idxColSchema, valSchema });
                allSchemaFields.add(innerTupleField);
            }   
        }

        // bag at the end for unknown columns
        allSchemaFields.add(bagField);

        // add top-level index elements if needed
        if (usePartitionFilter)
        {
            for (ColumnDef cdef : getIndexes())
            {
                ResourceFieldSchema idxSchema = new ResourceFieldSchema();
                idxSchema.setName("index_" + new String(cdef.getName()));
                AbstractType validator = validators.get(cdef.name);
                if (validator == null)
                    validator = marshallers.get(MarshallerType.DEFAULT_VALIDATOR);
                idxSchema.setType(StorageHelper.getPigType(validator));
                allSchemaFields.add(idxSchema);
            }
        }
        // top level schema contains everything
        schema.setFields(allSchemaFields.toArray(new ResourceFieldSchema[allSchemaFields.size()]));
        return schema;
    }

    /** set partition filter */
    public void setPartitionFilter(Expression partitionFilter) throws IOException
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(CassandraStorage.class);
        property.setProperty(StorageHelper.PARTITION_FILTER_SIGNATURE, indexExpressionsToString(filterToIndexExpressions(partitionFilter)));
    }

    /** prepare writer */
    public void prepareToWrite(RecordWriter writer)
    {
        this.writer = writer;
    }

    /** convert object to ByteBuffer */
    protected ByteBuffer objToBB(Object o)
    {
        if (o == null)
            return nullToBB();
        if (o instanceof java.lang.String)
            return ByteBuffer.wrap(new DataByteArray((String)o).get());
        if (o instanceof Integer)
            return Int32Type.instance.decompose((Integer)o);
        if (o instanceof Long)
            return LongType.instance.decompose((Long)o);
        if (o instanceof Float)
            return FloatType.instance.decompose((Float)o);
        if (o instanceof Double)
            return DoubleType.instance.decompose((Double)o);
        if (o instanceof UUID)
            return ByteBuffer.wrap(UUIDGen.decompose((UUID) o));
        if(o instanceof Tuple) {
            List<Object> objects = ((Tuple)o).getAll();
            //collections
            if (objects.size() > 0 && objects.get(0) instanceof String)
            {
                String collectionType = (String) objects.get(0);
                if ("set".equalsIgnoreCase(collectionType) ||
                        "list".equalsIgnoreCase(collectionType))
                    return objToListOrSetBB(objects.subList(1, objects.size()));
                else if ("map".equalsIgnoreCase(collectionType))
                    return objToMapBB(objects.subList(1, objects.size()));

            }
            return objToCompositeBB(objects);
        }

        return ByteBuffer.wrap(((DataByteArray) o).get());
    }

    private ByteBuffer objToListOrSetBB(List<Object> objects)
    {
        List<ByteBuffer> serialized = new ArrayList<ByteBuffer>(objects.size());
        for(Object sub : objects)
        {
            ByteBuffer buffer = objToBB(sub);
            serialized.add(buffer);
        }
        // NOTE: using protocol v1 serialization format for collections so as to not break
        // compatibility. Not sure if that's the right thing.
        return CollectionSerializer.pack(serialized, objects.size(), 1);
    }

    private ByteBuffer objToMapBB(List<Object> objects)
    {
        List<ByteBuffer> serialized = new ArrayList<ByteBuffer>(objects.size() * 2);
        for(Object sub : objects)
        {
            List<Object> keyValue = ((Tuple)sub).getAll();
            for (Object entry: keyValue)
            {
                ByteBuffer buffer = objToBB(entry);
                serialized.add(buffer);
            }
        }
        // NOTE: using protocol v1 serialization format for collections so as to not break
        // compatibility. Not sure if that's the right thing.
        return CollectionSerializer.pack(serialized, objects.size(), 1);
    }

    private ByteBuffer objToCompositeBB(List<Object> objects)
    {
        List<ByteBuffer> serialized = new ArrayList<ByteBuffer>(objects.size());
        int totalLength = 0;
        for(Object sub : objects)
        {
            ByteBuffer buffer = objToBB(sub);
            serialized.add(buffer);
            totalLength += 2 + buffer.remaining() + 1;
        }
        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer bb : serialized)
        {
            int length = bb.remaining();
            out.put((byte) ((length >> 8) & 0xFF));
            out.put((byte) (length & 0xFF));
            out.put(bb);
            out.put((byte) 0);
        }
        out.flip();
        return out;
    }

    /** write tuple data to cassandra */
    private void writeColumnsFromTuple(ByteBuffer key, Tuple t, int offset) throws IOException
    {
        ArrayList<Mutation> mutationList = new ArrayList<Mutation>();
        for (int i = offset; i < t.size(); i++)
        {
            if (t.getType(i) == DataType.BAG)
                writeColumnsFromBag(key, (DataBag) t.get(i));
            else if (t.getType(i) == DataType.TUPLE)
            {
                Tuple inner = (Tuple) t.get(i);
                if (inner.size() > 0) // may be empty, for an indexed column that wasn't present
                    mutationList.add(mutationFromTuple(inner));
            }
            else if (!usePartitionFilter)
            {
                throw new IOException("Output type was not a bag or a tuple");
            }
        }
        if (mutationList.size() > 0)
            writeMutations(key, mutationList);
    }

    /** compose Cassandra mutation from tuple */
    private Mutation mutationFromTuple(Tuple t) throws IOException
    {
        Mutation mutation = new Mutation();
        if (t.get(1) == null)
        {
            if (allow_deletes)
            {
                mutation.deletion = new Deletion();
                mutation.deletion.predicate = new org.apache.cassandra.thrift.SlicePredicate();
                mutation.deletion.predicate.column_names = Arrays.asList(objToBB(t.get(0)));
                mutation.deletion.setTimestamp(FBUtilities.timestampMicros());
            }
            else
                throw new IOException("null found but deletes are disabled, set " + PIG_ALLOW_DELETES +
                    "=true in environment or allow_deletes=true in URL to enable");
        }
        else
        {
            org.apache.cassandra.thrift.Column column = new org.apache.cassandra.thrift.Column();
            column.setName(objToBB(t.get(0)));
            column.setValue(objToBB(t.get(1)));
            column.setTimestamp(FBUtilities.timestampMicros());
            mutation.column_or_supercolumn = new ColumnOrSuperColumn();
            mutation.column_or_supercolumn.column = column;
        }
        return mutation;
    }

    /** write bag data to Cassandra */
    private void writeColumnsFromBag(ByteBuffer key, DataBag bag) throws IOException
    {
        List<Mutation> mutationList = new ArrayList<Mutation>();
        for (Tuple pair : bag)
        {
            Mutation mutation = new Mutation();
            if (DataType.findType(pair.get(1)) == DataType.BAG) // supercolumn
            {
                SuperColumn sc = new SuperColumn();
                sc.setName(objToBB(pair.get(0)));
                List<org.apache.cassandra.thrift.Column> columns = new ArrayList<org.apache.cassandra.thrift.Column>();
                for (Tuple subcol : (DataBag) pair.get(1))
                {
                    org.apache.cassandra.thrift.Column column = new org.apache.cassandra.thrift.Column();
                    column.setName(objToBB(subcol.get(0)));
                    column.setValue(objToBB(subcol.get(1)));
                    column.setTimestamp(FBUtilities.timestampMicros());
                    columns.add(column);
                }
                if (columns.isEmpty())
                {
                    if (allow_deletes)
                    {
                        mutation.deletion = new Deletion();
                        mutation.deletion.super_column = objToBB(pair.get(0));
                        mutation.deletion.setTimestamp(FBUtilities.timestampMicros());
                    }
                    else
                        throw new IOException("SuperColumn deletion attempted with empty bag, but deletes are disabled, set " +
                            PIG_ALLOW_DELETES + "=true in environment or allow_deletes=true in URL to enable");
                }
                else
                {
                    sc.columns = columns;
                    mutation.column_or_supercolumn = new ColumnOrSuperColumn();
                    mutation.column_or_supercolumn.super_column = sc;
                }
            }
            else
                mutation = mutationFromTuple(pair);
            mutationList.add(mutation);
            // for wide rows, we need to limit the amount of mutations we write at once
            if (mutationList.size() >= 10) // arbitrary, CFOF will re-batch this up, and BOF won't care
            {
                writeMutations(key, mutationList);
                mutationList.clear();
            }
        }
        // write the last batch
        if (mutationList.size() > 0)
            writeMutations(key, mutationList);
    }

    /** write mutation to Cassandra */
    private void writeMutations(ByteBuffer key, List<Mutation> mutations) throws IOException
    {
        try
        {
            writer.write(key, mutations);
        }
        catch (InterruptedException e)
        {
            throw new IOException(e);
        }
    }

    /** get a list of columns with defined index*/
    protected List<ColumnDef> getIndexes() throws IOException
    {
        CfDef cfdef = getCfDef(loadSignature);
        List<ColumnDef> indexes = new ArrayList<ColumnDef>();
        for (ColumnDef cdef : cfdef.column_metadata)
        {
            if (cdef.index_type != null)
                indexes.add(cdef);
        }
        return indexes;
    }

    /** get a list of Cassandra IndexExpression from Pig expression */
    private List<IndexExpression> filterToIndexExpressions(Expression expression) throws IOException
    {
        List<IndexExpression> indexExpressions = new ArrayList<IndexExpression>();
        Expression.BinaryExpression be = (Expression.BinaryExpression)expression;
        ByteBuffer name = ByteBuffer.wrap(be.getLhs().toString().getBytes());
        ByteBuffer value = ByteBuffer.wrap(be.getRhs().toString().getBytes());
        switch (expression.getOpType())
        {
            case OP_EQ:
                indexExpressions.add(new IndexExpression(name, IndexOperator.EQ, value));
                break;
            case OP_GE:
                indexExpressions.add(new IndexExpression(name, IndexOperator.GTE, value));
                break;
            case OP_GT:
                indexExpressions.add(new IndexExpression(name, IndexOperator.GT, value));
                break;
            case OP_LE:
                indexExpressions.add(new IndexExpression(name, IndexOperator.LTE, value));
                break;
            case OP_LT:
                indexExpressions.add(new IndexExpression(name, IndexOperator.LT, value));
                break;
            case OP_AND:
                indexExpressions.addAll(filterToIndexExpressions(be.getLhs()));
                indexExpressions.addAll(filterToIndexExpressions(be.getRhs()));
                break;
            default:
                throw new IOException("Unsupported expression type: " + expression.getOpType().name());
        }
        return indexExpressions;
    }

    /** convert a list of index expression to string */
    private static String indexExpressionsToString(List<IndexExpression> indexExpressions) throws IOException
    {
        assert indexExpressions != null;
        // oh, you thought cfdefToString was awful?
        IndexClause indexClause = new IndexClause();
        indexClause.setExpressions(indexExpressions);
        indexClause.setStart_key("".getBytes());
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        try
        {
            return Hex.bytesToHex(serializer.serialize(indexClause));
        }
        catch (TException e)
        {
            throw new IOException(e);
        }
    }

    /** convert string to a list of index expression */
    private static List<IndexExpression> indexExpressionsFromString(String ie) throws IOException
    {
        assert ie != null;
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        IndexClause indexClause = new IndexClause();
        try
        {
            deserializer.deserialize(indexClause, Hex.hexToBytes(ie));
        }
        catch (TException e)
        {
            throw new IOException(e);
        }
        return indexClause.getExpressions();
    }

    public ResourceStatistics getStatistics(String location, Job job)
    {
        return null;
    }

    public void cleanupOnFailure(String failure, Job job)
    {
    }

    public void cleanupOnSuccess(String location, Job job) throws IOException {
    }


    /** StoreFunc methods */
    public void setStoreFuncUDFContextSignature(String signature)
    {
        this.storeSignature = signature;
    }

    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException
    {
        return relativeToAbsolutePath(location, curDir);
    }

    /** output format */
    public OutputFormat getOutputFormat() throws IOException
    {
        try
        {
            return FBUtilities.construct(outputFormatClass, "outputformat");
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
    }


    @Override
    public InputFormat getInputFormat() throws IOException
    {
        try
        {
            return FBUtilities.construct(inputFormatClass, "inputformat");
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
    }

    /** get a list of index expression */
    private List<IndexExpression> getIndexExpressions() throws IOException
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(CassandraStorage.class);
        if (property.getProperty(StorageHelper.PARTITION_FILTER_SIGNATURE) != null)
            return indexExpressionsFromString(property.getProperty(StorageHelper.PARTITION_FILTER_SIGNATURE));
        else
            return null;
    }

    /** get a list of column for the column family */
    protected List<ColumnDef> getColumnMetadata(Cassandra.Client client)
    throws TException, CharacterCodingException, InvalidRequestException, ConfigurationException
    {   
        return getColumnMeta(client, true, true);
    }


    /** get column meta data */
    protected List<ColumnDef> getColumnMeta(Cassandra.Client client, boolean cassandraStorage, boolean includeCompactValueColumn)
            throws TException,
            CharacterCodingException,
            ConfigurationException
    {
        String query = String.format("SELECT column_name, validator, index_type, type " +
                        "FROM %s.%s " +
                        "WHERE keyspace_name = '%s' AND columnfamily_name = '%s'",
                SystemKeyspace.NAME,
                LegacySchemaTables.COLUMNS,
                keyspace,
                column_family);

        CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);

        List<CqlRow> rows = result.rows;
        List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();
        if (rows == null || rows.isEmpty())
        {
            // if CassandraStorage, just return the empty list
            if (cassandraStorage)
                return columnDefs;

            // otherwise for CqlNativeStorage, check metadata for classic thrift tables
            CFMetaData cfm = getCFMetaData(keyspace, column_family, client);
            for (ColumnDefinition def : cfm.regularAndStaticColumns())
            {
                ColumnDef cDef = new ColumnDef();
                String columnName = def.name.toString();
                String type = def.type.toString();
                logger.debug("name: {}, type: {} ", columnName, type);
                cDef.name = ByteBufferUtil.bytes(columnName);
                cDef.validation_class = type;
                columnDefs.add(cDef);
            }
            // we may not need to include the value column for compact tables as we
            // could have already processed it as schema_columnfamilies.value_alias
            if (columnDefs.size() == 0 && includeCompactValueColumn && cfm.compactValueColumn() != null)
            {
                ColumnDefinition def = cfm.compactValueColumn();
                if ("value".equals(def.name.toString()))
                {
                    ColumnDef cDef = new ColumnDef();
                    cDef.name = def.name.bytes;
                    cDef.validation_class = def.type.toString();
                    columnDefs.add(cDef);
                }
            }
            return columnDefs;
        }

        Iterator<CqlRow> iterator = rows.iterator();
        while (iterator.hasNext())
        {
            CqlRow row = iterator.next();
            ColumnDef cDef = new ColumnDef();
            String type = ByteBufferUtil.string(row.getColumns().get(3).value);
            if (!type.equals("regular"))
                continue;
            cDef.setName(ByteBufferUtil.clone(row.getColumns().get(0).value));
            cDef.validation_class = ByteBufferUtil.string(row.getColumns().get(1).value);
            ByteBuffer indexType = row.getColumns().get(2).value;
            if (indexType != null)
                cDef.index_type = getIndexType(ByteBufferUtil.string(indexType));
            columnDefs.add(cDef);
        }
        return columnDefs;
    }


    /** get CFMetaData of a column family */
    protected CFMetaData getCFMetaData(String ks, String cf, Cassandra.Client client)
            throws TException, ConfigurationException
    {
        KsDef ksDef = client.describe_keyspace(ks);
        for (CfDef cfDef : ksDef.cf_defs)
        {
            if (cfDef.name.equalsIgnoreCase(cf))
                return ThriftConversion.fromThrift(cfDef);
        }
        return null;
    }

    /** get index type from string */
    protected IndexType getIndexType(String type)
    {
        type = type.toLowerCase();
        if ("keys".equals(type))
            return IndexType.KEYS;
        else if("custom".equals(type))
            return IndexType.CUSTOM;
        else if("composites".equals(type))
            return IndexType.COMPOSITES;
        else
            return null;
    }

    /** return partition keys */
    public String[] getPartitionKeys(String location, Job job) throws IOException
    {
        if (!usePartitionFilter)
            return null;
        List<ColumnDef> indexes = getIndexes();
        String[] partitionKeys = new String[indexes.size()];
        for (int i = 0; i < indexes.size(); i++)
        {
            partitionKeys[i] = new String(indexes.get(i).getName());
        }
        return partitionKeys;
    }

    /** convert key to a tuple */
    private Tuple keyToTuple(ByteBuffer key, CfDef cfDef, AbstractType comparator) throws IOException
    {
        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        addKeyToTuple(tuple, key, cfDef, comparator);
        return tuple;
    }

    /** add key to a tuple */
    private void addKeyToTuple(Tuple tuple, ByteBuffer key, CfDef cfDef, AbstractType comparator) throws IOException
    {
        if( comparator instanceof AbstractCompositeType )
        {
            StorageHelper.setTupleValue(tuple, 0, composeComposite((AbstractCompositeType) comparator, key));
        }
        else
        {
            StorageHelper.setTupleValue(tuple, 0, StorageHelper.cassandraToObj(getDefaultMarshallers(cfDef).get(MarshallerType.KEY_VALIDATOR), key, nativeProtocolVersion));
        }

    }

    /** Deconstructs a composite type to a Tuple. */
    protected Tuple composeComposite(AbstractCompositeType comparator, ByteBuffer name) throws IOException
    {
        List<AbstractCompositeType.CompositeComponent> result = comparator.deconstruct(name);
        Tuple t = TupleFactory.getInstance().newTuple(result.size());
        for (int i=0; i<result.size(); i++)
            StorageHelper.setTupleValue(t, i, StorageHelper.cassandraToObj(result.get(i).comparator, result.get(i).value, nativeProtocolVersion));

        return t;
    }

    /** cassandra://[username:password@]<keyspace>/<columnfamily>[?slice_start=<start>&slice_end=<end>
     * [&reversed=true][&limit=1][&allow_deletes=true][&widerows=true]
     * [&use_secondary=true][&comparator=<comparator>][&partitioner=<partitioner>]]*/
    private void setLocationFromUri(String location) throws IOException
    {
        try
        {
            if (!location.startsWith("cassandra://"))
                throw new Exception("Bad scheme." + location);
            
            String[] urlParts = location.split("\\?");
            if (urlParts.length > 1)
            {
                Map<String, String> urlQuery = getQueryMap(urlParts[1]);
                AbstractType comparator = BytesType.instance;
                if (urlQuery.containsKey("comparator"))
                    comparator = TypeParser.parse(urlQuery.get("comparator"));
                if (urlQuery.containsKey("slice_start"))
                    slice_start = comparator.fromString(urlQuery.get("slice_start"));
                if (urlQuery.containsKey("slice_end"))
                    slice_end = comparator.fromString(urlQuery.get("slice_end"));
                if (urlQuery.containsKey("reversed"))
                    slice_reverse = Boolean.parseBoolean(urlQuery.get("reversed"));
                if (urlQuery.containsKey("limit"))
                    limit = Integer.parseInt(urlQuery.get("limit"));
                if (urlQuery.containsKey("allow_deletes"))
                    allow_deletes = Boolean.parseBoolean(urlQuery.get("allow_deletes"));
                if (urlQuery.containsKey("widerows"))
                    widerows = Boolean.parseBoolean(urlQuery.get("widerows"));
                if (urlQuery.containsKey("use_secondary"))
                    usePartitionFilter = Boolean.parseBoolean(urlQuery.get("use_secondary"));
                if (urlQuery.containsKey("split_size"))
                    splitSize = Integer.parseInt(urlQuery.get("split_size"));
                if (urlQuery.containsKey("partitioner"))
                    partitionerClass = urlQuery.get("partitioner");
                if (urlQuery.containsKey("init_address"))
                    initHostAddress = urlQuery.get("init_address");
                if (urlQuery.containsKey("rpc_port"))
                    rpcPort = urlQuery.get("rpc_port");
            }
            String[] parts = urlParts[0].split("/+");
            String[] credentialsAndKeyspace = parts[1].split("@");
            if (credentialsAndKeyspace.length > 1)
            {
                String[] credentials = credentialsAndKeyspace[0].split(":");
                username = credentials[0];
                password = credentials[1];
                keyspace = credentialsAndKeyspace[1];
            }
            else
            {
                keyspace = parts[1];
            }
            column_family = parts[2];
        }
        catch (Exception e)
        {
            throw new IOException("Expected 'cassandra://[username:password@]<keyspace>/<table>" +
                    "[?slice_start=<start>&slice_end=<end>[&reversed=true][&limit=1]" +
                    "[&allow_deletes=true][&widerows=true][&use_secondary=true]" +
                    "[&comparator=<comparator>][&split_size=<size>][&partitioner=<partitioner>]" +
                    "[&init_address=<host>][&rpc_port=<port>]]': " + e.getMessage());
        }
    }


    /** decompose the query to store the parameters in a map */
    public static Map<String, String> getQueryMap(String query) throws UnsupportedEncodingException
    {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>(params.length);
        for (String param : params)
        {
            String[] keyValue = param.split("=");
            map.put(keyValue[0], URLDecoder.decode(keyValue[1], "UTF-8"));
        }
        return map;
    }

    public ByteBuffer nullToBB()
    {
        return null;
    }

    /** return the CfInfo for the column family */
    protected CfDef getCfDef(Cassandra.Client client)
            throws TException,
            ConfigurationException,
            IOException
    {
        // get CF meta data
        String query = String.format("SELECT type, comparator, subcomparator, default_validator, key_validator " +
                        "FROM %s.%s " +
                        "WHERE keyspace_name = '%s' AND columnfamily_name = '%s'",
                SystemKeyspace.NAME,
                LegacySchemaTables.COLUMNFAMILIES,
                keyspace,
                column_family);

        CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);

        if (result == null || result.rows == null || result.rows.isEmpty())
            return null;

        Iterator<CqlRow> iteraRow = result.rows.iterator();
        CfDef cfDef = new CfDef();
        cfDef.keyspace = keyspace;
        cfDef.name = column_family;
        if (iteraRow.hasNext())
        {
            CqlRow cqlRow = iteraRow.next();

            cfDef.column_type = ByteBufferUtil.string(cqlRow.columns.get(0).value);
            cfDef.comparator_type = ByteBufferUtil.string(cqlRow.columns.get(1).value);
            ByteBuffer subComparator = cqlRow.columns.get(2).value;
            if (subComparator != null)
                cfDef.subcomparator_type = ByteBufferUtil.string(subComparator);
            cfDef.default_validation_class = ByteBufferUtil.string(cqlRow.columns.get(3).value);
            cfDef.key_validation_class = ByteBufferUtil.string(cqlRow.columns.get(4).value);
        }
        cfDef.column_metadata = getColumnMetadata(client);
        return cfDef;
    }

    /** get the columnfamily definition for the signature */
    protected CfDef getCfDef(String signature) throws IOException
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(CassandraStorage.class);
        String prop = property.getProperty(signature);
        return cfdefFromString(prop);
    }

    /** convert string back to CfDef */
    protected static CfDef cfdefFromString(String st) throws IOException
    {
        assert st != null;
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        CfDef cfDef = new CfDef();
        try
        {
            deserializer.deserialize(cfDef, Hex.hexToBytes(st));
        }
        catch (TException e)
        {
            throw new IOException(e);
        }
        return cfDef;
    }

    /** convert CfDef to string */
    protected static String cfdefToString(CfDef cfDef) throws IOException
    {
        assert cfDef != null;
        // this is so awful it's kind of cool!
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        try
        {
            return Hex.bytesToHex(serializer.serialize(cfDef));
        }
        catch (TException e)
        {
            throw new IOException(e);
        }
    }

    /** parse the string to a cassandra data type */
    protected AbstractType parseType(String type) throws IOException
    {
        try
        {
            // always treat counters like longs, specifically CCT.compose is not what we need
            if (type != null && type.equals("org.apache.cassandra.db.marshal.CounterColumnType"))
                return LongType.instance;
            return TypeParser.parse(type);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
        catch (SyntaxException e)
        {
            throw new IOException(e);
        }
    }

    /** convert a column to a tuple */
    protected Tuple columnToTuple(Cell col, CfDef cfDef, AbstractType comparator) throws IOException
    {
        Tuple pair = TupleFactory.getInstance().newTuple(2);

        ByteBuffer colName = col.name().toByteBuffer();

        // name
        if(comparator instanceof AbstractCompositeType)
            StorageHelper.setTupleValue(pair, 0, composeComposite((AbstractCompositeType) comparator, colName));
        else
            StorageHelper.setTupleValue(pair, 0, StorageHelper.cassandraToObj(comparator, colName, nativeProtocolVersion));

        // value
        Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);
        if (validators.get(colName) == null)
        {
            Map<MarshallerType, AbstractType> marshallers = getDefaultMarshallers(cfDef);
            StorageHelper.setTupleValue(pair, 1, StorageHelper.cassandraToObj(marshallers.get(MarshallerType.DEFAULT_VALIDATOR), col.value(), nativeProtocolVersion));
        }
        else
            StorageHelper.setTupleValue(pair, 1, StorageHelper.cassandraToObj(validators.get(colName), col.value(), nativeProtocolVersion));
        return pair;
    }

    /** construct a map to store the mashaller type to cassandra data type mapping */
    protected Map<MarshallerType, AbstractType> getDefaultMarshallers(CfDef cfDef) throws IOException
    {
        Map<MarshallerType, AbstractType> marshallers = new EnumMap<MarshallerType, AbstractType>(MarshallerType.class);
        AbstractType comparator;
        AbstractType subcomparator;
        AbstractType default_validator;
        AbstractType key_validator;

        comparator = parseType(cfDef.getComparator_type());
        subcomparator = parseType(cfDef.getSubcomparator_type());
        default_validator = parseType(cfDef.getDefault_validation_class());
        key_validator = parseType(cfDef.getKey_validation_class());

        marshallers.put(MarshallerType.COMPARATOR, comparator);
        marshallers.put(MarshallerType.DEFAULT_VALIDATOR, default_validator);
        marshallers.put(MarshallerType.KEY_VALIDATOR, key_validator);
        marshallers.put(MarshallerType.SUBCOMPARATOR, subcomparator);
        return marshallers;
    }

    /** get the validators */
    protected Map<ByteBuffer, AbstractType> getValidatorMap(CfDef cfDef) throws IOException
    {
        Map<ByteBuffer, AbstractType> validators = new HashMap<ByteBuffer, AbstractType>();
        for (ColumnDef cd : cfDef.getColumn_metadata())
        {
            if (cd.getValidation_class() != null && !cd.getValidation_class().isEmpty())
            {
                AbstractType validator = null;
                try
                {
                    validator = TypeParser.parse(cd.getValidation_class());
                    if (validator instanceof CounterColumnType)
                        validator = LongType.instance;
                    validators.put(cd.name, validator);
                }
                catch (ConfigurationException e)
                {
                    throw new IOException(e);
                }
                catch (SyntaxException e)
                {
                    throw new IOException(e);
                }
            }
        }
        return validators;
    }
}
