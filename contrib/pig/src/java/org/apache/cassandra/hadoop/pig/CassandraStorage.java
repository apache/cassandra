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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.marshal.*;
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
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
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
public class CassandraStorage extends LoadFunc implements StoreFuncInterface, LoadMetadata
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

        setTupleValue(pair, 0, marshallers.get(0).compose(name));
        if (col instanceof Column)
        {
            // standard
            if (validators.get(name) == null)
                setTupleValue(pair, 1, marshallers.get(1).compose(col.value()));
            else
                setTupleValue(pair, 1, validators.get(name).compose(col.value()));
            return pair;
        }

        // super
        ArrayList<Tuple> subcols = new ArrayList<Tuple>();
        for (IColumn subcol : col.getSubColumns())
            subcols.add(columnToTuple(subcol.name(), subcol, cfDef));
        
        pair.set(1, new DefaultDataBag(subcols));
        return pair;
    }

    private void setTupleValue(Tuple pair, int position, Object value) throws ExecException
    {
       if (value instanceof BigInteger)
           pair.set(position, ((BigInteger) value).intValue());
       else if (value instanceof ByteBuffer)
           pair.set(position, new DataByteArray(ByteBufferUtil.getArray((ByteBuffer) value)));
       else
           pair.set(position, value);
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
        AbstractType key_validator = null;
        try
        {
            comparator = TypeParser.parse(cfDef.getComparator_type());
            default_validator = TypeParser.parse(cfDef.getDefault_validation_class());
            key_validator = TypeParser.parse(cfDef.getKey_validation_class());
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }

        marshallers.add(comparator);
        marshallers.add(default_validator);
        marshallers.add(key_validator);
        return marshallers;
    }

    private Map<ByteBuffer, AbstractType> getValidatorMap(CfDef cfDef) throws IOException
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

    public static Map<String, String> getQueryMap(String query)
    {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params)
        {
            String[] keyValue = param.split("=");
            map.put(keyValue[0], keyValue[1]);
        }
        return map;
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

    public ResourceSchema getSchema(String location, Job job) throws IOException
    {
        setLocation(location, job);
        CfDef cfDef = getCfDef(loadSignature);

        if (cfDef.column_type.equals("Super"))
            return null;
        // top-level schema, no type
        ResourceSchema schema = new ResourceSchema();

        // get default marshallers and validators
        List<AbstractType> marshallers = getDefaultMarshallers(cfDef);
        Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);

        // add key
        ResourceFieldSchema keyFieldSchema = new ResourceFieldSchema();
        keyFieldSchema.setName("key");
        keyFieldSchema.setType(getPigType(marshallers.get(2)));

        // will become the bag of tuples
        ResourceFieldSchema bagFieldSchema = new ResourceFieldSchema();
        bagFieldSchema.setName("columns");
        bagFieldSchema.setType(DataType.BAG);
        ResourceSchema bagSchema = new ResourceSchema();

        List<ResourceFieldSchema> tupleFields = new ArrayList<ResourceFieldSchema>();

        // default comparator/validator
        ResourceSchema innerTupleSchema = new ResourceSchema();
        ResourceFieldSchema tupleField = new ResourceFieldSchema();
        tupleField.setType(DataType.TUPLE);
        tupleField.setSchema(innerTupleSchema);

        ResourceFieldSchema colSchema = new ResourceFieldSchema();
        colSchema.setName("name");
        colSchema.setType(getPigType(marshallers.get(0)));
        tupleFields.add(colSchema);

        ResourceFieldSchema valSchema = new ResourceFieldSchema();
        AbstractType validator = marshallers.get(1);
        valSchema.setName("value");
        valSchema.setType(getPigType(validator));
        tupleFields.add(valSchema);

        // defined validators/indexes
        for (ColumnDef cdef : cfDef.column_metadata)
        {
            colSchema = new ResourceFieldSchema();
            colSchema.setName(new String(cdef.getName()));
            colSchema.setType(getPigType(marshallers.get(0)));
            tupleFields.add(colSchema);

            valSchema = new ResourceFieldSchema();
            validator = validators.get(cdef.getName());
            if (validator == null)
                validator = marshallers.get(1);
            valSchema.setName("value");
            valSchema.setType(getPigType(validator));
            tupleFields.add(valSchema);
        }
        innerTupleSchema.setFields(tupleFields.toArray(new ResourceFieldSchema[tupleFields.size()]));

        // a bag can contain only one tuple, but that tuple can contain anything
        bagSchema.setFields(new ResourceFieldSchema[] { tupleField });
        bagFieldSchema.setSchema(bagSchema);
        // top level schema contains everything
        schema.setFields(new ResourceFieldSchema[] { keyFieldSchema, bagFieldSchema });
        return schema;
    }

    private byte getPigType(AbstractType type)
    {
        if (type instanceof LongType)
            return DataType.LONG;
        else if (type instanceof IntegerType)
            return DataType.INTEGER;
        else if (type instanceof AsciiType)
            return DataType.CHARARRAY;
        else if (type instanceof UTF8Type)
            return DataType.CHARARRAY;
        else if (type instanceof FloatType)
            return DataType.FLOAT;
        else if (type instanceof DoubleType)
            return DataType.DOUBLE;
        return DataType.BYTEARRAY;
    }

    public ResourceStatistics getStatistics(String location, Job job)
    {
        return null;
    }

    public String[] getPartitionKeys(String location, Job job)
    {
        return null;
    }

    public void setPartitionFilter(Expression partitionFilter)
    {
        // no-op
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
                       column.name = objToBB(pair.get(0));
                       column.value = objToBB(pair.get(1));
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
                client = ConfigHelper.getClientFromAddressList(conf);
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

    private static String cfdefToString(CfDef cfDef)
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
            deserializer.deserialize(cfDef, Hex.hexToBytes(st));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return cfDef;
    }
}

