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
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumn;
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
import org.apache.pig.impl.util.UDFContext;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

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
    public final static String PIG_ALLOW_DELETES = "PIG_ALLOW_DELETES";

    private final static ByteBuffer BOUND = ByteBufferUtil.EMPTY_BYTE_BUFFER;
    private static final Log logger = LogFactory.getLog(CassandraStorage.class);

    private ByteBuffer slice_start = BOUND;
    private ByteBuffer slice_end = BOUND;
    private boolean slice_reverse = false;
    private boolean allow_deletes = false;
    private String keyspace;
    private String column_family;
    private String loadSignature;
    private String storeSignature;

    private Configuration conf;
    private RecordReader<ByteBuffer, Map<ByteBuffer, IColumn>> reader;
    private RecordWriter<ByteBuffer, List<Mutation>> writer;
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
            ByteBuffer key = reader.getCurrentKey();
            Map<ByteBuffer, IColumn> cf = reader.getCurrentValue();
            assert key != null && cf != null;

            // output tuple, will hold the key, each indexed column in a tuple, then a bag of the rest
            Tuple tuple = TupleFactory.getInstance().newTuple();
            DefaultDataBag bag = new DefaultDataBag();
            // set the key
            tuple.append(new DataByteArray(ByteBufferUtil.getArray(key)));
            // we must add all the indexed columns first to match the schema
            Map<ByteBuffer, Boolean> added = new HashMap<ByteBuffer, Boolean>();
            // take care to iterate these in the same order as the schema does
            for (ColumnDef cdef : cfDef.column_metadata)
            {
                if (cf.containsKey(cdef.name))
                {
                    tuple.append(columnToTuple(cf.get(cdef.name), cfDef, parseType(cfDef.getComparator_type())));
                }
                else
                {   // otherwise, we need to add an empty tuple to take its place
                    tuple.append(TupleFactory.getInstance().newTuple());
                }
                added.put(cdef.name, true);
            }
            // now add all the other columns
            for (Map.Entry<ByteBuffer, IColumn> entry : cf.entrySet())
            {
                if (!added.containsKey(entry.getKey()))
                    bag.add(columnToTuple(entry.getValue(), cfDef, parseType(cfDef.getComparator_type())));
            }
            tuple.append(bag);
            return tuple;
        }
        catch (InterruptedException e)
        {
            throw new IOException(e.getMessage());
        }
    }

    private Tuple columnToTuple(IColumn col, CfDef cfDef, AbstractType comparator) throws IOException
    {
        Tuple pair = TupleFactory.getInstance().newTuple(2);
        List<AbstractType> marshallers = getDefaultMarshallers(cfDef);
        Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);

        setTupleValue(pair, 0, comparator.compose(col.name()));
        if (col instanceof Column)
        {
            // standard
            if (validators.get(col.name()) == null)
                setTupleValue(pair, 1, marshallers.get(1).compose(col.value()));
            else
                setTupleValue(pair, 1, validators.get(col.name()).compose(col.value()));
            return pair;
        }
        else
        {
            // super
            ArrayList<Tuple> subcols = new ArrayList<Tuple>();
            for (IColumn subcol : col.getSubColumns())
                subcols.add(columnToTuple(subcol, cfDef, parseType(cfDef.getSubcomparator_type())));

            pair.set(1, new DefaultDataBag(subcols));
        }
        return pair;
    }

    private void setTupleValue(Tuple pair, int position, Object value) throws ExecException
    {
       if (value instanceof BigInteger)
           pair.set(position, ((BigInteger) value).intValue());
       else if (value instanceof ByteBuffer)
           pair.set(position, new DataByteArray(ByteBufferUtil.getArray((ByteBuffer) value)));
       else if (value instanceof UUID)
           pair.set(position, new DataByteArray(UUIDGen.decompose((java.util.UUID) value)));
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
        AbstractType comparator;
        AbstractType subcomparator;
        AbstractType default_validator;
        AbstractType key_validator;
        try
        {
            comparator = TypeParser.parse(cfDef.getComparator_type());
            subcomparator = TypeParser.parse(cfDef.getSubcomparator_type());
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
        marshallers.add(subcomparator);
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

    private AbstractType parseType(String type) throws IOException
    {
        try
        {
            return TypeParser.parse(type);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
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
        if (System.getenv(PIG_ALLOW_DELETES) != null)
            allow_deletes = Boolean.valueOf(System.getenv(PIG_ALLOW_DELETES));
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
        if (loadSignature == null)
            loadSignature = location;
        initSchema(loadSignature);
    }

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
        List<AbstractType> marshallers = getDefaultMarshallers(cfDef);
        Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);

        // add key
        ResourceFieldSchema keyFieldSchema = new ResourceFieldSchema();
        keyFieldSchema.setName("key");
        keyFieldSchema.setType(getPigType(marshallers.get(2)));

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
        bagcolSchema.setType(getPigType(marshallers.get(0)));
        bagvalSchema.setType(getPigType(marshallers.get(1)));
        bagTupleSchema.setFields(new ResourceFieldSchema[] { bagcolSchema, bagvalSchema });
        bagTupleField.setSchema(bagTupleSchema);
        bagSchema.setFields(new ResourceFieldSchema[] { bagTupleField });
        bagField.setSchema(bagSchema);

        // will contain all fields for this schema
        List<ResourceFieldSchema> allSchemaFields = new ArrayList<ResourceFieldSchema>();
        // add the key first, then the indexed columns, and finally the bag
        allSchemaFields.add(keyFieldSchema);

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
            idxColSchema.setType(getPigType(marshallers.get(0)));

            ResourceFieldSchema valSchema = new ResourceFieldSchema();
            AbstractType validator = validators.get(cdef.name);
            if (validator == null)
                validator = marshallers.get(1);
            valSchema.setName("value");
            valSchema.setType(getPigType(validator));

            innerTupleSchema.setFields(new ResourceFieldSchema[] { idxColSchema, valSchema });
            allSchemaFields.add(innerTupleField);
        }
        // bag at the end for unknown columns
        allSchemaFields.add(bagField);

        // top level schema contains everything
        schema.setFields(allSchemaFields.toArray(new ResourceFieldSchema[allSchemaFields.size()]));
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

        return ByteBuffer.wrap(((DataByteArray) o).get());
    }

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
            writeColumnsFromBag(key, (DefaultDataBag) t.get(1));
        }
        else
            throw new IOException("Second argument in output must be a tuple or bag");
    }

    private void writeColumnsFromTuple(ByteBuffer key, Tuple t, int offset) throws IOException
    {
        ArrayList<Mutation> mutationList = new ArrayList<Mutation>();
        for (int i = offset; i < t.size(); i++)
        {
            if (t.getType(i) == DataType.BAG)
                writeColumnsFromBag(key, (DefaultDataBag) t.get(i));
            else if (t.getType(i) == DataType.TUPLE)
            {
                Tuple inner = (Tuple) t.get(i);
                if (inner.size() > 0) // may be empty, for an indexed column that wasn't present
                    mutationList.add(mutationFromTuple(inner));
            }
            else
                throw new IOException("Output type was not a bag or a tuple");
        }
        if (mutationList.size() > 0)
            writeMutations(key, mutationList);
    }

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
                throw new IOException("null found but deletes are disabled, set " + PIG_ALLOW_DELETES + "=true to enable");
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

    private void writeColumnsFromBag(ByteBuffer key, DefaultDataBag bag) throws IOException
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
                for (Tuple subcol : (DefaultDataBag) pair.get(1))
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
                        throw new IOException("SuperColumn deletion attempted with empty bag, but deletes are disabled, set " + PIG_ALLOW_DELETES + "=true to enable");
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
                if (cfDef != null)
                    property.setProperty(signature, cfdefToString(cfDef));
                else
                    throw new RuntimeException("Column family '" + column_family + "' not found in keyspace '" + keyspace + "'");
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

