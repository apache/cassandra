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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlRecordReader;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.Expression.OpType;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.UDFContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.external.biz.base64Coder.Base64Coder;


public class CqlNativeStorage extends LoadFunc implements StoreFuncInterface, LoadMetadata
{
    protected String DEFAULT_INPUT_FORMAT;
    protected String DEFAULT_OUTPUT_FORMAT;

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

    private static final Logger logger = LoggerFactory.getLogger(CqlNativeStorage.class);
    private int pageSize = 1000;
    private String columns;
    private String outputQuery;
    private String whereClause;

    private RecordReader<Long, Row> reader;
    private RecordWriter<Map<String, ByteBuffer>, List<ByteBuffer>> writer;
    private String nativePort;
    private String nativeCoreConnections;
    private String nativeMaxConnections;
    private String nativeMaxSimultReqs;
    private String nativeConnectionTimeout;
    private String nativeReadConnectionTimeout;
    private String nativeReceiveBufferSize;
    private String nativeSendBufferSize;
    private String nativeSolinger;
    private String nativeTcpNodelay;
    private String nativeReuseAddress;
    private String nativeKeepAlive;
    private String nativeAuthProvider;
    private String nativeSSLTruststorePath;
    private String nativeSSLKeystorePath;
    private String nativeSSLTruststorePassword;
    private String nativeSSLKeystorePassword;
    private String nativeSSLCipherSuites;
    private String inputCql;

    public CqlNativeStorage()
    {
        this(1000);
    }

    /** @param pageSize limit number of CQL rows to fetch in a thrift request */
    public CqlNativeStorage(int pageSize)
    {
        super();
        this.pageSize = pageSize;
        DEFAULT_INPUT_FORMAT = "org.apache.cassandra.hadoop.cql3.CqlInputFormat";
        DEFAULT_OUTPUT_FORMAT = "org.apache.cassandra.hadoop.cql3.CqlOutputFormat";
    }

    public void prepareToRead(RecordReader reader, PigSplit split)
    {
        this.reader = reader;
        if (reader instanceof CqlRecordReader) {
            nativeProtocolVersion = ((CqlRecordReader) reader).getNativeProtocolVersion();
        }
    }

    public void prepareToWrite(RecordWriter writer)
    {
        this.writer = writer;
    }

    /** get next row */
    public Tuple getNext() throws IOException
    {
        try
        {
            // load the next pair
            if (!reader.nextKeyValue())
                return null;

            TableInfo tableMetadata = getCfInfo(loadSignature);
            Row row = reader.getCurrentValue();
            Tuple tuple = TupleFactory.getInstance().newTuple(tableMetadata.getColumns().size());
            Iterator<ColumnInfo> itera = tableMetadata.getColumns().iterator();
            int i = 0;
            while (itera.hasNext())
            {
                ColumnInfo cdef = itera.next();
                ByteBuffer columnValue = row.getBytesUnsafe(cdef.getName());
                if (columnValue != null)
                {
                    AbstractType<?> validator = getValidatorMap(tableMetadata).get(ByteBufferUtil.bytes(cdef.getName()));
                    setTupleValue(tuple, i, cqlColumnToObj(ByteBufferUtil.bytes(cdef.getName()), columnValue,
                                                           tableMetadata), validator);
                }
                else
                    tuple.set(i, null);
                i++;
            }
            return tuple;
        }
        catch (InterruptedException e)
        {
            throw new IOException(e.getMessage());
        }
    }

    /** convert a cql column to an object */
    private Object cqlColumnToObj(ByteBuffer name, ByteBuffer columnValue, TableInfo cfDef) throws IOException
    {
        // standard
        Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);
        return StorageHelper.cassandraToObj(validators.get(name), columnValue, nativeProtocolVersion);
    }

    /** set the value to the position of the tuple */
    private void setTupleValue(Tuple tuple, int position, Object value, AbstractType<?> validator) throws ExecException
    {
        if (validator instanceof CollectionType)
            setCollectionTupleValues(tuple, position, value, validator);
        else
           StorageHelper.setTupleValue(tuple, position, value);
    }

    /** set the values of set/list at and after the position of the tuple */
    private void setCollectionTupleValues(Tuple tuple, int position, Object value, AbstractType<?> validator) throws ExecException
    {
        if (validator instanceof MapType)
        {
            setMapTupleValues(tuple, position, value, validator);
            return;
        }
        AbstractType elementValidator;
        if (validator instanceof SetType)
            elementValidator = ((SetType<?>) validator).getElementsType();
        else if (validator instanceof ListType)
            elementValidator = ((ListType<?>) validator).getElementsType();
        else
            return;

        int i = 0;
        Tuple innerTuple = TupleFactory.getInstance().newTuple(((Collection<?>) value).size());
        for (Object entry : (Collection<?>) value)
        {
            setTupleValue(innerTuple, i, cassandraToPigData(entry, elementValidator), elementValidator);
            i++;
        }
        tuple.set(position, innerTuple);
    }

    /** set the values of set/list at and after the position of the tuple */
    private void setMapTupleValues(Tuple tuple, int position, Object value, AbstractType<?> validator) throws ExecException
    {
        AbstractType<?> keyValidator = ((MapType<?, ?>) validator).getKeysType();
        AbstractType<?> valueValidator = ((MapType<?, ?>) validator).getValuesType();

        int i = 0;
        Tuple innerTuple = TupleFactory.getInstance().newTuple(((Map<?,?>) value).size());
        for(Map.Entry<?,?> entry :  ((Map<Object, Object>)value).entrySet())
        {
            Tuple mapEntryTuple = TupleFactory.getInstance().newTuple(2);
            setTupleValue(mapEntryTuple, 0, cassandraToPigData(entry.getKey(), keyValidator), keyValidator);
            setTupleValue(mapEntryTuple, 1, cassandraToPigData(entry.getValue(), valueValidator), valueValidator);
            innerTuple.set(i, mapEntryTuple);
            i++;
        }
        tuple.set(position, innerTuple);
    }

    private Object cassandraToPigData(Object obj, AbstractType validator)
    {
        if (validator instanceof DecimalType || validator instanceof InetAddressType)
            return validator.getString(validator.decompose(obj));
        return obj;
    }

    /** get the columnfamily definition for the signature */
    protected TableInfo getCfInfo(String signature) throws IOException
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(CqlNativeStorage.class);
        TableInfo cfInfo;
        try
        {
            cfInfo = cfdefFromString(property.getProperty(signature));
        }
        catch (ClassNotFoundException e)
        {
            throw new IOException(e);
        }
        return cfInfo;
    }

    /** return the CfInfo for the column family */
    protected TableMetadata getCfInfo(Session client)
            throws NoHostAvailableException,
            AuthenticationException,
            IllegalStateException
    {
        // get CF meta data
        return client.getCluster().getMetadata().getKeyspace(Metadata.quote(keyspace)).getTable(Metadata.quote(column_family));
    }

    /** output: (((name, value), (name, value)), (value ... value), (value...value)) */
    public void putNext(Tuple t) throws IOException
    {
        if (t.size() < 1)
        {
            // simply nothing here, we can't even delete without a key
            logger.warn("Empty output skipped, filter empty tuples to suppress this warning");
            return;
        }

        if (t.getType(0) == DataType.TUPLE)
        {
            if (t.getType(1) == DataType.TUPLE)
            {
                Map<String, ByteBuffer> key = tupleToKeyMap((Tuple)t.get(0));
                cqlQueryFromTuple(key, t, 1);
            }
            else
                throw new IOException("Second argument in output must be a tuple");
        }
        else
            throw new IOException("First argument in output must be a tuple");
    }

    /** convert key tuple to key map */
    private Map<String, ByteBuffer> tupleToKeyMap(Tuple t) throws IOException
    {
        Map<String, ByteBuffer> keys = new HashMap<String, ByteBuffer>();
        for (int i = 0; i < t.size(); i++)
        {
            if (t.getType(i) == DataType.TUPLE)
            {
                Tuple inner = (Tuple) t.get(i);
                if (inner.size() == 2)
                {
                    Object name = inner.get(0);
                    if (name != null)
                    {
                        keys.put(name.toString(), objToBB(inner.get(1)));
                    }
                    else
                        throw new IOException("Key name was empty");
                }
                else
                    throw new IOException("Keys were not in name and value pairs");
            }
            else
            {
                throw new IOException("keys was not a tuple");
            }
        }
        return keys;
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
        return CollectionSerializer.pack(serialized, objects.size(), 3);
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
        return CollectionSerializer.pack(serialized, objects.size(), 3);
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

    /** send CQL query request using data from tuple */
    private void cqlQueryFromTuple(Map<String, ByteBuffer> key, Tuple t, int offset) throws IOException
    {
        for (int i = offset; i < t.size(); i++)
        {
            if (t.getType(i) == DataType.TUPLE)
            {
                Tuple inner = (Tuple) t.get(i);
                if (inner.size() > 0)
                {
                    List<ByteBuffer> bindedVariables = bindedVariablesFromTuple(inner);
                    if (bindedVariables.size() > 0)
                        sendCqlQuery(key, bindedVariables);
                    else
                        throw new IOException("Missing binded variables");
                }
            }
            else
            {
                throw new IOException("Output type was not a tuple");
            }
        }
    }

    /** compose a list of binded variables */
    private List<ByteBuffer> bindedVariablesFromTuple(Tuple t) throws IOException
    {
        List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
        for (int i = 0; i < t.size(); i++)
            variables.add(objToBB(t.get(i)));
        return variables;
    }

    /** writer write the data by executing CQL query */
    private void sendCqlQuery(Map<String, ByteBuffer> key, List<ByteBuffer> bindedVariables) throws IOException
    {
        try
        {
            writer.write(key, bindedVariables);
        }
        catch (InterruptedException e)
        {
            throw new IOException(e);
        }
    }

    /** get the validators */
    protected Map<ByteBuffer, AbstractType> getValidatorMap(TableInfo cfDef) throws IOException
    {
        Map<ByteBuffer, AbstractType> validators = new HashMap<>();
        for (ColumnInfo cd : cfDef.getColumns())
        {
            if (cd.getTypeName() != null)
            {
                try
                {
                    AbstractType validator = TypeParser.parseCqlName(cd.getTypeName());
                    if (validator instanceof CounterColumnType)
                        validator = LongType.instance;
                    validators.put(ByteBufferUtil.bytes(cd.getName()), validator);
                }
                catch (ConfigurationException | SyntaxException e)
                {
                    throw new IOException(e);
                }
            }
        }
        return validators;
    }

    /** schema: (value, value, value) where keys are in the front. */
    public ResourceSchema getSchema(String location, Job job) throws IOException
    {
        setLocation(location, job);
        TableInfo cfInfo = getCfInfo(loadSignature);
        // top-level schema, no type
        ResourceSchema schema = new ResourceSchema();

        // get default validators
        Map<ByteBuffer, AbstractType> validators = getValidatorMap(cfInfo);

        // will contain all fields for this schema
        List<ResourceFieldSchema> allSchemaFields = new ArrayList<ResourceFieldSchema>();

        for (ColumnInfo cdef : cfInfo.getColumns())
        {
            ResourceFieldSchema valSchema = new ResourceFieldSchema();
            AbstractType<?> validator = validators.get(ByteBufferUtil.bytes(cdef.getName()));
            valSchema.setName(cdef.getName());
            valSchema.setType(StorageHelper.getPigType(validator));
            allSchemaFields.add(valSchema);
        }

        // top level schema contains everything
        schema.setFields(allSchemaFields.toArray(new ResourceFieldSchema[allSchemaFields.size()]));
        return schema;
    }

    public void setPartitionFilter(Expression partitionFilter) throws IOException
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(CqlNativeStorage.class);
        property.setProperty(StorageHelper.PARTITION_FILTER_SIGNATURE, partitionFilterToWhereClauseString(partitionFilter));
    }

    /**
     * Return cql where clauses for the corresponding partition filter. Make sure the data format matches
     * Only support the following Pig data types: int, long, float, double, boolean and chararray
     * */
    private String partitionFilterToWhereClauseString(Expression expression) throws IOException
    {
        Expression.BinaryExpression be = (Expression.BinaryExpression) expression;
        OpType op = expression.getOpType();
        String opString = op.toString();
        switch (op)
        {
            case OP_EQ:
                opString = " = ";
            case OP_GE:
            case OP_GT:
            case OP_LE:
            case OP_LT:
                String name = be.getLhs().toString();
                String value = be.getRhs().toString();
                return String.format("%s %s %s", name, opString, value);
            case OP_AND:
                return String.format("%s AND %s", partitionFilterToWhereClauseString(be.getLhs()), partitionFilterToWhereClauseString(be.getRhs()));
            default:
                throw new IOException("Unsupported expression type: " + opString);
        }
    }

    /** retrieve where clause for partition filter */
    private String getWhereClauseForPartitionFilter()
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(CqlNativeStorage.class);
        return property.getProperty(StorageHelper.PARTITION_FILTER_SIGNATURE);
    }

    /** set read configuration settings */
    public void setLocation(String location, Job job) throws IOException
    {
        conf = job.getConfiguration();
        setLocationFromUri(location);

        if (username != null && password != null)
        {
            ConfigHelper.setInputKeyspaceUserNameAndPassword(conf, username, password);
            CqlConfigHelper.setUserNameAndPassword(conf, username, password);
        }
        if (splitSize > 0)
            ConfigHelper.setInputSplitSize(conf, splitSize);
        if (partitionerClass!= null)
            ConfigHelper.setInputPartitioner(conf, partitionerClass);
        if (initHostAddress != null)
            ConfigHelper.setInputInitialAddress(conf, initHostAddress);
        if (rpcPort != null)
            ConfigHelper.setInputRpcPort(conf, rpcPort);
        if (nativePort != null)
            CqlConfigHelper.setInputNativePort(conf, nativePort);
        if (nativeCoreConnections != null)
            CqlConfigHelper.setInputCoreConnections(conf, nativeCoreConnections);
        if (nativeMaxConnections != null)
            CqlConfigHelper.setInputMaxConnections(conf, nativeMaxConnections);
        if (nativeMaxSimultReqs != null)
            CqlConfigHelper.setInputMaxSimultReqPerConnections(conf, nativeMaxSimultReqs);
        if (nativeConnectionTimeout != null)
            CqlConfigHelper.setInputNativeConnectionTimeout(conf, nativeConnectionTimeout);
        if (nativeReadConnectionTimeout != null)
            CqlConfigHelper.setInputNativeReadConnectionTimeout(conf, nativeReadConnectionTimeout);
        if (nativeReceiveBufferSize != null)
            CqlConfigHelper.setInputNativeReceiveBufferSize(conf, nativeReceiveBufferSize);
        if (nativeSendBufferSize != null)
            CqlConfigHelper.setInputNativeSendBufferSize(conf, nativeSendBufferSize);
        if (nativeSolinger != null)
            CqlConfigHelper.setInputNativeSolinger(conf, nativeSolinger);
        if (nativeTcpNodelay != null)
            CqlConfigHelper.setInputNativeTcpNodelay(conf, nativeTcpNodelay);
        if (nativeReuseAddress != null)
            CqlConfigHelper.setInputNativeReuseAddress(conf, nativeReuseAddress);
        if (nativeKeepAlive != null)
            CqlConfigHelper.setInputNativeKeepAlive(conf, nativeKeepAlive);
        if (nativeAuthProvider != null)
            CqlConfigHelper.setInputNativeAuthProvider(conf, nativeAuthProvider);
        if (nativeSSLTruststorePath != null)
            CqlConfigHelper.setInputNativeSSLTruststorePath(conf, nativeSSLTruststorePath);
        if (nativeSSLKeystorePath != null)
            CqlConfigHelper.setInputNativeSSLKeystorePath(conf, nativeSSLKeystorePath);
        if (nativeSSLTruststorePassword != null)
            CqlConfigHelper.setInputNativeSSLTruststorePassword(conf, nativeSSLTruststorePassword);
        if (nativeSSLKeystorePassword != null)
            CqlConfigHelper.setInputNativeSSLKeystorePassword(conf, nativeSSLKeystorePassword);
        if (nativeSSLCipherSuites != null)
            CqlConfigHelper.setInputNativeSSLCipherSuites(conf, nativeSSLCipherSuites);

        ConfigHelper.setInputColumnFamily(conf, keyspace, column_family);
        setConnectionInformation();

        CqlConfigHelper.setInputCQLPageRowSize(conf, String.valueOf(pageSize));
        if (inputCql != null)
            CqlConfigHelper.setInputCql(conf, inputCql);
        if (columns != null)
            CqlConfigHelper.setInputColumns(conf, columns);
        if (whereClause != null)
            CqlConfigHelper.setInputWhereClauses(conf, whereClause);

        String whereClauseForPartitionFilter = getWhereClauseForPartitionFilter();
        String wc = whereClause != null && !whereClause.trim().isEmpty()
                               ? whereClauseForPartitionFilter == null ? whereClause: String.format("%s AND %s", whereClause.trim(), whereClauseForPartitionFilter)
                               : whereClauseForPartitionFilter;

        if (wc != null)
        {
            logger.debug("where clause: {}", wc);
            CqlConfigHelper.setInputWhereClauses(conf, wc);
        }
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
        CqlConfigHelper.setOutputCql(conf, outputQuery);

        setConnectionInformation();

        if (ConfigHelper.getOutputRpcPort(conf) == 0)
            throw new IOException("PIG_OUTPUT_RPC_PORT or PIG_RPC_PORT environment variable not set");
        if (ConfigHelper.getOutputInitialAddress(conf) == null)
            throw new IOException("PIG_OUTPUT_INITIAL_ADDRESS or PIG_INITIAL_ADDRESS environment variable not set");
        if (ConfigHelper.getOutputPartitioner(conf) == null)
            throw new IOException("PIG_OUTPUT_PARTITIONER or PIG_PARTITIONER environment variable not set");

        initSchema(storeSignature);
    }

    /** Methods to get the column family schema from Cassandra */
    protected void initSchema(String signature) throws IOException
    {
        Properties properties = UDFContext.getUDFContext().getUDFProperties(CqlNativeStorage.class);

        // Only get the schema if we haven't already gotten it
        if (!properties.containsKey(signature))
        {
            try (Session client = CqlConfigHelper.getInputCluster(ConfigHelper.getInputInitialAddress(conf), conf).connect())
            {
                client.execute("USE " + keyspace);

                // compose the CfDef for the columfamily
                TableMetadata cfInfo = getCfInfo(client);

                if (cfInfo != null)
                {
                    properties.setProperty(signature, cfdefToString(cfInfo));
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


    /** convert CfDef to string */
    protected static String cfdefToString(TableMetadata cfDef) throws IOException
    {
        TableInfo tableInfo = new TableInfo(cfDef);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream( baos ))
        {
            oos.writeObject(tableInfo);
        }

        return new String( Base64Coder.encode(baos.toByteArray()) );
    }

    /** convert string back to CfDef */
    protected static TableInfo cfdefFromString(String st) throws IOException, ClassNotFoundException
    {
        byte [] data = Base64Coder.decode( st );
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data)))
        {
            Object o = ois.readObject();
            return (TableInfo)o;
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

    private void setLocationFromUri(String location) throws IOException
    {
        try
        {
            if (!location.startsWith("cql://"))
                throw new Exception("Bad scheme: " + location);

            String[] urlParts = location.split("\\?");
            if (urlParts.length > 1)
            {
                Map<String, String> urlQuery = getQueryMap(urlParts[1]);

                // each page row size
                if (urlQuery.containsKey("page_size"))
                    pageSize = Integer.parseInt(urlQuery.get("page_size"));

                // output prepared statement
                if (urlQuery.containsKey("output_query"))
                    outputQuery = urlQuery.get("output_query");

                //split size
                if (urlQuery.containsKey("split_size"))
                    splitSize = Integer.parseInt(urlQuery.get("split_size"));
                if (urlQuery.containsKey("partitioner"))
                    partitionerClass = urlQuery.get("partitioner");
                if (urlQuery.containsKey("use_secondary"))
                    usePartitionFilter = Boolean.parseBoolean(urlQuery.get("use_secondary"));
                if (urlQuery.containsKey("init_address"))
                    initHostAddress = urlQuery.get("init_address");

                if (urlQuery.containsKey("native_port"))
                    nativePort = urlQuery.get("native_port");
                if (urlQuery.containsKey("core_conns"))
                    nativeCoreConnections = urlQuery.get("core_conns");
                if (urlQuery.containsKey("max_conns"))
                    nativeMaxConnections = urlQuery.get("max_conns");
                if (urlQuery.containsKey("max_simult_reqs"))
                    nativeMaxSimultReqs = urlQuery.get("max_simult_reqs");
                if (urlQuery.containsKey("native_timeout"))
                    nativeConnectionTimeout = urlQuery.get("native_timeout");
                if (urlQuery.containsKey("native_read_timeout"))
                    nativeReadConnectionTimeout = urlQuery.get("native_read_timeout");
                if (urlQuery.containsKey("rec_buff_size"))
                    nativeReceiveBufferSize = urlQuery.get("rec_buff_size");
                if (urlQuery.containsKey("send_buff_size"))
                    nativeSendBufferSize = urlQuery.get("send_buff_size");
                if (urlQuery.containsKey("solinger"))
                    nativeSolinger = urlQuery.get("solinger");
                if (urlQuery.containsKey("tcp_nodelay"))
                    nativeTcpNodelay = urlQuery.get("tcp_nodelay");
                if (urlQuery.containsKey("reuse_address"))
                    nativeReuseAddress = urlQuery.get("reuse_address");
                if (urlQuery.containsKey("keep_alive"))
                    nativeKeepAlive = urlQuery.get("keep_alive");
                if (urlQuery.containsKey("auth_provider"))
                    nativeAuthProvider = urlQuery.get("auth_provider");
                if (urlQuery.containsKey("trust_store_path"))
                    nativeSSLTruststorePath = urlQuery.get("trust_store_path");
                if (urlQuery.containsKey("key_store_path"))
                    nativeSSLKeystorePath = urlQuery.get("key_store_path");
                if (urlQuery.containsKey("trust_store_password"))
                    nativeSSLTruststorePassword = urlQuery.get("trust_store_password");
                if (urlQuery.containsKey("key_store_password"))
                    nativeSSLKeystorePassword = urlQuery.get("key_store_password");
                if (urlQuery.containsKey("cipher_suites"))
                    nativeSSLCipherSuites = urlQuery.get("cipher_suites");
                if (urlQuery.containsKey("input_cql"))
                    inputCql = urlQuery.get("input_cql");
                if (urlQuery.containsKey("columns"))
                    columns = urlQuery.get("columns");
                if (urlQuery.containsKey("where_clause"))
                    whereClause = urlQuery.get("where_clause");
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
            throw new IOException("Expected 'cql://[username:password@]<keyspace>/<columnfamily>" +
                    "[?[page_size=<size>][&columns=<col1,col2>][&output_query=<prepared_statement>]" +
                    "[&where_clause=<clause>][&split_size=<size>][&partitioner=<partitioner>][&use_secondary=true|false]" +
                    "[&init_address=<host>][&native_port=<native_port>][&core_conns=<core_conns>]" +
                    "[&max_conns=<max_conns>][&min_simult_reqs=<min_simult_reqs>][&max_simult_reqs=<max_simult_reqs>]" +
                    "[&native_timeout=<native_timeout>][&native_read_timeout=<native_read_timeout>][&rec_buff_size=<rec_buff_size>]" +
                    "[&send_buff_size=<send_buff_size>][&solinger=<solinger>][&tcp_nodelay=<tcp_nodelay>][&reuse_address=<reuse_address>]" +
                    "[&keep_alive=<keep_alive>][&auth_provider=<auth_provider>][&trust_store_path=<trust_store_path>]" +
                    "[&key_store_path=<key_store_path>][&trust_store_password=<trust_store_password>]" +
                    "[&key_store_password=<key_store_password>][&cipher_suites=<cipher_suites>][&input_cql=<input_cql>]" +
                    "[columns=<columns>][where_clause=<where_clause>]]': " + e.getMessage());
        }
    }

    public ByteBuffer nullToBB()
    {
        return ByteBuffer.wrap(new byte[0]);
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

    public void cleanupOnFailure(String failure, Job job)
    {
    }

    public void cleanupOnSuccess(String location, Job job) throws IOException {
    }

    /** return partition keys */
    public String[] getPartitionKeys(String location, Job job) throws IOException
    {
        if (!usePartitionFilter)
            return null;
        TableInfo tableMetadata = getCfInfo(loadSignature);
        String[] partitionKeys = new String[tableMetadata.getPartitionKey().size()];
        for (int i = 0; i < tableMetadata.getPartitionKey().size(); i++)
        {
            partitionKeys[i] = tableMetadata.getPartitionKey().get(i).getName();
        }
        return partitionKeys;
    }

    public void checkSchema(ResourceSchema schema) throws IOException
    {
        // we don't care about types, they all get casted to ByteBuffers
    }

    public ResourceStatistics getStatistics(String location, Job job)
    {
        return null;
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

    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException
    {
        return relativeToAbsolutePath(location, curDir);
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

    /** StoreFunc methods */
    public void setStoreFuncUDFContextSignature(String signature)
    {
        this.storeSignature = signature;
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
    }

    /** get the full class name */
    protected String getFullyQualifiedClassName(String classname)
    {
        return classname.contains(".") ? classname : "org.apache.cassandra.hadoop." + classname;
    }
}

class TableInfo implements Serializable
{
    private final List<ColumnInfo> columns;
    private final List<ColumnInfo> partitionKey;
    private final String name;

    public TableInfo(TableMetadata tableMetadata)
    {
        List<ColumnMetadata> cmColumns = tableMetadata.getColumns();
        columns = new ArrayList<>(cmColumns.size());
        for (ColumnMetadata cm : cmColumns)
        {
            columns.add(new ColumnInfo(this, cm));
        }
        List<ColumnMetadata> cmPartitionKey = tableMetadata.getPartitionKey();
        partitionKey = new ArrayList<>(cmPartitionKey.size());
        for (ColumnMetadata cm : cmPartitionKey)
        {
            partitionKey.add(new ColumnInfo(this, cm));
        }
        name = tableMetadata.getName();
    }

    public List<ColumnInfo> getPartitionKey()
    {
        return partitionKey;
    }

    public List<ColumnInfo> getColumns()
    {
        return columns;
    }

    public String getName()
    {
        return name;
    }
}

class ColumnInfo implements Serializable
{
    private final TableInfo table;
    private final String name;
    private final String typeName;

    public ColumnInfo(TableInfo tableInfo, ColumnMetadata columnMetadata)
    {
        table = tableInfo;
        name = columnMetadata.getName();
        typeName = columnMetadata.getType().toString();
    }

    public String getName()
    {
        return name;
    }

    public String getTypeName()
    {
        return typeName;
    }
}