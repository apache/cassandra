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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlRecordReader;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.Expression;
import org.apache.pig.ResourceSchema;
import org.apache.pig.Expression.OpType;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.UDFContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

public class CqlNativeStorage extends AbstractCassandraStorage
{
    private static final Logger logger = LoggerFactory.getLogger(CqlNativeStorage.class);
    private int pageSize = 1000;
    private String columns;
    private String outputQuery;
    private String whereClause;
    private boolean hasCompactValueAlias = false;

    private RecordReader<Long, Row> reader;
    private RecordWriter<Map<String, ByteBuffer>, List<ByteBuffer>> writer;
    private String nativePort;
    private String nativeCoreConnections;
    private String nativeMaxConnections;
    private String nativeMinSimultReqs;
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

            CfInfo cfInfo = getCfInfo(loadSignature);
            CfDef cfDef = cfInfo.cfDef;
            Row row = reader.getCurrentValue();
            Tuple tuple = TupleFactory.getInstance().newTuple(cfDef.column_metadata.size());
            Iterator<ColumnDef> itera = cfDef.column_metadata.iterator();
            int i = 0;
            while (itera.hasNext())
            {
                ColumnDef cdef = itera.next();
                ByteBuffer columnValue = row.getBytesUnsafe(ByteBufferUtil.string(cdef.name.duplicate()));
                if (columnValue != null)
                {
                    Cell cell = new BufferCell(CellNames.simpleDense(cdef.name), columnValue);
                    AbstractType<?> validator = getValidatorMap(cfDef).get(cdef.name);
                    setTupleValue(tuple, i, cqlColumnToObj(cell, cfDef), validator);
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
    private Object cqlColumnToObj(Cell col, CfDef cfDef) throws IOException
    {
        // standard
        Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);
        ByteBuffer cellName = col.name().toByteBuffer();
        if (validators.get(cellName) == null)
            return cassandraToObj(getDefaultMarshallers(cfDef).get(MarshallerType.DEFAULT_VALIDATOR), col.value());
        else
            return cassandraToObj(validators.get(cellName), col.value());
    }

    /** set the value to the position of the tuple */
    private void setTupleValue(Tuple tuple, int position, Object value, AbstractType<?> validator) throws ExecException
    {
        if (validator instanceof CollectionType)
            setCollectionTupleValues(tuple, position, value, validator);
        else
           setTupleValue(tuple, position, value);
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

    /** include key columns */
    protected List<ColumnDef> getColumnMetadata(Cassandra.Client client)
            throws InvalidRequestException,
            UnavailableException,
            TimedOutException,
            SchemaDisagreementException,
            TException,
            CharacterCodingException,
            org.apache.cassandra.exceptions.InvalidRequestException,
            ConfigurationException,
            NotFoundException
    {
        List<ColumnDef> keyColumns = null;
        // get key columns
        try
        {
            keyColumns = getKeysMeta(client);
        }
        catch(Exception e)
        {
            logger.error("Error in retrieving key columns" , e);
        }

        // get other columns
        List<ColumnDef> columns = getColumnMeta(client, false, !hasCompactValueAlias);

        // combine all columns in a list
        if (keyColumns != null && columns != null)
            keyColumns.addAll(columns);

        return keyColumns;
    }

    /** get keys meta data */
    private List<ColumnDef> getKeysMeta(Cassandra.Client client)
            throws Exception
    {
        String query = "SELECT key_aliases, " +
                "       column_aliases, " +
                "       key_validator, " +
                "       comparator, " +
                "       keyspace_name, " +
                "       value_alias, " +
                "       default_validator " +
                "FROM system.schema_columnfamilies " +
                "WHERE keyspace_name = '%s'" +
                "  AND columnfamily_name = '%s' ";

        CqlResult result = client.execute_cql3_query(
                ByteBufferUtil.bytes(String.format(query, keyspace, column_family)),
                Compression.NONE,
                ConsistencyLevel.ONE);

        if (result == null || result.rows == null || result.rows.isEmpty())
            return null;

        Iterator<CqlRow> iteraRow = result.rows.iterator();
        List<ColumnDef> keys = new ArrayList<ColumnDef>();
        if (iteraRow.hasNext())
        {
            CqlRow cqlRow = iteraRow.next();
            String name = ByteBufferUtil.string(cqlRow.columns.get(4).value);
            logger.debug("Found ksDef name: {}", name);
            String keyString = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(0).getValue()));

            logger.debug("partition keys: {}", keyString);
            List<String> keyNames = FBUtilities.fromJsonList(keyString);

            Iterator<String> iterator = keyNames.iterator();
            while (iterator.hasNext())
            {
                ColumnDef cDef = new ColumnDef();
                cDef.name = ByteBufferUtil.bytes(iterator.next());
                keys.add(cDef);
            }
            // classic thrift tables
            if (keys.size() == 0)
            {
                CFMetaData cfm = getCFMetaData(keyspace, column_family, client);
                for (ColumnDefinition def : cfm.partitionKeyColumns())
                {
                    String key = def.name.toString();
                    logger.debug("name: {} ", key);
                    ColumnDef cDef = new ColumnDef();
                    cDef.name = ByteBufferUtil.bytes(key);
                    keys.add(cDef);
                }
                for (ColumnDefinition def : cfm.clusteringColumns())
                {
                    String key = def.name.toString();
                    logger.debug("name: {} ", key);
                    ColumnDef cDef = new ColumnDef();
                    cDef.name = ByteBufferUtil.bytes(key);
                    keys.add(cDef);
                }
            }

            keyString = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(1).getValue()));

            logger.debug("cluster keys: {}", keyString);
            keyNames = FBUtilities.fromJsonList(keyString);

            iterator = keyNames.iterator();
            while (iterator.hasNext())
            {
                ColumnDef cDef = new ColumnDef();
                cDef.name = ByteBufferUtil.bytes(iterator.next());
                keys.add(cDef);
            }

            String validator = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(2).getValue()));
            logger.debug("row key validator: {}", validator);
            AbstractType<?> keyValidator = parseType(validator);

            Iterator<ColumnDef> keyItera = keys.iterator();
            if (keyValidator instanceof CompositeType)
            {
                Iterator<AbstractType<?>> typeItera = ((CompositeType) keyValidator).types.iterator();
                while (typeItera.hasNext())
                    keyItera.next().validation_class = typeItera.next().toString();
            }
            else
                keyItera.next().validation_class = keyValidator.toString();

            validator = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(3).getValue()));
            logger.debug("cluster key validator: {}", validator);

            if (keyItera.hasNext() && validator != null && !validator.isEmpty())
            {
                AbstractType<?> clusterKeyValidator = parseType(validator);

                if (clusterKeyValidator instanceof CompositeType)
                {
                    Iterator<AbstractType<?>> typeItera = ((CompositeType) clusterKeyValidator).types.iterator();
                    while (keyItera.hasNext())
                        keyItera.next().validation_class = typeItera.next().toString();
                }
                else
                    keyItera.next().validation_class = clusterKeyValidator.toString();
            }

            // compact value_alias column
            if (cqlRow.columns.get(5).value != null)
            {
                try
                {
                    String compactValidator = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(6).getValue()));
                    logger.debug("default validator: {}", compactValidator);
                    AbstractType<?> defaultValidator = parseType(compactValidator);

                    ColumnDef cDef = new ColumnDef();
                    cDef.name = cqlRow.columns.get(5).value;
                    cDef.validation_class = defaultValidator.toString();
                    keys.add(cDef);
                    hasCompactValueAlias = true;
                }
                catch (Exception e)
                {
                    // no compact column at value_alias
                }
            }

        }
        return keys;
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

    /** schema: (value, value, value) where keys are in the front. */
    public ResourceSchema getSchema(String location, Job job) throws IOException
    {
        setLocation(location, job);
        CfInfo cfInfo = getCfInfo(loadSignature);
        CfDef cfDef = cfInfo.cfDef;
        // top-level schema, no type
        ResourceSchema schema = new ResourceSchema();

        // get default marshallers and validators
        Map<MarshallerType, AbstractType> marshallers = getDefaultMarshallers(cfDef);
        Map<ByteBuffer, AbstractType> validators = getValidatorMap(cfDef);

        // will contain all fields for this schema
        List<ResourceFieldSchema> allSchemaFields = new ArrayList<ResourceFieldSchema>();

        for (ColumnDef cdef : cfDef.column_metadata)
        {
            ResourceFieldSchema valSchema = new ResourceFieldSchema();
            AbstractType validator = validators.get(cdef.name);
            if (validator == null)
                validator = marshallers.get(MarshallerType.DEFAULT_VALIDATOR);
            valSchema.setName(new String(cdef.getName()));
            valSchema.setType(getPigType(validator));
            allSchemaFields.add(valSchema);
        }

        // top level schema contains everything
        schema.setFields(allSchemaFields.toArray(new ResourceFieldSchema[allSchemaFields.size()]));
        return schema;
    }

    public void setPartitionFilter(Expression partitionFilter) throws IOException
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(AbstractCassandraStorage.class);
        property.setProperty(PARTITION_FILTER_SIGNATURE, partitionFilterToWhereClauseString(partitionFilter));
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
        Properties property = context.getUDFProperties(AbstractCassandraStorage.class);
        return property.getProperty(PARTITION_FILTER_SIGNATURE);
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
        if (nativeMinSimultReqs != null)
            CqlConfigHelper.setInputMinSimultReqPerConnections(conf, nativeMinSimultReqs);
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
        if (System.getenv(PIG_INPUT_SPLIT_SIZE) != null)
        {
            try
            {
                ConfigHelper.setInputSplitSize(conf, Integer.parseInt(System.getenv(PIG_INPUT_SPLIT_SIZE)));
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
                if (urlQuery.containsKey("min_simult_reqs"))
                    nativeMinSimultReqs = urlQuery.get("min_simult_reqs");
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

    /**
     * Thrift API can't handle null, so use empty byte array
     */
    public ByteBuffer nullToBB()
    {
        return ByteBuffer.wrap(new byte[0]);
    }
}
