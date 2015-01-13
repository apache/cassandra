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
import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.marshal.AbstractCompositeType.CompositeComponent;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.UUIDGen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.UDFContext;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A LoadStoreFunc for retrieving data from and storing data to Cassandra
 */
public abstract class AbstractCassandraStorage extends LoadFunc implements StoreFuncInterface, LoadMetadata
{

    protected enum MarshallerType { COMPARATOR, DEFAULT_VALIDATOR, KEY_VALIDATOR, SUBCOMPARATOR };

    // system environment variables that can be set to configure connection info:
    // alternatively, Hadoop JobConf variables can be set using keys from ConfigHelper
    public final static String PIG_INPUT_RPC_PORT = "PIG_INPUT_RPC_PORT";
    public final static String PIG_INPUT_INITIAL_ADDRESS = "PIG_INPUT_INITIAL_ADDRESS";
    public final static String PIG_INPUT_PARTITIONER = "PIG_INPUT_PARTITIONER";
    public final static String PIG_OUTPUT_RPC_PORT = "PIG_OUTPUT_RPC_PORT";
    public final static String PIG_OUTPUT_INITIAL_ADDRESS = "PIG_OUTPUT_INITIAL_ADDRESS";
    public final static String PIG_OUTPUT_PARTITIONER = "PIG_OUTPUT_PARTITIONER";
    public final static String PIG_RPC_PORT = "PIG_RPC_PORT";
    public final static String PIG_INITIAL_ADDRESS = "PIG_INITIAL_ADDRESS";
    public final static String PIG_PARTITIONER = "PIG_PARTITIONER";
    public final static String PIG_INPUT_FORMAT = "PIG_INPUT_FORMAT";
    public final static String PIG_OUTPUT_FORMAT = "PIG_OUTPUT_FORMAT";
    public final static String PIG_INPUT_SPLIT_SIZE = "PIG_INPUT_SPLIT_SIZE";

    protected String DEFAULT_INPUT_FORMAT;
    protected String DEFAULT_OUTPUT_FORMAT;

    public final static String PARTITION_FILTER_SIGNATURE = "cassandra.partition.filter";

    private static final Logger logger = LoggerFactory.getLogger(AbstractCassandraStorage.class);

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


    public AbstractCassandraStorage()
    {
        super();
    }

    /** Deconstructs a composite type to a Tuple. */
    protected Tuple composeComposite(AbstractCompositeType comparator, ByteBuffer name) throws IOException
    {
        List<CompositeComponent> result = comparator.deconstruct(name);
        Tuple t = TupleFactory.getInstance().newTuple(result.size());
        for (int i=0; i<result.size(); i++)
            setTupleValue(t, i, cassandraToObj(result.get(i).comparator, result.get(i).value));

        return t;
    }

    /** convert a column to a tuple */
    protected Tuple columnToTuple(Cell col, CfInfo cfInfo, AbstractType comparator) throws IOException
    {
        CfDef cfDef = cfInfo.cfDef;
        Tuple pair = TupleFactory.getInstance().newTuple(2);

        ByteBuffer colName = col.name().toByteBuffer();

        // name
        if(comparator instanceof AbstractCompositeType)
            setTupleValue(pair, 0, composeComposite((AbstractCompositeType)comparator,colName));
        else
            setTupleValue(pair, 0, cassandraToObj(comparator, colName));

        // value
        Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);
        if (cfInfo.cql3Table && !cfInfo.compactCqlTable)
        {
            ByteBuffer[] names = ((AbstractCompositeType) parseType(cfDef.comparator_type)).split(colName);
            colName = names[names.length-1];
        }
        if (validators.get(colName) == null)
        {
            Map<MarshallerType, AbstractType> marshallers = getDefaultMarshallers(cfDef);
            setTupleValue(pair, 1, cassandraToObj(marshallers.get(MarshallerType.DEFAULT_VALIDATOR), col.value()));
        }
        else
            setTupleValue(pair, 1, cassandraToObj(validators.get(colName), col.value()));
        return pair;
    }

    /** set the value to the position of the tuple */
    protected void setTupleValue(Tuple pair, int position, Object value) throws ExecException
    {
       if (value instanceof BigInteger)
           pair.set(position, ((BigInteger) value).intValue());
       else if (value instanceof ByteBuffer)
           pair.set(position, new DataByteArray(ByteBufferUtil.getArray((ByteBuffer) value)));
       else if (value instanceof UUID)
           pair.set(position, new DataByteArray(UUIDGen.decompose((java.util.UUID) value)));
       else if (value instanceof Date)
           pair.set(position, TimestampType.instance.decompose((Date) value).getLong());
       else
           pair.set(position, value);
    }

    /** get the columnfamily definition for the signature */
    protected CfInfo getCfInfo(String signature) throws IOException
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(AbstractCassandraStorage.class);
        String prop = property.getProperty(signature);
        CfInfo cfInfo = new CfInfo();
        cfInfo.cfDef = cfdefFromString(prop.substring(2));
        cfInfo.compactCqlTable = prop.charAt(0) == '1' ? true : false;
        cfInfo.cql3Table = prop.charAt(1) == '1' ? true : false;
        return cfInfo;
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

    /** decompose the query to store the parameters in a map */
    public static Map<String, String> getQueryMap(String query) throws UnsupportedEncodingException 
    {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params)
        {
            String[] keyValue = param.split("=");
            map.put(keyValue[0], URLDecoder.decode(keyValue[1],"UTF-8"));
        }
        return map;
    }

    /** set hadoop cassandra connection settings */
    protected void setConnectionInformation() throws IOException
    {
        if (System.getenv(PIG_RPC_PORT) != null)
        {
            ConfigHelper.setInputRpcPort(conf, System.getenv(PIG_RPC_PORT));
            ConfigHelper.setOutputRpcPort(conf, System.getenv(PIG_RPC_PORT));
        }

        if (System.getenv(PIG_INPUT_RPC_PORT) != null)
            ConfigHelper.setInputRpcPort(conf, System.getenv(PIG_INPUT_RPC_PORT));
        if (System.getenv(PIG_OUTPUT_RPC_PORT) != null)
            ConfigHelper.setOutputRpcPort(conf, System.getenv(PIG_OUTPUT_RPC_PORT));

        if (System.getenv(PIG_INITIAL_ADDRESS) != null)
        {
            ConfigHelper.setInputInitialAddress(conf, System.getenv(PIG_INITIAL_ADDRESS));
            ConfigHelper.setOutputInitialAddress(conf, System.getenv(PIG_INITIAL_ADDRESS));
        }
        if (System.getenv(PIG_INPUT_INITIAL_ADDRESS) != null)
            ConfigHelper.setInputInitialAddress(conf, System.getenv(PIG_INPUT_INITIAL_ADDRESS));
        if (System.getenv(PIG_OUTPUT_INITIAL_ADDRESS) != null)
            ConfigHelper.setOutputInitialAddress(conf, System.getenv(PIG_OUTPUT_INITIAL_ADDRESS));

        if (System.getenv(PIG_PARTITIONER) != null)
        {
            ConfigHelper.setInputPartitioner(conf, System.getenv(PIG_PARTITIONER));
            ConfigHelper.setOutputPartitioner(conf, System.getenv(PIG_PARTITIONER));
        }
        if(System.getenv(PIG_INPUT_PARTITIONER) != null)
            ConfigHelper.setInputPartitioner(conf, System.getenv(PIG_INPUT_PARTITIONER));
        if(System.getenv(PIG_OUTPUT_PARTITIONER) != null)
            ConfigHelper.setOutputPartitioner(conf, System.getenv(PIG_OUTPUT_PARTITIONER));
        if (System.getenv(PIG_INPUT_FORMAT) != null)
            inputFormatClass = getFullyQualifiedClassName(System.getenv(PIG_INPUT_FORMAT));
        else
            inputFormatClass = DEFAULT_INPUT_FORMAT;
        if (System.getenv(PIG_OUTPUT_FORMAT) != null)
            outputFormatClass = getFullyQualifiedClassName(System.getenv(PIG_OUTPUT_FORMAT));
        else
            outputFormatClass = DEFAULT_OUTPUT_FORMAT;
    }

    /** get the full class name */
    protected String getFullyQualifiedClassName(String classname)
    {
        return classname.contains(".") ? classname : "org.apache.cassandra.hadoop." + classname;
    }

    /** get pig type for the cassandra data type*/
    protected byte getPigType(AbstractType type)
    {
        if (type instanceof LongType || type instanceof DateType || type instanceof TimestampType) // DateType is bad and it should feel bad
            return DataType.LONG;
        else if (type instanceof IntegerType || type instanceof Int32Type) // IntegerType will overflow at 2**31, but is kept for compatibility until pig has a BigInteger
            return DataType.INTEGER;
        else if (type instanceof AsciiType || type instanceof UTF8Type || type instanceof DecimalType || type instanceof InetAddressType)
            return DataType.CHARARRAY;
        else if (type instanceof FloatType)
            return DataType.FLOAT;
        else if (type instanceof DoubleType)
            return DataType.DOUBLE;
        else if (type instanceof AbstractCompositeType || type instanceof CollectionType)
            return DataType.TUPLE;

        return DataType.BYTEARRAY;
    }

    public ResourceStatistics getStatistics(String location, Job job)
    {
        return null;
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

    public void checkSchema(ResourceSchema schema) throws IOException
    {
        // we don't care about types, they all get casted to ByteBuffers
    }

    protected abstract ByteBuffer nullToBB();

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

    public void cleanupOnFailure(String failure, Job job)
    {
    }

    public void cleanupOnSuccess(String location, Job job) throws IOException {
    }


    /** Methods to get the column family schema from Cassandra */
    protected void initSchema(String signature) throws IOException
    {
        Properties properties = UDFContext.getUDFContext().getUDFProperties(AbstractCassandraStorage.class);

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
                    credentials.put(IAuthenticator.USERNAME_KEY, username);
                    credentials.put(IAuthenticator.PASSWORD_KEY, password);

                    try
                    {
                        client.login(new AuthenticationRequest(credentials));
                    }
                    catch (AuthenticationException e)
                    {
                        logger.error("Authentication exception: invalid username and/or password");
                        throw new IOException(e);
                    }
                    catch (AuthorizationException e)
                    {
                        throw new AssertionError(e); // never actually throws AuthorizationException.
                    }
                }

                // compose the CfDef for the columfamily
                CfInfo cfInfo = getCfInfo(client);

                if (cfInfo.cfDef != null)
                {
                    StringBuilder sb = new StringBuilder();
                    sb.append(cfInfo.compactCqlTable ? 1 : 0).append(cfInfo.cql3Table ? 1: 0).append(cfdefToString(cfInfo.cfDef));
                    properties.setProperty(signature, sb.toString());
                }
                else
                    throw new IOException(String.format("Column family '%s' not found in keyspace '%s'",
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

    /** return the CfInfo for the column family */
    protected CfInfo getCfInfo(Cassandra.Client client)
            throws InvalidRequestException,
                   UnavailableException,
                   TimedOutException,
                   SchemaDisagreementException,
                   TException,
                   NotFoundException,
                   org.apache.cassandra.exceptions.InvalidRequestException,
                   ConfigurationException,
                   IOException
    {
        // get CF meta data
        String query = "SELECT type," +
                       "       comparator," +
                       "       subcomparator," +
                       "       default_validator," +
                       "       key_validator," +
                       "       key_aliases " +
                       "FROM system.schema_columnfamilies " +
                       "WHERE keyspace_name = '%s' " +
                       "  AND columnfamily_name = '%s' ";

        CqlResult result = client.execute_cql3_query(
                                ByteBufferUtil.bytes(String.format(query, keyspace, column_family)),
                                Compression.NONE,
                                ConsistencyLevel.ONE);

        if (result == null || result.rows == null || result.rows.isEmpty())
            return null;

        Iterator<CqlRow> iteraRow = result.rows.iterator();
        CfDef cfDef = new CfDef();
        cfDef.keyspace = keyspace;
        cfDef.name = column_family;
        boolean cql3Table = false;
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
            String keyAliases = ByteBufferUtil.string(cqlRow.columns.get(5).value);
            if (FBUtilities.fromJsonList(keyAliases).size() > 0)
                cql3Table = true;
        }
        cfDef.column_metadata = getColumnMetadata(client);
        CfInfo cfInfo = new CfInfo();
        cfInfo.cfDef = cfDef;
        if (cql3Table && !(parseType(cfDef.comparator_type) instanceof AbstractCompositeType))
            cfInfo.compactCqlTable = true;
        if (cql3Table)
            cfInfo.cql3Table = true;; 
        return cfInfo;
    }

    /** get a list of columns */
    protected abstract List<ColumnDef> getColumnMetadata(Cassandra.Client client)
            throws InvalidRequestException,
            UnavailableException,
            TimedOutException,
            SchemaDisagreementException,
            TException,
            CharacterCodingException,
            org.apache.cassandra.exceptions.InvalidRequestException,
            ConfigurationException,
            NotFoundException;

    /** get column meta data */
    protected List<ColumnDef> getColumnMeta(Cassandra.Client client, boolean cassandraStorage, boolean includeCompactValueColumn)
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
        String query = "SELECT column_name, " +
                       "       validator, " +
                       "       index_type, " +
                       "       type " +
                       "FROM system.schema_columns " +
                       "WHERE keyspace_name = '%s' " +
                       "  AND columnfamily_name = '%s'";

        CqlResult result = client.execute_cql3_query(
                                   ByteBufferUtil.bytes(String.format(query, keyspace, column_family)),
                                   Compression.NONE,
                                   ConsistencyLevel.ONE);

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

    /** get a list of columns with defined index*/
    protected List<ColumnDef> getIndexes() throws IOException
    {
        CfDef cfdef = getCfInfo(loadSignature).cfDef;
        List<ColumnDef> indexes = new ArrayList<ColumnDef>();
        for (ColumnDef cdef : cfdef.column_metadata)
        {
            if (cdef.index_type != null)
                indexes.add(cdef);
        }
        return indexes;
    }

    /** get CFMetaData of a column family */
    protected CFMetaData getCFMetaData(String ks, String cf, Cassandra.Client client)
            throws NotFoundException,
            InvalidRequestException,
            TException,
            org.apache.cassandra.exceptions.InvalidRequestException,
            ConfigurationException
    {
        KsDef ksDef = client.describe_keyspace(ks);
        for (CfDef cfDef : ksDef.cf_defs)
        {
            if (cfDef.name.equalsIgnoreCase(cf))
                return CFMetaData.fromThrift(cfDef);
        }
        return null;
    }

    protected Object cassandraToObj(AbstractType validator, ByteBuffer value)
    {
        if (validator instanceof DecimalType || validator instanceof InetAddressType)
            return validator.getString(value);

        if (validator instanceof CollectionType)
        {
            // For CollectionType, the compose() method assumes the v3 protocol format of collection, which
            // is not correct here since we query using the CQL-over-thrift interface which use the pre-v3 format
            return ((CollectionSerializer)validator.getSerializer()).deserializeForNativeProtocol(value, nativeProtocolVersion);
        }

        return validator.compose(value);
    }

    protected static class CfInfo
    {
        boolean compactCqlTable = false;
        boolean cql3Table = false;
        CfDef cfDef;
    }
}

