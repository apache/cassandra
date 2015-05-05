package org.apache.cassandra.hadoop.pig;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

public class StorageHelper
{
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


    public final static String PARTITION_FILTER_SIGNATURE = "cassandra.partition.filter";

    protected static void setConnectionInformation(Configuration conf)
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
    }

    protected static Object cassandraToObj(AbstractType validator, ByteBuffer value, int nativeProtocolVersion)
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

    /** set the value to the position of the tuple */
    protected static void setTupleValue(Tuple pair, int position, Object value) throws ExecException
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

    /** get pig type for the cassandra data type*/
    protected static byte getPigType(AbstractType type)
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
}
