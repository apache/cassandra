package #package_name#;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.cql3.functions.JavaUDF;
import org.apache.cassandra.cql3.functions.UDFContext;
import org.apache.cassandra.transport.ProtocolVersion;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;

public final class #class_name# extends JavaUDF
{
    public #class_name#(TypeCodec<Object> returnCodec, TypeCodec<Object>[] argCodecs, UDFContext udfContext)
    {
        super(returnCodec, argCodecs, udfContext);
    }

    protected ByteBuffer executeImpl(ProtocolVersion protocolVersion, List<ByteBuffer> params)
    {
        #return_type# result = #execute_internal_name#(
#arguments#
        );
        return super.decompose(protocolVersion, result);
    }

    protected Object executeAggregateImpl(ProtocolVersion protocolVersion, Object firstParam, List<ByteBuffer> params)
    {
        #return_type# result = #execute_internal_name#(
#arguments_aggregate#
        );
        return result;
    }

    private #return_type# #execute_internal_name#(#argument_list#)
    {
#body#
    }
}
