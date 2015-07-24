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

package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Base class for all Java UDFs.
 * Used to separate internal classes like {@link UDFunction} from user provided code.
 * Only references <b>to</b> this class (and generated implementations) are allowed -
 * references from this class back to C* code are not allowed (except argument/return type information).
 */
public abstract class JavaUDF
{
    private final DataType returnDataType;
    private final DataType[] argDataTypes;

    protected JavaUDF(DataType returnDataType, DataType[] argDataTypes)
    {
        this.returnDataType = returnDataType;
        this.argDataTypes = argDataTypes;
    }

    protected abstract ByteBuffer executeImpl(int protocolVersion, List<ByteBuffer> params);

    protected Object compose(int protocolVersion, int argIndex, ByteBuffer value)
    {
        return UDFunction.compose(argDataTypes, protocolVersion, argIndex, value);
    }

    protected ByteBuffer decompose(int protocolVersion, Object value)
    {
        return UDFunction.decompose(returnDataType, protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected float compose_float(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (float) DataType.cfloat().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected double compose_double(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (double) DataType.cdouble().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected byte compose_byte(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (byte) DataType.tinyint().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected short compose_short(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (short) DataType.smallint().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected int compose_int(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (int) DataType.cint().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected long compose_long(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (long) DataType.bigint().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected boolean compose_boolean(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (boolean) DataType.cboolean().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }
}
