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

import com.datastax.driver.core.TypeCodec;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Base class for all Java UDFs.
 * Used to separate internal classes like {@link UDFunction} from user provided code.
 * Only references <b>to</b> this class (and generated implementations) are allowed -
 * references from this class back to C* code are not allowed (except argument/return type information).
 */
public abstract class JavaUDF
{
    private final TypeCodec<Object> returnCodec;
    private final TypeCodec<Object>[] argCodecs;

    protected final UDFContext udfContext;

    protected JavaUDF(TypeCodec<Object> returnCodec, TypeCodec<Object>[] argCodecs, UDFContext udfContext)
    {
        this.returnCodec = returnCodec;
        this.argCodecs = argCodecs;
        this.udfContext = udfContext;
    }

    protected abstract ByteBuffer executeImpl(ProtocolVersion protocolVersion, List<ByteBuffer> params);

    protected abstract Object executeAggregateImpl(ProtocolVersion protocolVersion, Object firstParam, List<ByteBuffer> params);

    protected Object compose(ProtocolVersion protocolVersion, int argIndex, ByteBuffer value)
    {
        return UDFunction.compose(argCodecs, protocolVersion, argIndex, value);
    }

    protected ByteBuffer decompose(ProtocolVersion protocolVersion, Object value)
    {
        return UDFunction.decompose(returnCodec, protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected float compose_float(ProtocolVersion protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (float) UDHelper.deserialize(TypeCodec.cfloat(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected double compose_double(ProtocolVersion protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (double) UDHelper.deserialize(TypeCodec.cdouble(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected byte compose_byte(ProtocolVersion protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (byte) UDHelper.deserialize(TypeCodec.tinyInt(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected short compose_short(ProtocolVersion protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (short) UDHelper.deserialize(TypeCodec.smallInt(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected int compose_int(ProtocolVersion protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (int) UDHelper.deserialize(TypeCodec.cint(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected long compose_long(ProtocolVersion protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (long) UDHelper.deserialize(TypeCodec.bigint(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected boolean compose_boolean(ProtocolVersion protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (boolean) UDHelper.deserialize(TypeCodec.cboolean(), protocolVersion, value);
    }
}
