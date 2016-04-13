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

    protected abstract ByteBuffer executeImpl(int protocolVersion, List<ByteBuffer> params);

    protected Object compose(int protocolVersion, int argIndex, ByteBuffer value)
    {
        return UDFunction.compose(argCodecs, protocolVersion, argIndex, value);
    }

    protected ByteBuffer decompose(int protocolVersion, Object value)
    {
        return UDFunction.decompose(returnCodec, protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected float compose_float(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (float) UDHelper.deserialize(TypeCodec.cfloat(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected double compose_double(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (double) UDHelper.deserialize(TypeCodec.cdouble(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected byte compose_byte(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (byte) UDHelper.deserialize(TypeCodec.tinyInt(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected short compose_short(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (short) UDHelper.deserialize(TypeCodec.smallInt(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected int compose_int(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (int) UDHelper.deserialize(TypeCodec.cint(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected long compose_long(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (long) UDHelper.deserialize(TypeCodec.bigint(), protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected boolean compose_boolean(int protocolVersion, int argIndex, ByteBuffer value)
    {
        assert value != null && value.remaining() > 0;
        return (boolean) UDHelper.deserialize(TypeCodec.cboolean(), protocolVersion, value);
    }
}
