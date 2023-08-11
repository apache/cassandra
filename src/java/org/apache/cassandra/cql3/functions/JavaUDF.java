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

import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Base class for all Java UDFs.
 * Used to separate internal classes like {@link UDFunction} from user provided code.
 * Only references <b>to</b> this class (and generated implementations) are allowed -
 * references from this class back to C* code are not allowed (except argument/return type information).
 */
public abstract class JavaUDF
{
    private final UDFDataType returnType;
    protected final UDFContext udfContext;

    protected JavaUDF(UDFDataType returnType, UDFContext udfContext)
    {
        this.returnType = returnType;
        this.udfContext = udfContext;
    }

    protected abstract ByteBuffer executeImpl(Arguments arguments);

    protected abstract Object executeAggregateImpl(Object state, Arguments arguments);

    //~///////////////////////////////////////////////////////////////////////////////////////////
    // The decompose method is overloaded to avoid boxing of the result if it is a primitive type.
    //~///////////////////////////////////////////////////////////////////////////////////////////

    // do not remove - used by generated Java UDFs
    protected final ByteBuffer decompose(ProtocolVersion protocolVersion, Object value)
    {
        return returnType.decompose(protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected final ByteBuffer decompose(ProtocolVersion protocolVersion, byte value)
    {
        return returnType.decompose(protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected final ByteBuffer decompose(ProtocolVersion protocolVersion, short value)
    {
        return returnType.decompose(protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected final ByteBuffer decompose(ProtocolVersion protocolVersion, int value)
    {
        return returnType.decompose(protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected final ByteBuffer decompose(ProtocolVersion protocolVersion, long value)
    {
        return returnType.decompose(protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected final ByteBuffer decompose(ProtocolVersion protocolVersion, float value)
    {
        return returnType.decompose(protocolVersion, value);
    }

    // do not remove - used by generated Java UDFs
    protected final ByteBuffer decompose(ProtocolVersion protocolVersion, double value)
    {
        return returnType.decompose(protocolVersion, value);
    }
}
