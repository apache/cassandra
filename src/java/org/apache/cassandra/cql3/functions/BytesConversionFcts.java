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

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class BytesConversionFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        for (CQL3Type type : CQL3Type.Native.values())
        {
            if (type == CQL3Type.Native.BLOB)
                continue;

            functions.add(new ToBlobFunction(type));
            functions.add(new FromBlobFunction(type));
        }
    }

    private static abstract class BytesConversionFct extends NativeScalarFunction
    {
        public BytesConversionFct(String name, AbstractType<?> returnType, AbstractType<?>... argsType)
        {
            super(name, returnType, argsType);
        }

        @Override
        public Arguments newArguments(ProtocolVersion version)
        {
            return FunctionArguments.newNoopInstance(version, 1);
        }
    }

    // Most of the X_as_blob and blob_as_X functions are basically no-op since everything is
    // bytes internally. They only "trick" the type system.
    public static class ToBlobFunction extends BytesConversionFct
    {
        private final CQL3Type fromType;

        public ToBlobFunction(CQL3Type fromType)
        {
            this(fromType, false);
        }

        private ToBlobFunction(CQL3Type fromType, boolean useLegacyName)
        {
            super(fromType + (useLegacyName ? "asblob" : "_as_blob"),
                  BytesType.instance,
                  fromType.getType().udfType());
            this.fromType = fromType;
        }

        @Override
        public ByteBuffer execute(Arguments arguments)
        {
            return arguments.get(0);
        }

        @Override
        public NativeFunction withLegacyName()
        {
            return new ToBlobFunction(fromType, true);
        }
    }

    public static class FromBlobFunction extends BytesConversionFct
    {
        private final CQL3Type toType;

        public FromBlobFunction(CQL3Type toType)
        {
            this(toType, false);
        }

        private FromBlobFunction(CQL3Type toType, boolean useLegacyName)
        {
            super((useLegacyName ? "blobas" : "blob_as_") + toType,
                  toType.getType().udfType(),
                  BytesType.instance);
            this.toType = toType;
        }

        @Override
        public ByteBuffer execute(Arguments arguments)
        {
            ByteBuffer val = arguments.get(0);

            if (val != null)
            {
                try
                {
                    toType.getType().validate(val);
                }
                catch (MarshalException e)
                {
                    throw new InvalidRequestException(String.format("In call to function %s, value 0x%s is not a " +
                                                                    "valid binary representation for type %s",
                                                                    name, ByteBufferUtil.bytesToHex(val), toType));
                }
            }

            return val;
        }

        @Override
        public NativeFunction withLegacyName()
        {
            return new FromBlobFunction(toType, true);
        }
    }
}
