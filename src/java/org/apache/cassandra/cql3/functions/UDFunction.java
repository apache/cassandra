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
import java.util.*;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.UserType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Base class for User Defined Functions.
 */
public abstract class UDFunction extends AbstractFunction implements ScalarFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDFunction.class);

    protected final List<ColumnIdentifier> argNames;

    protected final String language;
    protected final String body;

    protected final DataType[] argDataTypes;
    protected final DataType returnDataType;
    protected final boolean calledOnNullInput;

    protected UDFunction(FunctionName name,
                         List<ColumnIdentifier> argNames,
                         List<AbstractType<?>> argTypes,
                         AbstractType<?> returnType,
                         boolean calledOnNullInput,
                         String language,
                         String body)
    {
        this(name, argNames, argTypes, UDHelper.driverTypes(argTypes), returnType,
             UDHelper.driverType(returnType), calledOnNullInput, language, body);
    }

    protected UDFunction(FunctionName name,
                         List<ColumnIdentifier> argNames,
                         List<AbstractType<?>> argTypes,
                         DataType[] argDataTypes,
                         AbstractType<?> returnType,
                         DataType returnDataType,
                         boolean calledOnNullInput,
                         String language,
                         String body)
    {
        super(name, argTypes, returnType);
        assert new HashSet<>(argNames).size() == argNames.size() : "duplicate argument names";
        this.argNames = argNames;
        this.language = language;
        this.body = body;
        this.argDataTypes = argDataTypes;
        this.returnDataType = returnDataType;
        this.calledOnNullInput = calledOnNullInput;
    }

    public static UDFunction create(FunctionName name,
                                    List<ColumnIdentifier> argNames,
                                    List<AbstractType<?>> argTypes,
                                    AbstractType<?> returnType,
                                    boolean calledOnNullInput,
                                    String language,
                                    String body)
    throws InvalidRequestException
    {
        if (!DatabaseDescriptor.enableUserDefinedFunctions())
            throw new InvalidRequestException("User-defined-functions are disabled in cassandra.yaml - set enable_user_defined_functions=true to enable if you are aware of the security risks");

        switch (language)
        {
            case "java": return JavaSourceUDFFactory.buildUDF(name, argNames, argTypes, returnType, calledOnNullInput, body);
            default: return new ScriptBasedUDF(name, argNames, argTypes, returnType, calledOnNullInput, language, body);
        }
    }

    /**
     * It can happen that a function has been declared (is listed in the scheam) but cannot
     * be loaded (maybe only on some nodes). This is the case for instance if the class defining
     * the class is not on the classpath for some of the node, or after a restart. In that case,
     * we create a "fake" function so that:
     *  1) the broken function can be dropped easily if that is what people want to do.
     *  2) we return a meaningful error message if the function is executed (something more precise
     *     than saying that the function doesn't exist)
     */
    public static UDFunction createBrokenFunction(FunctionName name,
                                                  List<ColumnIdentifier> argNames,
                                                  List<AbstractType<?>> argTypes,
                                                  AbstractType<?> returnType,
                                                  boolean calledOnNullInput,
                                                  String language,
                                                  String body,
                                                  final InvalidRequestException reason)
    {
        return new UDFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body)
        {
            public ByteBuffer executeUserDefined(int protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
            {
                throw new InvalidRequestException(String.format("Function '%s' exists but hasn't been loaded successfully "
                                                                + "for the following reason: %s. Please see the server log for details",
                                                                this,
                                                                reason.getMessage()));
            }
        };
    }

    public final ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
    {
        if (!DatabaseDescriptor.enableUserDefinedFunctions())
            throw new InvalidRequestException("User-defined-functions are disabled in cassandra.yaml - set enable_user_defined_functions=true to enable if you are aware of the security risks");

        if (!isCallableWrtNullable(parameters))
            return null;
        return executeUserDefined(protocolVersion, parameters);
    }

    public boolean isCallableWrtNullable(List<ByteBuffer> parameters)
    {
        if (!calledOnNullInput)
            for (ByteBuffer parameter : parameters)
                if (parameter == null || parameter.remaining() == 0)
                    return false;
        return true;
    }

    protected abstract ByteBuffer executeUserDefined(int protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException;

    public boolean isAggregate()
    {
        return false;
    }

    public boolean isNative()
    {
        return false;
    }

    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }

    public List<ColumnIdentifier> argNames()
    {
        return argNames;
    }

    public String body()
    {
        return body;
    }

    public String language()
    {
        return language;
    }

    /**
     * Used by UDF implementations (both Java code generated by {@link org.apache.cassandra.cql3.functions.JavaSourceUDFFactory}
     * and script executor {@link org.apache.cassandra.cql3.functions.ScriptBasedUDF}) to convert the C*
     * serialized representation to the Java object representation.
     *
     * @param protocolVersion the native protocol version used for serialization
     * @param argIndex index of the UDF input argument
     */
    protected Object compose(int protocolVersion, int argIndex, ByteBuffer value)
    {
        return value == null ? null : argDataTypes[argIndex].deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected float compose_float(int protocolVersion, int argIndex, ByteBuffer value)
    {
        return value == null ? 0f : (float)DataType.cfloat().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected double compose_double(int protocolVersion, int argIndex, ByteBuffer value)
    {
        return value == null ? 0d : (double)DataType.cdouble().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected int compose_int(int protocolVersion, int argIndex, ByteBuffer value)
    {
        return value == null ? 0 : (int)DataType.cint().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected long compose_long(int protocolVersion, int argIndex, ByteBuffer value)
    {
        return value == null ? 0L : (long)DataType.bigint().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    // do not remove - used by generated Java UDFs
    protected boolean compose_boolean(int protocolVersion, int argIndex, ByteBuffer value)
    {
        return value != null && (boolean) DataType.cboolean().deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    /**
     * Used by UDF implementations (both Java code generated by {@link org.apache.cassandra.cql3.functions.JavaSourceUDFFactory}
     * and script executor {@link org.apache.cassandra.cql3.functions.ScriptBasedUDF}) to convert the Java
     * object representation for the return value to the C* serialized representation.
     *
     * @param protocolVersion the native protocol version used for serialization
     */
    protected ByteBuffer decompose(int protocolVersion, Object value)
    {
        return value == null ? null : returnDataType.serialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDFunction))
            return false;

        UDFunction that = (UDFunction)o;
        return Objects.equal(name, that.name)
            && Objects.equal(argNames, that.argNames)
            && Functions.typeEquals(argTypes, that.argTypes)
            && Functions.typeEquals(returnType, that.returnType)
            && Objects.equal(language, that.language)
            && Objects.equal(body, that.body);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, argNames, argTypes, returnType, language, body);
    }

    public void userTypeUpdated(String ksName, String typeName)
    {
        boolean updated = false;

        for (int i = 0; i < argDataTypes.length; i++)
        {
            DataType dataType = argDataTypes[i];
            if (dataType instanceof UserType)
            {
                UserType userType = (UserType) dataType;
                if (userType.getKeyspace().equals(ksName) && userType.getTypeName().equals(typeName))
                {
                    KSMetaData ksm = Schema.instance.getKSMetaData(ksName);
                    assert ksm != null;

                    org.apache.cassandra.db.marshal.UserType ut = ksm.userTypes.getType(ByteBufferUtil.bytes(typeName));

                    DataType newUserType = UDHelper.driverType(ut);
                    argDataTypes[i] = newUserType;

                    argTypes.set(i, ut);

                    updated = true;
                }
            }
        }

        if (updated)
            MigrationManager.announceNewFunction(this, true);
    }
}
