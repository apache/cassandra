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

package org.apache.cassandra.cql3.functions.masking;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.FunctionFactory;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.FunctionParameter;
import org.apache.cassandra.cql3.functions.NativeFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.StringType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A {@link MaskingFunction} that replaces the specified column value by its hash according to the specified
 * algorithm. The available algorithms are those defined by the registered security {@link java.security.Provider}s.
 * If no algorithm is passed to the function, the {@link #DEFAULT_ALGORITHM} will be used.
 */
public class HashMaskingFunction extends MaskingFunction
{
    public static final String NAME = "hash";

    /** The default hashing algorithm to be used if no other algorithm is specified in the call to the function. */
    public static final String DEFAULT_ALGORITHM = "SHA-256";

    // The default message digest is lazily built to prevent a failure during server startup if the algorithm is not
    // available. That way, if the algorithm is not found only the calls to the function will fail.
    private static final Supplier<MessageDigest> DEFAULT_DIGEST = Suppliers.memoize(() -> messageDigest(DEFAULT_ALGORITHM));
    private static final AbstractType<?>[] DEFAULT_ARGUMENTS = {};

    /** The type of the supplied algorithm argument, {@code null} if that argument isn't supplied. */
    @Nullable
    private final StringType algorithmArgumentType;

    private HashMaskingFunction(FunctionName name, AbstractType<?> inputType, @Nullable StringType algorithmArgumentType)
    {
        super(name, BytesType.instance, inputType, argumentsType(algorithmArgumentType));
        this.algorithmArgumentType = algorithmArgumentType;
    }

    private static AbstractType<?>[] argumentsType(@Nullable StringType algorithmArgumentType)
    {
        // the algorithm argument is optional, so we will have different signatures depending on whether that argument
        // is supplied or not
        return algorithmArgumentType == null
               ? DEFAULT_ARGUMENTS
               : new AbstractType<?>[]{ algorithmArgumentType };
    }

    @Override
    public final ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
    {
        MessageDigest digest;
        if (algorithmArgumentType == null || parameters.get(1) == null)
        {
            digest = DEFAULT_DIGEST.get();
        }
        else
        {
            String algorithm = algorithmArgumentType.compose(parameters.get(1));
            digest = messageDigest(algorithm);
        }

        return hash(digest, parameters.get(0));
    }

    @VisibleForTesting
    @Nullable
    static ByteBuffer hash(MessageDigest digest, ByteBuffer value)
    {
        if (value == null)
            return null;

        byte[] hash = digest.digest(ByteBufferUtil.getArray(value));
        return BytesType.instance.compose(ByteBuffer.wrap(hash));
    }

    @VisibleForTesting
    static MessageDigest messageDigest(String algorithm)
    {
        try
        {
            return MessageDigest.getInstance(algorithm);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new InvalidRequestException("Hash algorithm not found: " + algorithm);
        }
    }

    /** @return a {@link FunctionFactory} to build new {@link HashMaskingFunction}s. */
    public static FunctionFactory factory()
    {
        return new MaskingFunction.Factory(NAME,
                                           FunctionParameter.anyType(false),
                                           FunctionParameter.optional(FunctionParameter.fixed(CQL3Type.Native.TEXT)))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                switch (argTypes.size())
                {
                    case 1:
                        return new HashMaskingFunction(name, argTypes.get(0), null);
                    case 2:
                        return new HashMaskingFunction(name, argTypes.get(0), UTF8Type.instance);
                    default:
                        throw invalidNumberOfArgumentsException();
                }
            }
        };
    }
}
