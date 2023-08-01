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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.cql3.functions.Arguments;
import org.apache.cassandra.cql3.functions.FunctionArguments;
import org.apache.cassandra.cql3.functions.FunctionFactory;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.FunctionParameter;
import org.apache.cassandra.cql3.functions.NativeFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
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
    public static final ByteBuffer DEFAULT_ALGORITHM = UTF8Type.instance.decompose("SHA-256");

    /**
     * All the created digests that we use to generate the hash, so we don't need to get them on every call.
     * We use the serialized algorithm as key, so we don't have to deserialize it for getting a cache hit.
     */
    private static final Map<ByteBuffer, MessageDigest> DIGESTS = new ConcurrentHashMap<>();

    private static final AbstractType<?>[] DEFAULT_ARGUMENTS = {};
    private static final AbstractType<?>[] ARGUMENTS_WITH_ALGORITHM = new AbstractType<?>[]{ UTF8Type.instance };

    private HashMaskingFunction(FunctionName name, AbstractType<?> inputType, boolean hasAlgorithmArgument)
    {
        super(name, BytesType.instance, inputType, hasAlgorithmArgument ? ARGUMENTS_WITH_ALGORITHM : DEFAULT_ARGUMENTS);
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        return new FunctionArguments(version,
                                     ArgumentDeserializer.NOOP_DESERIALIZER, // the value to be masked
                                     (v, b) -> messageDigest(b)); // the algorithm, if any
    }

    @Override
    public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
    {
        ByteBuffer value = arguments.get(0);
        if (value == null)
            return null;

        MessageDigest digest = arguments.get(1);
        if (digest == null)
            digest = messageDigest(DEFAULT_ALGORITHM);

        return HashMaskingFunction.hash(digest, value);
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
    static MessageDigest messageDigest(@Nullable ByteBuffer algorithm)
    {
        ByteBuffer cacheKey = algorithm == null ? DEFAULT_ALGORITHM : algorithm;
        MessageDigest digest = DIGESTS.get(cacheKey);
        if (digest == null)
        {
            digest = messageDigest(UTF8Type.instance.compose(cacheKey));
            DIGESTS.put(cacheKey.duplicate(), digest);
        }
        return digest;
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
                        return new HashMaskingFunction(name, argTypes.get(0), false);
                    case 2:
                        return new HashMaskingFunction(name, argTypes.get(0), true);
                    default:
                        throw invalidNumberOfArgumentsException();
                }
            }
        };
    }
}
