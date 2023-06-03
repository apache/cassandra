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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.cql3.functions.Arguments;
import org.apache.cassandra.cql3.functions.FunctionArguments;
import org.apache.cassandra.cql3.functions.FunctionFactory;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.FunctionParameter;
import org.apache.cassandra.cql3.functions.NativeFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

import static java.lang.String.format;

/**
 * A {@link MaskingFunction} applied to a {@link org.apache.cassandra.db.marshal.StringType} value that,
 * depending on {@link Kind}:
 * <ul>
 * <li>Replaces each character between the supplied positions by the supplied padding character. In other words,
 * it will mask all the characters except the first m and last n.</li>
 * <li>Replaces each character before and after the supplied positions by the supplied padding character. In other
 * words, it will only mask all the first m and last n characters.</li>
 * </ul>
 * The returned value will allways be of the same type as the first string-based argument.
 */
public class PartialMaskingFunction extends MaskingFunction
{
    /** The character to be used as padding if no other character is supplied when calling the function. */
    public static final char DEFAULT_PADDING_CHAR = '*';

    /** The type of partial masking to perform, inner or outer. */
    private final Kind kind;

    /** The original type of the masked value. */
    private final AbstractType<String> inputType;

    /** The custom argument deserializer for the padding character. */
    private final ArgumentDeserializer paddingArgumentDeserializer;

    private PartialMaskingFunction(FunctionName name,
                                   Kind kind,
                                   AbstractType<String> inputType,
                                   boolean hasPaddingArgument)
    {
        super(name, inputType, inputType, argumentsType(hasPaddingArgument));

        this.kind = kind;
        this.inputType = inputType;

        paddingArgumentDeserializer = (version, buffer) -> {

            if (buffer == null || !hasPaddingArgument)
                return null;

            String arg = UTF8Type.instance.compose(buffer);
            if (arg.length() != 1)
            {
                throw new InvalidRequestException(format("The padding argument for function %s should " +
                                                         "be single-character, but '%s' has %d characters.",
                                                         name, arg, arg.length()));
            }
            return arg.charAt(0);
        };
    }

    private static AbstractType<?>[] argumentsType(boolean hasPaddingArgument)
    {
        // The padding argument is optional, so we provide different signatures depending on whether it's present or not.
        // Also, the padding argument should be a single character, but we don't have a data type for that, so we use
        // a string-based argument. We will later validate on execution that the string argument is single-character.
        return hasPaddingArgument
               ? new AbstractType<?>[]{ Int32Type.instance, Int32Type.instance, UTF8Type.instance }
               : new AbstractType<?>[]{ Int32Type.instance, Int32Type.instance };
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        return new FunctionArguments(version,
                                     inputType.getArgumentDeserializer(),
                                     Int32Type.instance.getArgumentDeserializer(),
                                     Int32Type.instance.getArgumentDeserializer(),
                                     paddingArgumentDeserializer);
    }

    @Override
    public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
    {
        String value = arguments.get(0);
        if (value == null)
            return null;

        int begin = arguments.get(1) != null ? arguments.getAsInt(1) : 0;
        int end = arguments.get(2) != null ? arguments.getAsInt(2) : 0;
        char padding = arguments.get(3) == null ? DEFAULT_PADDING_CHAR : arguments.get(3);

        String maskedValue = kind.mask(value, begin, end, padding);
        return inputType.decompose(maskedValue);
    }

    public enum Kind
    {
        /** Masks everything except the first {@code begin} and last {@code end} characters. */
        INNER
        {
            @Override
            protected boolean shouldMask(int pos, int begin, int end)
            {
                return pos >= begin && pos <= end;
            }
        },
        /** Masks only the first {@code begin} and last {@code end} characters. */
        OUTER
        {
            @Override
            protected boolean shouldMask(int pos, int begin, int end)
            {
                return pos < begin || pos > end;
            }
        };

        protected abstract boolean shouldMask(int pos, int begin, int end);

        @VisibleForTesting
        public String mask(String value, int begin, int end, char padding)
        {
            if (StringUtils.isEmpty(value))
                return value;

            int size = value.length();
            int endIndex = size - 1 - end;
            char[] chars = new char[size];

            for (int i = 0; i < size; i++)
            {
                chars[i] = shouldMask(i, begin, endIndex) ? padding : value.charAt(i);
            }

            return new String(chars);
        }
    }

    /** @return a collection of function factories to build new {@code PartialMaskingFunction} functions. */
    public static Collection<FunctionFactory> factories()
    {
        return Stream.of(Kind.values())
                     .map(PartialMaskingFunction::factory)
                     .collect(Collectors.toSet());
    }

    private static FunctionFactory factory(Kind kind)
    {
        return new MaskingFunction.Factory(kind.name(),
                                           FunctionParameter.string(),
                                           FunctionParameter.fixed(CQL3Type.Native.INT),
                                           FunctionParameter.fixed(CQL3Type.Native.INT),
                                           FunctionParameter.optional(FunctionParameter.fixed(CQL3Type.Native.TEXT)))
        {
            @Override
            @SuppressWarnings("unchecked")
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                AbstractType<String> inputType = (AbstractType<String>) argTypes.get(0);
                return new PartialMaskingFunction(name, kind, inputType, argTypes.size() == 4);
            }
        };
    }
}
