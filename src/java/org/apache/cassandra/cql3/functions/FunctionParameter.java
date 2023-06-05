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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.AssignmentTestable.TestResult.NOT_ASSIGNABLE;

/**
 * Generic, loose definition of a function parameter, able to infer the specific data type of the parameter in the
 * function specifically built by a {@link FunctionFactory} for a particular function call.
 */
public interface FunctionParameter
{
    /**
     * Tries to infer the data type of the parameter for an argument in a call to the function.
     *
     * @param keyspace the current keyspace
     * @param arg a parameter value in a specific function call
     * @param receiverType the type of the object that will receive the result of the function call
     * @param inferredTypes the types that have been inferred for the other parameters
     * @return the inferred data type of the parameter, or {@link null} it isn't possible to infer it
     */
    @Nullable
    default AbstractType<?> inferType(String keyspace,
                                      AssignmentTestable arg,
                                      @Nullable AbstractType<?> receiverType,
                                      @Nullable List<AbstractType<?>> inferredTypes)
    {
        return arg.getCompatibleTypeIfKnown(keyspace);
    }

    void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType);

    /**
     * @return a function parameter definition that accepts values of string-based data types (text, varchar and ascii)
     */
    static FunctionParameter string()
    {
        return fixed(CQL3Type.Native.TEXT, CQL3Type.Native.VARCHAR, CQL3Type.Native.ASCII);
    }

    /**
     * @param types the accepted data types
     * @return a function parameter definition that accepts values of a specific data type
     */
    static FunctionParameter fixed(CQL3Type... types)
    {
        assert types.length > 0;

        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             @Nullable List<AbstractType<?>> inferredTypes)
            {
                AbstractType<?> inferred = arg.getCompatibleTypeIfKnown(keyspace);
                return inferred != null ? inferred : types[0].getType();
            }

            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                if (Arrays.stream(types).allMatch(t -> argType.testAssignment(t.getType()) == NOT_ASSIGNABLE))
                    throw new InvalidRequestException(format("Function %s requires an argument of type %s, " +
                                                             "but found argument %s of type %s",
                                                             name, this, arg, argType.asCQL3Type()));
            }

            @Override
            public String toString()
            {
                if (types.length == 1)
                    return types[0].toString();

                return '[' + Arrays.stream(types).map(Object::toString).collect(Collectors.joining("|")) + ']';
            }
        };
    }

    /**
     * @param inferFromReceiver whether the parameter should try to use the function receiver to infer its data type
     * @return a function parameter definition that accepts columns of any data type
     */
    static FunctionParameter anyType(boolean inferFromReceiver)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             @Nullable List<AbstractType<?>> inferredTypes)
            {
                AbstractType<?> type = arg.getCompatibleTypeIfKnown(keyspace);
                return type == null && inferFromReceiver ? receiverType : type;
            }

            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                // nothing to do here, all types are accepted
            }

            @Override
            public String toString()
            {
                return "any";
            }
        };
    }

    /**
     * @return a function parameter definition that accepts values of any type, provided that it's the same type as all
     * the other parameters
     */
    static FunctionParameter sameAs(int index, FunctionParameter parameter)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             @Nullable List<AbstractType<?>> inferredTypes)
            {
                AbstractType<?> type = inferredTypes == null ? null : inferredTypes.get(index);
                return type != null ? type : parameter.inferType(keyspace, arg, receiverType, inferredTypes);
            }

            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                parameter.validateType(name, arg, argType);
            }

            @Override
            public String toString()
            {
                return parameter.toString();
            }
        };
    }

    /**
     * @return a function parameter definition that accepts values of type {@link VectorType}.
     */
    static FunctionParameter vector(CQL3Type type)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             List<AbstractType<?>> inferredTypes)
            {
                AbstractType<?> inferred = arg.getCompatibleTypeIfKnown(keyspace);
                if (inferred != null && arg instanceof Selectable.WithList)
                {
                    return VectorType.getInstance(type.getType(), ((Selectable.WithList) arg).getSize());
                }

                return inferred == null ? receiverType : inferred;
            }

            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                if (argType.isVector())
                {
                    VectorType<?> vectorType = (VectorType<?>) argType;
                    if (vectorType.elementType.asCQL3Type() == type)
                        return;
                }
                else if (argType.isList()) // if it's terminal it will be a list
                {
                    ListType<?> listType = (ListType<?>) argType;
                    if (listType.getElementsType().testAssignment(type.getType()) == NOT_ASSIGNABLE)
                        return;
                }

                throw new InvalidRequestException(format("Function %s requires a %s vector argument, " +
                                "but found argument %s of type %s",
                        name, type, arg, argType.asCQL3Type()));
            }

            @Override
            public String toString()
            {
                return format("vector<%s, n>", type);
            }
        };
    }
}
