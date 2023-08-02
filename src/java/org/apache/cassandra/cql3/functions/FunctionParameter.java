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
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.SetType;
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
     * @return whether this parameter is optional
     */
    default boolean isOptional()
    {
        return false;
    }

    /**
     * @param wrapped the wrapped parameter
     * @return a function parameter definition that accepts the specified wrapped parameter, considering it optional as
     * defined by {@link #isOptional()}.
     */
    static FunctionParameter optional(FunctionParameter wrapped)
    {
        return new FunctionParameter()
        {
            @Nullable
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             @Nullable List<AbstractType<?>> inferredTypes)
            {
                return wrapped.inferType(keyspace, arg, receiverType, inferredTypes);
            }

            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                wrapped.validateType(name, arg, argType);
            }

            @Override
            public boolean isOptional()
            {
                return true;
            }

            @Override
            public String toString()
            {
                return '[' + wrapped.toString() + ']';
            }
        };
    }

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
     * @param index the index of the function argument that this parameter is associated with
     * @param preferOther whether the parameter should prefer the type of the other parameter over its own type
     * @param parameter the type of this parameter when the type of the associated parameter is unknown
     * @return a function parameter definition that is expected to have the same type as another parameter
     */
    static FunctionParameter sameAs(int index, boolean preferOther, FunctionParameter parameter)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             @Nullable List<AbstractType<?>> inferredTypes)
            {
                if (preferOther)
                {
                    AbstractType<?> other = inferredTypes == null ? null : inferredTypes.get(index);
                    return other == null ? parameter.inferType(keyspace, arg, receiverType, inferredTypes) : other;
                }

                AbstractType<?> inferred = parameter.inferType(keyspace, arg, receiverType, inferredTypes);
                return inferred == null && inferredTypes != null ? inferredTypes.get(index) : inferred;
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
     * @return a function parameter definition that accepts values of type {@link CollectionType}, independently of the
     * types of its elements.
     */
    static FunctionParameter anyCollection()
    {
        return new FunctionParameter()
        {
            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                if (!argType.isCollection())
                    throw new InvalidRequestException(format("Function %s requires a collection argument, " +
                                                             "but found argument %s of type %s",
                                                             name, arg, argType.asCQL3Type()));
            }

            @Override
            public String toString()
            {
                return "collection";
            }
        };
    }

    /**
     * @return a function parameter definition that accepts values of type {@link SetType} or {@link ListType}.
     */
    static FunctionParameter setOrList()
    {
        return new FunctionParameter()
        {
            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                if (argType.isCollection())
                {
                    CollectionType.Kind kind = ((CollectionType<?>) argType).kind;
                    if (kind == CollectionType.Kind.SET || kind == CollectionType.Kind.LIST)
                        return;
                }

                throw new InvalidRequestException(format("Function %s requires a set or list argument, " +
                                                         "but found argument %s of type %s",
                                                         name, arg, argType.asCQL3Type()));
            }

            @Override
            public String toString()
            {
                return "numeric_set_or_list";
            }
        };
    }

    /**
     * @return a function parameter definition that accepts values of type {@link SetType} or {@link ListType},
     * provided that its elements are numeric.
     */
    static FunctionParameter numericSetOrList()
    {
        return new FunctionParameter()
        {
            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                AbstractType<?> elementType = null;
                if (argType.isCollection())
                {
                    CollectionType<?> collectionType = (CollectionType<?>) argType;
                    if (collectionType.kind == CollectionType.Kind.SET)
                    {
                        elementType = ((SetType<?>) argType).getElementsType();
                    }
                    else if (collectionType.kind == CollectionType.Kind.LIST)
                    {
                        elementType = ((ListType<?>) argType).getElementsType();
                    }
                }

                if (!(elementType instanceof NumberType))
                    throw new InvalidRequestException(format("Function %s requires a numeric set/list argument, " +
                                                             "but found argument %s of type %s",
                                                             name, arg, argType.asCQL3Type()));
            }

            @Override
            public String toString()
            {
                return "numeric_set_or_list";
            }
        };
    }

    /**
     * @return a function parameter definition that accepts values of type {@link MapType}, independently of the types
     * of the map keys and values.
     */
    static FunctionParameter anyMap()
    {
        return new FunctionParameter()
        {
            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                if (!argType.isUDT() && !(argType instanceof MapType))
                    throw new InvalidRequestException(format("Function %s requires a map argument, " +
                                                             "but found argument %s of type %s",
                                                             name, arg, argType.asCQL3Type()));
            }

            @Override
            public String toString()
            {
                return "map";
            }
        };
    }

    /**
     * @param type the type of the vector elements
     * @return a function parameter definition that accepts values of type {@link VectorType} with elements of the
     * specified {@code type} and any dimensions.
     */
    static FunctionParameter vector(CQL3Type type)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             @Nullable List<AbstractType<?>> inferredTypes)
            {
                if (arg instanceof Selectable.WithArrayLiteral)
                    return VectorType.getInstance(type.getType(), ((Selectable.WithArrayLiteral) arg).getSize());

                AbstractType<?> inferred = arg.getCompatibleTypeIfKnown(keyspace);
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
                else if (argType instanceof ListType) // if it's terminal it will be a list
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
