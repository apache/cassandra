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

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.SetType;
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
     * @return the inferred data type of the parameter, or {@link null} it isn't possible to infer it
     */
    @Nullable
    default AbstractType<?> inferType(String keyspace, AssignmentTestable arg, @Nullable AbstractType<?> receiverType)
    {
        return arg.getCompatibleTypeIfKnown(keyspace);
    }

    void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType);

    /**
     * @param type the accepted data type
     * @return a function parameter definition that accepts values of a specific data type
     */
    public static FunctionParameter fixed(AbstractType<?> type)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace, AssignmentTestable arg, AbstractType<?> receiverType)
            {
                AbstractType<?> inferred = arg.getCompatibleTypeIfKnown(keyspace);
                return inferred != null ? inferred : type;
            }

            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                if (argType.testAssignment(type) == NOT_ASSIGNABLE)
                    throw new InvalidRequestException(format("Function %s requires an argument of type %s, " +
                                                             "but found argument %s of type %s",
                                                             name, type, arg, argType.asCQL3Type()));
            }

            @Override
            public String toString()
            {
                return type.toString();
            }
        };
    }

    /**
     * @param inferFromReceiver whether the parameter should try to use the function receiver to infer its data type
     * @return a function parameter definition that accepts columns of any data type
     */
    public static FunctionParameter anyType(boolean inferFromReceiver)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace, AssignmentTestable arg, AbstractType<?> receiverType)
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
     * @return a function parameter definition that accepts values of type {@link CollectionType}, independently of the
     * types of its elements.
     */
    public static FunctionParameter anyCollection()
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
    public static FunctionParameter setOrList()
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
    public static FunctionParameter numericSetOrList()
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
    public static FunctionParameter anyMap()
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
}
