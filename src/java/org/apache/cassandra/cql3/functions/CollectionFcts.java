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
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Native CQL functions for collections (sets, list and maps).
 * <p>
 * All the functions provided here are {@link NativeScalarFunction}, and they are meant to be applied to single
 * collection values to perform some kind of aggregation with the elements of the collection argument. When possible,
 * the implementation of these aggregation functions is based on the accross-rows aggregation functions available on
 * {@link AggregateFcts}, so both across-rows and within-collection aggregations have the same behaviour.
 */
public class CollectionFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(new FunctionFactory("map_keys", FunctionParameter.anyMap())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeMapKeysFunction(name.name, (MapType<?, ?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("map_values", FunctionParameter.anyMap())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeMapValuesFunction(name.name, (MapType<?, ?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("collection_count", FunctionParameter.anyCollection())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeCollectionCountFunction(name.name, (CollectionType<?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("collection_min", FunctionParameter.setOrList())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeCollectionMinFunction(name.name, (CollectionType<?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("collection_max", FunctionParameter.setOrList())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeCollectionMaxFunction(name.name, (CollectionType<?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("collection_sum", FunctionParameter.numericSetOrList())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeCollectionSumFunction(name.name, (CollectionType<?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("collection_avg", FunctionParameter.numericSetOrList())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeCollectionAvgFunction(name.name, (CollectionType<?>) argTypes.get(0));
            }
        });
    }

    /**
     * Returns a native scalar function for getting the keys of a map column, as a set.
     *
     * @param name      the name of the function
     * @param inputType the type of the map argument by the returned function
     * @param <K>       the class of the map argument keys
     * @param <V>       the class of the map argument values
     * @return a function returning a serialized set containing the keys of the map passed as argument
     */
    private static <K, V> NativeScalarFunction makeMapKeysFunction(String name, MapType<K, V> inputType)
    {
        SetType<K> outputType = SetType.getInstance(inputType.getKeysType(), false);

        return new NativeScalarFunction(name, outputType, inputType)
        {
            @Override
            public ByteBuffer execute(Arguments arguments)
            {
                if (arguments.containsNulls())
                    return null;

                Map<K, V> map = arguments.get(0);
                Set<K> keys = map.keySet();
                return outputType.decompose(keys);
            }
        };
    }

    /**
     * Returns a native scalar function for getting the values of a map column, as a list.
     *
     * @param name      the name of the function
     * @param inputType the type of the map argument accepted by the returned function
     * @param <K>       the class of the map argument keys
     * @param <V>       the class of the map argument values
     * @return a function returning a serialized list containing the values of the map passed as argument
     */
    private static <K, V> NativeScalarFunction makeMapValuesFunction(String name, MapType<K, V> inputType)
    {
        ListType<V> outputType = ListType.getInstance(inputType.getValuesType(), false);

        return new NativeScalarFunction(name, outputType, inputType)
        {
            @Override
            public ByteBuffer execute(Arguments arguments)
            {
                if (arguments.containsNulls())
                    return null;

                Map<K, V> map = arguments.get(0);
                List<V> values = ImmutableList.copyOf(map.values());
                return outputType.decompose(values);
            }
        };
    }

    /**
     * Returns a native scalar function for getting the number of elements in a collection.
     *
     * @param name      the name of the function
     * @param inputType the type of the collection argument accepted by the returned function
     * @param <T>       the type of the elements of the collection argument
     * @return a function returning the number of elements in the collection passed as argument, as a 32-bit integer
     */
    private static <T> NativeScalarFunction makeCollectionCountFunction(String name, CollectionType<T> inputType)
    {
        return new NativeScalarFunction(name, Int32Type.instance, inputType)
        {
            @Override
            public Arguments newArguments(ProtocolVersion version)
            {
                return FunctionArguments.newNoopInstance(version, 1);
            }

            @Override
            public ByteBuffer execute(Arguments arguments)
            {
                if (arguments.containsNulls())
                    return null;


                int size = inputType.size(arguments.get(0));
                return Int32Type.instance.decompose(size);
            }
        };
    }

    /**
     * Returns a native scalar function for getting the min element in a collection.
     *
     * @param name      the name of the function
     * @param inputType the type of the collection argument accepted by the returned function
     * @param <T>       the type of the elements of the collection argument
     * @return a function returning the min element in the collection passed as argument
     */
    private static <T> NativeScalarFunction makeCollectionMinFunction(String name, CollectionType<T> inputType)
    {
        AbstractType<?> elementsType = elementsType(inputType);
        NativeAggregateFunction function = elementsType.isCounter()
                                           ? AggregateFcts.minFunctionForCounter
                                           : AggregateFcts.makeMinFunction(elementsType);
        return new CollectionAggregationFunction(name, inputType, function);
    }

    /**
     * Returns a native scalar function for getting the max element in a collection.
     *
     * @param name      the name of the function
     * @param inputType the type of the collection argument accepted by the returned function
     * @param <T>       the type of the elements of the collection argument
     * @return a function returning the max element in the collection passed as argument
     */
    private static <T> NativeScalarFunction makeCollectionMaxFunction(String name, CollectionType<T> inputType)
    {
        AbstractType<?> elementsType = elementsType(inputType);
        NativeAggregateFunction function = elementsType.isCounter()
                                           ? AggregateFcts.maxFunctionForCounter
                                           : AggregateFcts.makeMaxFunction(elementsType);
        return new CollectionAggregationFunction(name, inputType, function);
    }

    /**
     * Returns a native scalar function for getting the sum of the elements in a numeric collection.
     * </p>
     * The value returned by the function is of the same type as elements of its input collection, so there is a risk
     * of overflow if the sum of the values exceeds the maximum value that the type can represent.
     *
     * @param name      the name of the function
     * @param inputType the type of the collection argument accepted by the returned function
     * @param <T>       the type of the elements of the collection argument
     * @return a function returning the sum of the elements in the collection passed as argument
     */
    private static <T> NativeScalarFunction makeCollectionSumFunction(String name, CollectionType<T> inputType)
    {
        CQL3Type elementsType = elementsType(inputType).asCQL3Type();
        NativeAggregateFunction function = getSumFunction((CQL3Type.Native) elementsType);
        return new CollectionAggregationFunction(name, inputType, function);
    }

    private static NativeAggregateFunction getSumFunction(CQL3Type.Native type)
    {
        switch (type)
        {
            case TINYINT:
                return AggregateFcts.sumFunctionForByte;
            case SMALLINT:
                return AggregateFcts.sumFunctionForShort;
            case INT:
                return AggregateFcts.sumFunctionForInt32;
            case BIGINT:
                return AggregateFcts.sumFunctionForLong;
            case FLOAT:
                return AggregateFcts.sumFunctionForFloat;
            case DOUBLE:
                return AggregateFcts.sumFunctionForDouble;
            case VARINT:
                return AggregateFcts.sumFunctionForVarint;
            case DECIMAL:
                return AggregateFcts.sumFunctionForDecimal;
            default:
                throw new AssertionError("Expected numeric collection but found " + type);
        }
    }

    /**
     * Returns a native scalar function for getting the average of the elements in a numeric collection.
     * </p>
     * The average of an empty collection returns zero. The value returned by the function is of the same type as the
     * elements of its input collection, so if those don't have a decimal part then the returned average won't have a
     * decimal part either.
     *
     * @param name      the name of the function
     * @param inputType the type of the collection argument accepted by the returned function
     * @param <T>       the type of the elements of the collection argument
     * @return a function returning the average value of the elements in the collection passed as argument
     */
    private static <T> NativeScalarFunction makeCollectionAvgFunction(String name, CollectionType<T> inputType)
    {
        CQL3Type elementsType = elementsType(inputType).asCQL3Type();
        NativeAggregateFunction function = getAvgFunction((CQL3Type.Native) elementsType);
        return new CollectionAggregationFunction(name, inputType, function);
    }

    private static NativeAggregateFunction getAvgFunction(CQL3Type.Native type)
    {
        switch (type)
        {
            case TINYINT:
                return AggregateFcts.avgFunctionForByte;
            case SMALLINT:
                return AggregateFcts.avgFunctionForShort;
            case INT:
                return AggregateFcts.avgFunctionForInt32;
            case BIGINT:
                return AggregateFcts.avgFunctionForLong;
            case FLOAT:
                return AggregateFcts.avgFunctionForFloat;
            case DOUBLE:
                return AggregateFcts.avgFunctionForDouble;
            case VARINT:
                return AggregateFcts.avgFunctionForVarint;
            case DECIMAL:
                return AggregateFcts.avgFunctionForDecimal;
            default:
                throw new AssertionError("Expected numeric collection but found " + type);
        }
    }

    /**
     * @return the type of the elements of the specified collection type.
     */
    private static AbstractType<?> elementsType(CollectionType<?> type)
    {
        if (type.kind == CollectionType.Kind.LIST)
        {
            return ((ListType<?>) type).getElementsType();
        }

        if (type.kind == CollectionType.Kind.SET)
        {
            return ((SetType<?>) type).getElementsType();
        }

        throw new AssertionError("Cannot get the element type of: " + type);
    }

    /**
     * A {@link NativeScalarFunction} for aggregating the elements of a collection according to the aggregator of
     * a certain {@link NativeAggregateFunction}.
     * <p>
     * {@link NativeAggregateFunction} is meant to be used for aggregating values accross rows, but here we use that
     * function to aggregate the elements of a single collection value. That way, functions such as {@code avg} and
     * {@code collection_avg} should have the same behaviour when applied to row columns or collection elements.
     */
    private static class CollectionAggregationFunction extends NativeScalarFunction
    {
        private final CollectionType<?> inputType;
        private final NativeAggregateFunction aggregateFunction;

        public CollectionAggregationFunction(String name,
                                             CollectionType<?> inputType,
                                             NativeAggregateFunction aggregateFunction)
        {
            super(name, aggregateFunction.returnType, inputType);
            this.inputType = inputType;
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public Arguments newArguments(ProtocolVersion version)
        {
            return FunctionArguments.newNoopInstance(version, 1);
        }

        @Override
        public ByteBuffer execute(Arguments arguments)
        {
            if (arguments.containsNulls())
                return null;

            Arguments args = aggregateFunction.newArguments(arguments.getProtocolVersion());
            AggregateFunction.Aggregate aggregate = aggregateFunction.newAggregate();

            inputType.forEach(arguments.get(0), element -> {
                args.set(0, element);
                aggregate.addInput(args);
            });

            return aggregate.compute(arguments.getProtocolVersion());
        }
    }
}
