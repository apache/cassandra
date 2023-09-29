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

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

public abstract class VectorFcts
{
    private static boolean isFloatVector(AbstractType<?> type)
    {
        type = type.unwrap();
        return type instanceof VectorType && ((VectorType<?>) type).getElementsType() == FloatType.instance;
    }

    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(similarity_function("similarity_cosine", VectorSimilarityFunction.COSINE));
        functions.add(similarity_function("similarity_euclidean", VectorSimilarityFunction.EUCLIDEAN));
        functions.add(similarity_function("similarity_dot_product", VectorSimilarityFunction.DOT_PRODUCT));
    }

    private static FunctionFactory similarity_function(String name, VectorSimilarityFunction f)
    {
        return new FunctionFactory(name,
                                   FunctionParameter.sameAs(1, FunctionParameter.vector(CQL3Type.Native.FLOAT)),
                                   FunctionParameter.sameAs(0, FunctionParameter.vector(CQL3Type.Native.FLOAT)))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                // check that all arguments have the same vector dimensions
                VectorType<Float> firstArgType = (VectorType<Float>) argTypes.get(0);
                int dimensions = firstArgType.dimension;
                if (!argTypes.stream().allMatch(t -> ((VectorType<?>) t).dimension == dimensions))
                    throw new InvalidRequestException("All arguments must have the same vector dimensions");
                return createSimilarityFunction(name.name, firstArgType, f);
            }
        };
    }

    private static NativeFunction createSimilarityFunction(String name, VectorType<Float> type, VectorSimilarityFunction f)
    {
        return new NativeScalarFunction(name, FloatType.instance, type, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
            {
                var v1 = type.getSerializer().deserializeFloatArray(parameters.get(0));
                var v2 = type.getSerializer().deserializeFloatArray(parameters.get(1));
                return FloatType.instance.decompose(f.compare(v1, v2));
            }
        };
    }
}
