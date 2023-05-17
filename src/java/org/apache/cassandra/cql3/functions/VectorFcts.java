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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.lucene.index.VectorSimilarityFunction;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class VectorFcts
{
    public static Logger logger = LoggerFactory.getLogger(VectorFcts.class);

    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(new SimilarityFunctionFactory("cosine_similarity", VectorSimilarityFunction.COSINE));
        functions.add(new SimilarityFunctionFactory("dotproduct_similarity", VectorSimilarityFunction.DOT_PRODUCT));
        functions.add(new SimilarityFunctionFactory("euclidean_similarity", VectorSimilarityFunction.EUCLIDEAN));
    }

    private static FunctionParameter floatVector()
    {
        return new FunctionParameter()
        {
            @Override
            public void validateType(FunctionName name, AssignmentTestable arg, AbstractType<?> argType)
            {
                if (!argType.isVector())
                    throw new InvalidRequestException(format("Function %s requires a vector argument, " +
                                                             "but found argument %s of type %s",
                                                             name, arg, argType.asCQL3Type()));
                // TODO validate that it's float, when we have other vectors to worry about
            }

            @Override
            public String toString()
            {
                return "float vector";
            }
        };
    }

    private static class SimilarityFunctionFactory extends FunctionFactory
    {
        private final VectorSimilarityFunction similarityFunction;

        public SimilarityFunctionFactory(String name, VectorSimilarityFunction similarityFunction)
        {
            super(name, VectorFcts.floatVector(), VectorFcts.floatVector());
            this.similarityFunction = similarityFunction;
        }

        @Override
        protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
        {
            var vectorType = (VectorType) argTypes.get(0);
            return new NativeScalarFunction(name.name, vectorType, vectorType)
            {
                @Override
                public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
                {
                    ByteBuffer b1 = parameters.get(0);
                    ByteBuffer b2 = parameters.get(0);
                    if (b1 == null || b2 == null)
                        return null;

                    var v1 = vectorType.serializer.deserialize(b1);
                    var v2 = vectorType.serializer.deserialize(b1);
                    var vCosine = similarityFunction.compare(v1, v2);
                    return FloatType.instance.getSerializer().serialize(vCosine);
                }
            };
        }
    }
}
