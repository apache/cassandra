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

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ClusterMetadataFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(transformationKind);
    }

    public static final NativeScalarFunction transformationKind = new TransformationKind();
    private static final class TransformationKind extends NativeScalarFunction
    {

        private TransformationKind()
        {
            super("transformation_kind", UTF8Type.instance, Int32Type.instance);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            Number id = arguments.get(0);
            if (id.intValue() < 0 || id.intValue() > Transformation.Kind.values().length -1)
                throw new InvalidRequestException(id + " is not a valid Transformation.Kind id");

            Transformation.Kind kind = Transformation.Kind.fromId(id.intValue());
            return ByteBufferUtil.bytes(kind.name());

        }
    }
}
