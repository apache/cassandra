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

package org.apache.cassandra.cql3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;

public class ConstraintFunctionExpression
{
    public final ConstraintFunction executor;
    public final List<ColumnIdentifier> arg;

    public static final Serializer serializer = new Serializer();

    public static final Map<String, ConstraintFunction> supportedConstraintFunctions = Map.of(
        LengthConstraint.class.getName(), new LengthConstraint()
    );

    public ConstraintFunctionExpression(ConstraintFunction executor, List<ColumnIdentifier> arg)
    {
        this.executor = executor;
        this.arg = arg;
    }


    public void checkConstraint(Operator relationType, String term, TableMetadata tableMetadata, Map<String, Term.Raw> columnValues)
    {
        executor.evaluate(this.arg, relationType, term, tableMetadata, columnValues);
    }

    public void validateConstraint(Operator relationType, String term, TableMetadata tableMetadata)
    {
        executor.validate(this.arg, relationType, term, tableMetadata);
    }

    public String toCqlString()
    {
        return toString();
    }

    @Override
    public String toString()
    {
        List<String> argsString = new ArrayList<>();
        for (ColumnIdentifier columnIdentifier : arg)
        {
            argsString.add(columnIdentifier.toCQLString());
        }
        String args = String.join(", ", argsString);
        return String.format("%s(%s)", executor.getName(), args);
    }

    public final static class Serializer implements IVersionedAsymmetricSerializer<ConstraintFunctionExpression, ConstraintFunctionExpression>
    {
        @Override
        public void serialize(ConstraintFunctionExpression constraintFunctionExpression, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(constraintFunctionExpression.executor.getClass().getName());
            out.writeUnsignedVInt32(constraintFunctionExpression.arg.size());
            for (ColumnIdentifier arg : constraintFunctionExpression.arg)
                out.writeUTF(arg.toString());
        }

        @Override
        public ConstraintFunctionExpression deserialize(DataInputPlus in, int version) throws IOException
        {
            String executorClass = in.readUTF();
            ConstraintFunction executor;
            try
            {
                executor = supportedConstraintFunctions.get(executorClass);
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }
            int argCount = in.readUnsignedVInt32();
            List<ColumnIdentifier> arg = new ArrayList<>();
            for (int i = 0; i < argCount; i++)
            {
                arg.add(new ColumnIdentifier(in.readUTF(), true));
            }
            return new ConstraintFunctionExpression(executor, arg);
        }

        @Override
        public long serializedSize(ConstraintFunctionExpression constraintFunctionExpression, int version)
        {
            long sizeInBytes = TypeSizes.sizeof(constraintFunctionExpression.executor.getClass().getName())
            + TypeSizes.sizeof(constraintFunctionExpression.arg.size());

            for (ColumnIdentifier id : constraintFunctionExpression.arg)
            {
                sizeInBytes += TypeSizes.sizeof(id.toString());
            }

            return sizeInBytes;
        }
    }
}
