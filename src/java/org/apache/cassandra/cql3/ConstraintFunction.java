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
import java.util.stream.Collectors;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.serialization.Version;

public class ConstraintFunction
{
    public final CqlConstraintFunctionExecutor executor;
    public final List<ColumnIdentifier> arg;

    public static Serializer serializer = new Serializer();

    public ConstraintFunction(CqlConstraintFunctionExecutor executor, List<ColumnIdentifier> arg)
    {
        this.executor = executor;
        this.arg = arg;
    }


    public void checkConstraint(Operator relationType, String term, TableMetadata tableMetadata, Map<String, String> columnValues)
    {
        executor.checkConstraint(this.arg, relationType, term, tableMetadata, columnValues);
    }

    public void validateConstraint(Operator relationType, String term, TableMetadata tableMetadata)
    {
        executor.validate(this.arg, relationType, term, tableMetadata);
    }

    @Override
    public String toString()
    {
        List<String> argsString = arg.stream().map(a -> a.toCQLString()).collect(Collectors.toList());
        String args = String.join(", ", argsString);
        return String.format("%s(%s)", executor.getName(), args);
    }

    public static class Serializer
    {
        public void serialize(ConstraintFunction constraintFunction, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(constraintFunction.executor.getClass().getName());
            out.writeUnsignedVInt32(constraintFunction.arg.size());
            for (ColumnIdentifier arg : constraintFunction.arg)
            {
                out.writeUTF(arg.toString());
            }
        }

        public ConstraintFunction deserialize(DataInputPlus in) throws IOException
        {
            String executorClass = in.readUTF();
            CqlConstraintFunctionExecutor executor;
            try
            {
                executor = (CqlConstraintFunctionExecutor) Class.forName(executorClass).getConstructor().newInstance();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            int argCount = in.readUnsignedVInt32();
            List<ColumnIdentifier> arg = new ArrayList<>();
            for (int i = 0; i < argCount; i++)
            {
                arg.add(new ColumnIdentifier(in.readUTF(), true));
            }
            return new ConstraintFunction(executor, arg);
        }
    }
}
