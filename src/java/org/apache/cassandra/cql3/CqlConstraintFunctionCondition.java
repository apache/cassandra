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
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.tcm.serialization.Version;

public class CqlConstraintFunctionCondition implements ConstraintCondition
{
    public final ConstraintFunction function;
    public final Operator relationType;
    public final String term;

    public static Serializer serializer = new Serializer();

    public final static class Raw {
        public final ConstraintFunction function;
        public final Operator relationType;
        public final String term;

        public Raw(ConstraintFunction function, Operator relationType, String term)
        {
            this.function = function;
            this.relationType = relationType;
            this.term = term;
        }

        public CqlConstraintFunctionCondition prepare()
        {
            return new CqlConstraintFunctionCondition(function, relationType, term);
        }
    }

    public CqlConstraintFunctionCondition(ConstraintFunction function, Operator relationType, String term)
    {
        this.function = function;
        this.relationType = relationType;
        this.term = term;
    }

    public void checkCondition(Map<String, String> columnValues, ColumnMetadata columnMetadata, TableMetadata tableMetadata)
    {
        if (function != null)
            function.checkConstraint(relationType, term, tableMetadata, columnValues);
    }

    public void validateCondition(ColumnMetadata columnMetadata, TableMetadata tableMetadata)
    {
        if (columnMetadata != null)
            validateArgs(columnMetadata);
        if (function != null)
            function.validateConstraint(relationType, term, tableMetadata);
    }

    void validateArgs(ColumnMetadata columnMetadata)
    {
        if (function != null && !Objects.equals(function.arg.get(0).toCQLString(), columnMetadata.name.toCQLString()))
            throw new ConstraintViolationException("Function parameter should be the column name");
    }

    @Override
    public String toString()
    {
        return String.format("%s %s %s", function, relationType, term);
    }

    @Override
    public void serialize(ConstraintCondition constraintFunctionCondition, DataOutputPlus out, Version version) throws IOException
    {
        serializer.serialize((CqlConstraintFunctionCondition) constraintFunctionCondition, out, version);
    }

    @Override
    public ConstraintCondition deserialize(DataInputPlus in, String keyspace, AbstractType<?> columnType, Types types, UserFunctions functions, Version version) throws IOException
    {
        return serializer.deserialize(in, keyspace, columnType, types, functions, version);
    }

    public static class Serializer
    {
        public void serialize(CqlConstraintFunctionCondition cqlConstraintFunctionCondition, DataOutputPlus out, Version version) throws IOException
        {
            ConstraintFunction.serializer.serialize(cqlConstraintFunctionCondition.function, out, version);
            out.writeUTF(cqlConstraintFunctionCondition.relationType.toString());
            out.writeUTF(cqlConstraintFunctionCondition.term);
        }

        public CqlConstraintFunctionCondition deserialize(DataInputPlus in, String keyspace, AbstractType<?> columnType, Types types, UserFunctions functions, Version version) throws IOException
        {
            ConstraintFunction constraintFunction = ConstraintFunction.serializer.deserialize(in);
            Operator relationType = Operator.valueOf(in.readUTF());
            final String term = in.readUTF();
            return new CqlConstraintFunctionCondition(constraintFunction, relationType, term);
        }
    }
}
