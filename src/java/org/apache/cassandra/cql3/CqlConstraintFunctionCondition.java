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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class CqlConstraintFunctionCondition implements ConstraintCondition
{
    public final ConstraintFunction function;
    public final Operator relationType;
    public final String term;

    public static Serializer serializer = new Serializer();

    public final static class Raw
    {
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

    @Override
    public IVersionedAsymmetricSerializer<ConstraintCondition, ConstraintCondition> getSerializer()
    {
        return serializer;
    }

    @Override
    public void evaluate(Map<String, String> columnValues, ColumnMetadata columnMetadata, TableMetadata tableMetadata)
    {
        if (function != null)
            function.checkConstraint(relationType, term, tableMetadata, columnValues);
    }

    @Override
    public void validate(Map<String, ColumnMetadata> columnMetadata, TableMetadata tableMetadata)
    {
        if (columnMetadata != null)
            validateArgs(columnMetadata);
        if (function != null)
            function.validateConstraint(relationType, term, tableMetadata);
    }

    void validateArgs(Map<String, ColumnMetadata> columnMetadata)
    {
        if (function == null)
            throw new ConstraintInvalidException("Function parameter should be the column name");

        for (ColumnIdentifier param : function.arg)
            if (!columnMetadata.containsKey(param.toString()))
                throw new ConstraintInvalidException("Function parameter should be the column name");
    }

    @Override
    public String toString()
    {
        return String.format("%s %s %s", function, relationType, term);
    }

    public static class Serializer implements IVersionedAsymmetricSerializer<ConstraintCondition, ConstraintCondition>
    {
        @Override
        public void serialize(ConstraintCondition constraintCondition, DataOutputPlus out, int version) throws IOException
        {
            CqlConstraintFunctionCondition condition = (CqlConstraintFunctionCondition) constraintCondition;
            ConstraintFunction.serializer.serialize(condition.function, out, version);
            out.writeUTF(condition.relationType.toString());
            out.writeUTF(condition.term);
        }

        @Override
        public ConstraintCondition deserialize(DataInputPlus in, int version) throws IOException
        {
            ConstraintFunction constraintFunction = ConstraintFunction.serializer.deserialize(in, version);
            Operator relationType = Operator.valueOf(in.readUTF());
            final String term = in.readUTF();
            return new CqlConstraintFunctionCondition(constraintFunction, relationType, term);
        }

        @Override
        public long serializedSize(ConstraintCondition constraintCondition, int version)
        {
            CqlConstraintFunctionCondition condition = (CqlConstraintFunctionCondition) constraintCondition;
            return TypeSizes.sizeof(condition.term)
                   + TypeSizes.sizeof(condition.relationType.toString())
                   + ConstraintFunction.serializer.serializedSize(condition.function, version);
        }
    }
}
