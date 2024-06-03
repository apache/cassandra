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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.tcm.serialization.Version;

public class ConstraintScalarCondition implements ConstraintCondition
{
    public final ColumnIdentifier param;
    public final Operator relationType;
    public final String term;

    public static Serializer serializer = new Serializer();

    public final static class Raw {
        public final ColumnIdentifier param;
        public final Operator relationType;
        public final String term;

        public Raw(ColumnIdentifier param, Operator relationType, String term)
        {
            this.param = param;
            this.relationType = relationType;
            this.term = term;
        }

        public ConstraintScalarCondition prepare()
        {
            return new ConstraintScalarCondition(param, relationType, term);
        }
    }

    public ConstraintScalarCondition(ColumnIdentifier param, Operator relationType, String term)
    {
        this.param = param;
        this.relationType = relationType;
        this.term = term;
    }

    public void checkCondition(Map<String, String> columnValues, ColumnMetadata columnMetadata, TableMetadata tableMetadata)
    {
        int columnValue = Integer.parseInt(columnValues.get(param.toString()));
        int sizeConstraint = Integer.parseInt(term);

        switch (relationType)
        {
            case EQ:
                if (columnValue != sizeConstraint)
                    throw new ConstraintViolationException(param + " value length should be exactly " + sizeConstraint);
                break;
            case NEQ:
                if (columnValue == sizeConstraint)
                    throw new ConstraintViolationException(param + " value length different than " + sizeConstraint);
                break;
            case GT:
                if (columnValue <= sizeConstraint)
                    throw new ConstraintViolationException(param + " value length should be larger than " + sizeConstraint);
                break;
            case LT:
                if (columnValue >= sizeConstraint)
                    throw new ConstraintViolationException(param + " value length should be smaller than " + sizeConstraint);
                break;
            case GTE:
                if (columnValue < sizeConstraint)
                    throw new ConstraintViolationException(param + " value length should be larger or equal than " + sizeConstraint);
                break;
            case LTE:
                if (columnValue > sizeConstraint)
                    throw new ConstraintViolationException(param + " value length should be smaller or equala than " + sizeConstraint);
                break;
            default:
                throw new ConstraintViolationException("Invalid relation type: " + relationType);
        }
    }

    public void validateCondition(ColumnMetadata columnMetadata, TableMetadata tableMetadata)
    {
        if (!(tableMetadata.getColumn(param).type instanceof org.apache.cassandra.db.marshal.NumberType))
            throw new ConstraintViolationException(param + " is not a number");
    }

    @Override
    public String toString()
    {
        return String.format("%s %s %s", param, relationType, term);
    }

    public void serialize(ConstraintCondition constraintFunctionCondition, DataOutputPlus out, Version version) throws IOException
    {
        serializer.serialize((ConstraintScalarCondition) constraintFunctionCondition, out, version);
    }

    @Override
    public ConstraintCondition deserialize(DataInputPlus in, String keyspace, AbstractType<?> columnType, Types types, UserFunctions functions, Version version) throws IOException
    {
        return serializer.deserialize(in, keyspace, columnType, types, functions, version);
    }

    public static class Serializer
    {
        public void serialize(ConstraintScalarCondition constraintFunctionCondition, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(constraintFunctionCondition.param.toString());
            out.writeUTF(constraintFunctionCondition.relationType.toString());
            out.writeUTF(constraintFunctionCondition.term);
        }

        public ConstraintScalarCondition deserialize(DataInputPlus in, String keyspace, AbstractType<?> columnType, Types types, UserFunctions functions, Version version) throws IOException
        {
            ColumnIdentifier param = new ColumnIdentifier(in.readUTF(), true);
            Operator relationType = Operator.valueOf(in.readUTF());
            final String term = in.readUTF();
            return new ConstraintScalarCondition(param, relationType, term);
        }
    }
}
