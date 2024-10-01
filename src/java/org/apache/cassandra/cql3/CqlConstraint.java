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
import java.util.UUID;

import com.google.common.base.Objects;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.tcm.serialization.Version;

public class CqlConstraint
{
    public ColumnIdentifier constraintName;
    public final ColumnIdentifier columnName;
    public final ConstraintCondition constraintCondition;

    public static Serializer serializer = new Serializer();

    public final static class Raw {
        public final ColumnIdentifier constraintName;
        public final ConstraintCondition constraintCondition;
        public ColumnIdentifier columnName;

        public Raw(ColumnIdentifier constraintName, ConstraintCondition constraintCondition)
        {
            this.constraintName = constraintName;
            this.constraintCondition = constraintCondition;
        }

        public CqlConstraint prepare(ColumnIdentifier columnName)
        {
            return new CqlConstraint(constraintName, columnName, constraintCondition);
        }

        public CqlConstraint prepareWithName(ColumnIdentifier constraintName)
        {
            return new CqlConstraint(constraintName, null, constraintCondition);
        }
    }

    public CqlConstraint(ColumnIdentifier constraintName, ColumnIdentifier columnName, ConstraintCondition constraintCondition)
    {
        if (constraintName == null)
        {
            final String randomConstraintName = UUID.randomUUID().toString().replace("-", "");
            this.constraintName = new ColumnIdentifier(randomConstraintName, false);
        }
        else
        {
            this.constraintName = constraintName;
        }
        this.columnName = columnName;
        this.constraintCondition = constraintCondition;
    }

    public void appendCqlTo(CqlBuilder builder)
    {
        builder.append(" CHECK ").append(toString());
    }

    public void checkConstraint(Map<String, String> columnValues, ColumnMetadata columnMetadata, TableMetadata tableMetadata)
    {
        constraintCondition.checkCondition(columnValues, columnMetadata, tableMetadata);
    }

    public void validateConstraint(ColumnMetadata columnMetadata, TableMetadata tableMetadata)
    {
        constraintCondition.validateCondition(columnMetadata, tableMetadata);
    }

    @Override
    public String toString()
    {
        return constraintCondition.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(constraintName);
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof CqlConstraint
               && Objects.equal(constraintName, ((CqlConstraint) obj).constraintName)
               && Objects.equal(constraintCondition, ((CqlConstraint) obj).constraintCondition);
    }


    public static class Serializer
    {
        public void serialize(CqlConstraint cqlConstraint, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(cqlConstraint.constraintName.toString());
            out.writeUTF(cqlConstraint.columnName.toString());
            out.writeUTF(cqlConstraint.constraintCondition.getClass().toString());
            cqlConstraint.constraintCondition.serialize(cqlConstraint.constraintCondition, out, version);
        }

        public CqlConstraint deserialize(DataInputPlus in, String keyspace, AbstractType<?> columnType, Types types, UserFunctions functions, Version version) throws IOException
        {
            String nameText = in.readUTF();
            String columnName = in.readUTF();
            ColumnIdentifier identifier = new ColumnIdentifier(nameText, true);
            ColumnIdentifier columnNameIdentifier = new ColumnIdentifier(columnName, true);
            String columnConstraintClassName = in.readUTF();
            ConstraintCondition condition = ConstraintSerializerFactory.getCqlConditionSerializer(columnConstraintClassName)
                                                                       .deserialize(in, keyspace, columnType, types, functions, version);
            return new CqlConstraint(identifier, columnNameIdentifier, condition);
        }
    }

    public static class ConstraintSerializerFactory
    {
        public static CqlConstraintSerializer getCqlConditionSerializer(String columnConstraintClassName)
        {
            if (columnConstraintClassName.equals(CqlConstraintFunctionCondition.class.getName()))
                return CqlConstraintFunctionCondition.serializer;
            else if (columnConstraintClassName.equals(ConstraintScalarCondition.class.getName()))
                return ConstraintScalarCondition.serializer;
            throw new IllegalArgumentException(String.format("Condition %s needs to have an implemented serializer", columnConstraintClassName));
        }
    }
}
