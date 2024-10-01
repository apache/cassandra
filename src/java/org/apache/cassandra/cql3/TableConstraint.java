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

public class TableConstraint
{
    public final ColumnIdentifier name;
    public final ConstraintCondition constraintCondition;

    public final static Serializer serializer = new Serializer();

    public final static class Raw {
        public final ColumnIdentifier name;
        public final ConstraintCondition constraintCondition;

        public Raw(ColumnIdentifier name, ConstraintCondition constraintCondition)
        {
            this.name = name;
            this.constraintCondition = constraintCondition;
        }

        public TableConstraint prepare()
        {
            return new TableConstraint(name, constraintCondition);
        }
    }

    public TableConstraint(ColumnIdentifier name, ConstraintCondition constraintCondition)
    {
        this.name = name;
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

    public static class Serializer
    {
        public void serialize(TableConstraint columnConstraint, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(columnConstraint.name.toString());
            columnConstraint.constraintCondition.serialize(columnConstraint.constraintCondition, out, version);
        }

        public TableConstraint deserialize(DataInputPlus in, String keyspace, AbstractType<?> columnType, Types types, UserFunctions functions, Version version) throws IOException
        {
            String nameText = in.readUTF();
            ColumnIdentifier identifier = new ColumnIdentifier(nameText, true);
            return new TableConstraint(identifier, null);
        }
    }
}
