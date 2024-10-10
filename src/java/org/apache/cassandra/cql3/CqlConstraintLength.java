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


import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class CqlConstraintLength implements CqlConstraintFunctionExecutor
{
    @Override
    public String getName()
    {
        return "LENGTH";
    }

    @Override
    public void evaluate(List<ColumnIdentifier> args, Operator relationType, String term, TableMetadata tableMetadata, Map<String, String> columnValues)
    {
        ColumnMetadata columnMetadata = tableMetadata.getColumn(args.get(0));
        if (!columnValues.containsKey(columnMetadata.name.toString()))
            throw new ConstraintViolationException(columnMetadata.name + " is not an existing column name.");

        String columnValue = columnValues.get(columnMetadata.name.toString());
        int valueLength = stripColumnValue(columnValue).length();
        int sizeConstraint = Integer.parseInt(term);

        switch (relationType)
        {
            case EQ:
                if (valueLength != sizeConstraint)
                    throw new ConstraintViolationException(columnMetadata.name + " value length should be exactly " + sizeConstraint);
                break;
            case NEQ:
                if (valueLength == sizeConstraint)
                    throw new ConstraintViolationException(columnMetadata.name + " value length different than " + sizeConstraint);
                break;
            case GT:
                if (valueLength <= sizeConstraint)
                    throw new ConstraintViolationException(columnMetadata.name + " value length should be larger than " + sizeConstraint);
                break;
            case LT:
                if (valueLength >= sizeConstraint)
                    throw new ConstraintViolationException(columnMetadata.name + " value length should be smaller than " + sizeConstraint);
                break;
            case GTE:
                if (valueLength < sizeConstraint)
                    throw new ConstraintViolationException(columnMetadata.name + " value length should be larger or equal than " + sizeConstraint);
                break;
            case LTE:
                if (valueLength > sizeConstraint)
                    throw new ConstraintViolationException(columnMetadata.name + " value length should be smaller or equala than " + sizeConstraint);
                break;
            default:
                throw new ConstraintViolationException("Invalid relation type: " + relationType);
        }
    }

    @Override
    public void validate(List<ColumnIdentifier> args, Operator relationType, String term, TableMetadata tableMetadata)
    {
        if (args.size() != 1)
            throw new ConstraintInvalidException("LENGTH requires exactly one argument");

        ColumnMetadata columnMetadata = tableMetadata.getColumn(args.get(0));
        if (columnMetadata.type.getClass() != UTF8Type.class && columnMetadata.type.getClass() != AsciiType.class)
        {
            throw new ConstraintInvalidException("Column should be of type "
                                                 + UTF8Type.class + " or " + AsciiType.class
                                                 + " but got " + columnMetadata.type.getClass());
        }
    }
}
