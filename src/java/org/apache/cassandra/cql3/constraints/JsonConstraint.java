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

package org.apache.cassandra.cql3.constraints;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.JsonUtils;

import static java.lang.String.format;

public class JsonConstraint extends ConstraintFunction
{
    private static final List<AbstractType<?>> SUPPORTED_TYPES = List.of(UTF8Type.instance, AsciiType.instance);

    public static final String FUNCTION_NAME = "JSON";

    public JsonConstraint(ColumnIdentifier columnName)
    {
        this(columnName, FUNCTION_NAME);
    }

    public JsonConstraint(ColumnIdentifier columnName, String name)
    {
        super(columnName, name);
    }

    @Override
    public void internalEvaluate(AbstractType<?> valueType, Operator relationType, String term, ByteBuffer columnValue)
    {
        try
        {
            JsonUtils.decodeJson(valueType.getString(columnValue));
        }
        catch (MarshalException ex)
        {
            throw new ConstraintViolationException(format("Value for column '%s' violated %s constraint as it is not a valid JSON.",
                                                          columnName.toCQLString(),
                                                          name));
        }
    }

    @Override
    public void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException
    {
    }

    @Override
    public List<AbstractType<?>> getSupportedTypes()
    {
        return SUPPORTED_TYPES;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof JsonConstraint))
            return false;

        JsonConstraint other = (JsonConstraint) o;

        return columnName.equals(other.columnName) && name.equals(other.name);
    }
}
