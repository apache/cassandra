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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.functions.types.ParseUtils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.Operator.EQ;
import static org.apache.cassandra.cql3.Operator.NEQ;

public class RegexpConstraint extends ConstraintFunction
{
    public static final String FUNCTION_NAME = "REGEXP";
    private static final List<AbstractType<?>> SUPPORTED_TYPES = List.of(UTF8Type.instance, AsciiType.instance);
    private static final List<Operator> ALLOWED_FUNCTION_OPERATORS = List.of(EQ, NEQ);

    private Pattern pattern;

    public RegexpConstraint(List<String> args)
    {
        super(FUNCTION_NAME, args);
    }

    @Override
    protected void internalEvaluate(AbstractType<?> valueType, Operator relationType, String regexp, ByteBuffer columnValue)
    {
        assert pattern != null;
        Matcher matcher = pattern.matcher(valueType.getString(columnValue));

        switch (relationType)
        {
            case EQ:
                if (!matcher.matches())
                    throw new ConstraintViolationException(format("Value does not match regular expression %s", regexp));
                break;
            case NEQ:
                if (matcher.matches())
                    throw new ConstraintViolationException(format("Value does match regular expression %s", regexp));
                break;
            default:
                throw new IllegalStateException("Unsupported operator: " + relationType);
        }
    }

    @Override
    public List<AbstractType<?>> getSupportedTypes()
    {
        return SUPPORTED_TYPES;
    }

    @Override
    public List<Operator> getSupportedOperators()
    {
        return ALLOWED_FUNCTION_OPERATORS;
    }

    @Override
    public void validate(ColumnMetadata columnMetadata, String regexp) throws InvalidConstraintDefinitionException
    {
        super.validate(columnMetadata, regexp);
        try
        {
            // compilation of a regexp every single time upon evaluation is not performance friendly
            // so we "cache" the compiled regexp for further reuse upon actual validation
            pattern = Pattern.compile(ParseUtils.unquote(regexp));
        }
        catch (Exception e)
        {
            throw new InvalidConstraintDefinitionException(format("String '%s' is not a valid regular expression", ParseUtils.unquote(regexp)));
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof RegexpConstraint))
            return false;

        RegexpConstraint other = (RegexpConstraint) o;

        return columnName.equals(other.columnName);
    }
}
