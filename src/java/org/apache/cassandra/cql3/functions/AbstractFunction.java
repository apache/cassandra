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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQL3Type.Tuple;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

import org.apache.commons.lang3.text.StrBuilder;

import static java.util.stream.Collectors.toList;

/**
 * Base class for our native/hardcoded functions.
 */
public abstract class AbstractFunction implements Function
{
    protected final FunctionName name;
    protected final List<AbstractType<?>> argTypes;
    protected final AbstractType<?> returnType;

    protected AbstractFunction(FunctionName name, List<AbstractType<?>> argTypes, AbstractType<?> returnType)
    {
        this.name = name;
        this.argTypes = argTypes;
        this.returnType = returnType;
    }

    public FunctionName name()
    {
        return name;
    }

    public List<AbstractType<?>> argTypes()
    {
        return argTypes;
    }

    public AbstractType<?> returnType()
    {
        return returnType;
    }

    public List<String> argumentsList()
    {
        return argTypes().stream()
                         .map(AbstractType::asCQL3Type)
                         .map(CQL3Type::toString)
                         .collect(toList());
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof AbstractFunction))
            return false;

        AbstractFunction that = (AbstractFunction)o;
        return Objects.equal(this.name, that.name)
            && Objects.equal(this.argTypes, that.argTypes)
            && Objects.equal(this.returnType, that.returnType);
    }

    public void addFunctionsTo(List<Function> functions)
    {
        functions.add(this);
    }

    public boolean referencesUserType(ByteBuffer name)
    {
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, argTypes, returnType);
    }

    public final AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        // We should ignore the fact that the receiver type is frozen in our comparison as functions do not support
        // frozen types for return type
        AbstractType<?> returnType = returnType();
        if (receiver.type.isFreezable() && !receiver.type.isMultiCell())
            returnType = returnType.freeze();

        if (receiver.type.equals(returnType))
            return AssignmentTestable.TestResult.EXACT_MATCH;

        if (receiver.type.isValueCompatibleWith(returnType))
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
    }

    @Override
    public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
    {
        return returnType();
    }

    @Override
    public String toString()
    {
        return new CqlBuilder().append(name)
                              .append(" : (")
                              .appendWithSeparators(argTypes, (b, t) -> b.append(toCqlString(t)), ", ")
                              .append(") -> ")
                              .append(returnType)
                              .toString();
    }

    public String elementKeyspace()
    {
        return name.keyspace;
    }

    public String elementName()
    {
        return new CqlBuilder().append(name.name)
                               .append('(')
                               .appendWithSeparators(argTypes, (b, t) -> b.append(toCqlString(t)), ", ")
                               .append(')')
                               .toString();
    }

    /**
     * Converts the specified type into its CQL representation.
     *
     * <p>For user function and aggregates tuples need to be handle in a special way as they are frozen by nature
     * but the frozen keyword should not appear in their CQL definition.</p>
     *
     * @param type the type
     * @return the CQL representation of the specified type
     */
    protected String toCqlString(AbstractType<?> type)
    {
        return type.isTuple() ? ((Tuple) type.asCQL3Type()).toString(false)
                              : type.asCQL3Type().toString();
    }

    @Override
    public String columnName(List<String> columnNames)
    {
        return new StrBuilder(name().toString()).append('(')
                                                .appendWithSeparators(columnNames, ", ")
                                                .append(')')
                                                .toString();
    }

    /*
     * We need to compare the CQL3 representation of the type because comparing
     * the AbstractType will fail for example if a UDT has been changed.
     * Reason is that UserType.equals() takes the field names and types into account.
     * Example CQL sequence that would fail when comparing AbstractType:
     *    CREATE TYPE foo ...
     *    CREATE FUNCTION bar ( par foo ) RETURNS foo ...
     *    ALTER TYPE foo ADD ...
     * or
     *    ALTER TYPE foo ALTER ...
     * or
     *    ALTER TYPE foo RENAME ...
     */
    public boolean typesMatch(List<AbstractType<?>> types)
    {
        if (argTypes().size() != types.size())
            return false;

        for (int i = 0; i < argTypes().size(); i++)
            if (!typesMatch(argTypes().get(i), types.get(i)))
                return false;

        return true;
    }

    private static boolean typesMatch(AbstractType<?> t1, AbstractType<?> t2)
    {
        return t1.freeze().asCQL3Type().toString().equals(t2.freeze().asCQL3Type().toString());
    }
}
