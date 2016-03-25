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
package org.apache.cassandra.index.sasi.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndex;
import org.apache.cassandra.index.sasi.utils.TypeUtil;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Expression
{
    private static final Logger logger = LoggerFactory.getLogger(Expression.class);

    public enum Op
    {
        EQ, MATCH, PREFIX, SUFFIX, CONTAINS, NOT_EQ, RANGE;

        public static Op valueOf(Operator operator)
        {
            switch (operator)
            {
                case EQ:
                    return EQ;

                case NEQ:
                    return NOT_EQ;

                case LT:
                case GT:
                case LTE:
                case GTE:
                    return RANGE;

                case LIKE_PREFIX:
                    return PREFIX;

                case LIKE_SUFFIX:
                    return SUFFIX;

                case LIKE_CONTAINS:
                    return CONTAINS;

                case LIKE_MATCHES:
                    return MATCH;

                default:
                    throw new IllegalArgumentException("unknown operator: " + operator);
            }
        }
    }

    private final QueryController controller;

    public final AbstractAnalyzer analyzer;

    public final ColumnIndex index;
    public final AbstractType<?> validator;
    public final boolean isLiteral;

    @VisibleForTesting
    protected Op operation;

    public Bound lower, upper;
    public List<ByteBuffer> exclusions = new ArrayList<>();

    public Expression(Expression other)
    {
        this(other.controller, other.index);
        operation = other.operation;
    }

    public Expression(QueryController controller, ColumnIndex columnIndex)
    {
        this.controller = controller;
        this.index = columnIndex;
        this.analyzer = columnIndex.getAnalyzer();
        this.validator = columnIndex.getValidator();
        this.isLiteral = columnIndex.isLiteral();
    }

    @VisibleForTesting
    public Expression(String name, AbstractType<?> validator)
    {
        this(null, new ColumnIndex(UTF8Type.instance, ColumnDefinition.regularDef("sasi", "internal", name, validator), null));
    }

    public Expression setLower(Bound newLower)
    {
        lower = newLower == null ? null : new Bound(newLower.value, newLower.inclusive);
        return this;
    }

    public Expression setUpper(Bound newUpper)
    {
        upper = newUpper == null ? null : new Bound(newUpper.value, newUpper.inclusive);
        return this;
    }

    public Expression setOp(Op op)
    {
        this.operation = op;
        return this;
    }

    public Expression add(Operator op, ByteBuffer value)
    {
        boolean lowerInclusive = false, upperInclusive = false;
        switch (op)
        {
            case LIKE_PREFIX:
            case LIKE_SUFFIX:
            case LIKE_CONTAINS:
            case LIKE_MATCHES:
            case EQ:
                lower = new Bound(value, true);
                upper = lower;
                operation = Op.valueOf(op);
                break;

            case NEQ:
                // index expressions are priority sorted
                // and NOT_EQ is the lowest priority, which means that operation type
                // is always going to be set before reaching it in case of RANGE or EQ.
                if (operation == null)
                {
                    operation = Op.NOT_EQ;
                    lower = new Bound(value, true);
                    upper = lower;
                }
                else
                    exclusions.add(value);
                break;

            case LTE:
                upperInclusive = true;
            case LT:
                operation = Op.RANGE;
                upper = new Bound(value, upperInclusive);
                break;

            case GTE:
                lowerInclusive = true;
            case GT:
                operation = Op.RANGE;
                lower = new Bound(value, lowerInclusive);
                break;
        }

        return this;
    }

    public Expression addExclusion(ByteBuffer value)
    {
        exclusions.add(value);
        return this;
    }

    public boolean isSatisfiedBy(ByteBuffer value)
    {
        if (!TypeUtil.isValid(value, validator))
        {
            int size = value.remaining();
            if ((value = TypeUtil.tryUpcast(value, validator)) == null)
            {
                logger.error("Can't cast value for {} to size accepted by {}, value size is {}.",
                             index.getColumnName(),
                             validator,
                             FBUtilities.prettyPrintMemory(size));
                return false;
            }
        }

        if (lower != null)
        {
            // suffix check
            if (isLiteral)
            {
                if (!validateStringValue(value, lower.value))
                    return false;
            }
            else
            {
                // range or (not-)equals - (mainly) for numeric values
                int cmp = validator.compare(lower.value, value);

                // in case of (NOT_)EQ lower == upper
                if (operation == Op.EQ || operation == Op.NOT_EQ)
                    return cmp == 0;

                if (cmp > 0 || (cmp == 0 && !lower.inclusive))
                    return false;
            }
        }

        if (upper != null && lower != upper)
        {
            // string (prefix or suffix) check
            if (isLiteral)
            {
                if (!validateStringValue(value, upper.value))
                    return false;
            }
            else
            {
                // range - mainly for numeric values
                int cmp = validator.compare(upper.value, value);
                if (cmp < 0 || (cmp == 0 && !upper.inclusive))
                    return false;
            }
        }

        // as a last step let's check exclusions for the given field,
        // this covers EQ/RANGE with exclusions.
        for (ByteBuffer term : exclusions)
        {
            if (isLiteral && validateStringValue(value, term))
                return false;
            else if (validator.compare(term, value) == 0)
                return false;
        }

        return true;
    }

    private boolean validateStringValue(ByteBuffer columnValue, ByteBuffer requestedValue)
    {
        analyzer.reset(columnValue.duplicate());
        while (analyzer.hasNext())
        {
            ByteBuffer term = analyzer.next();

            boolean isMatch = false;
            switch (operation)
            {
                case EQ:
                case MATCH:
                // Operation.isSatisfiedBy handles conclusion on !=,
                // here we just need to make sure that term matched it
                case NOT_EQ:
                    isMatch = validator.compare(term, requestedValue) == 0;
                    break;

                case PREFIX:
                    isMatch = ByteBufferUtil.startsWith(term, requestedValue);
                    break;

                case SUFFIX:
                    isMatch = ByteBufferUtil.endsWith(term, requestedValue);
                    break;

                case CONTAINS:
                    isMatch = ByteBufferUtil.contains(term, requestedValue);
                    break;
            }

            if (isMatch)
                return true;
        }

        return false;
    }

    public Op getOp()
    {
        return operation;
    }

    public void checkpoint()
    {
        if (controller == null)
            return;

        controller.checkpoint();
    }

    public boolean hasLower()
    {
        return lower != null;
    }

    public boolean hasUpper()
    {
        return upper != null;
    }

    public boolean isLowerSatisfiedBy(OnDiskIndex.DataTerm term)
    {
        if (!hasLower())
            return true;

        if (nonMatchingPartial(term))
            return false;

        int cmp = term.compareTo(validator, lower.value, false);
        return cmp > 0 || cmp == 0 && lower.inclusive;
    }

    public boolean isUpperSatisfiedBy(OnDiskIndex.DataTerm term)
    {
        if (!hasUpper())
            return true;

        if (nonMatchingPartial(term))
            return false;

        int cmp = term.compareTo(validator, upper.value, false);
        return cmp < 0 || cmp == 0 && upper.inclusive;
    }

    public boolean isIndexed()
    {
        return index.isIndexed();
    }

    public String toString()
    {
        return String.format("Expression{name: %s, op: %s, lower: (%s, %s), upper: (%s, %s), exclusions: %s}",
                             index.getColumnName(),
                             operation,
                             lower == null ? "null" : validator.getString(lower.value),
                             lower != null && lower.inclusive,
                             upper == null ? "null" : validator.getString(upper.value),
                             upper != null && upper.inclusive,
                             Iterators.toString(Iterators.transform(exclusions.iterator(), validator::getString)));
    }

    public int hashCode()
    {
        return new HashCodeBuilder().append(index.getColumnName())
                                    .append(operation)
                                    .append(validator)
                                    .append(lower).append(upper)
                                    .append(exclusions).build();
    }

    public boolean equals(Object other)
    {
        if (!(other instanceof Expression))
            return false;

        if (this == other)
            return true;

        Expression o = (Expression) other;

        return Objects.equals(index.getColumnName(), o.index.getColumnName())
                && validator.equals(o.validator)
                && operation == o.operation
                && Objects.equals(lower, o.lower)
                && Objects.equals(upper, o.upper)
                && exclusions.equals(o.exclusions);
    }

    private boolean nonMatchingPartial(OnDiskIndex.DataTerm term)
    {
        return term.isPartial() && operation == Op.PREFIX;
    }

    public static class Bound
    {
        public final ByteBuffer value;
        public final boolean inclusive;

        public Bound(ByteBuffer value, boolean inclusive)
        {
            this.value = value;
            this.inclusive = inclusive;
        }

        public boolean equals(Object other)
        {
            if (!(other instanceof Bound))
                return false;

            Bound o = (Bound) other;
            return value.equals(o.value) && inclusive == o.inclusive;
        }
    }
}
