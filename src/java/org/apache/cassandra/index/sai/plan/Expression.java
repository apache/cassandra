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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.utils.TypeUtil;

public class Expression
{
    private static final Logger logger = LoggerFactory.getLogger(Expression.class);

    public enum IndexOperator
    {
        EQ, NEQ, RANGE, CONTAINS_KEY, CONTAINS_VALUE, NOT_CONTAINS_KEY, NOT_CONTAINS_VALUE;

        public static IndexOperator valueOf(Operator operator)
        {
            switch (operator)
            {
                case EQ:
                    return EQ;

                case NEQ:
                    return NEQ;

                case CONTAINS:
                    return CONTAINS_VALUE; // non-frozen map: value contains term;

                case CONTAINS_KEY:
                    return CONTAINS_KEY; // non-frozen map: value contains key term;

                case NOT_CONTAINS:
                    return NOT_CONTAINS_VALUE;

                case NOT_CONTAINS_KEY:
                    return NOT_CONTAINS_KEY;

                case LT:
                case GT:
                case LTE:
                case GTE:
                    return RANGE;

                default:
                    return null;
            }
        }

        public boolean isEquality()
        {
            return this == EQ || this == CONTAINS_KEY || this == CONTAINS_VALUE;
        }

        public boolean isEqualityOrRange()
        {
            return isEquality() || this == RANGE;
        }

        public boolean isNonEquality()
        {
            return this == NEQ || this == NOT_CONTAINS_KEY || this == NOT_CONTAINS_VALUE;
        }
    }

    public final AbstractAnalyzer.AnalyzerFactory analyzerFactory;

    public final IndexContext context;
    public final AbstractType<?> validator;

    @VisibleForTesting
    protected IndexOperator operator;

    public Bound lower, upper;
    // The upperInclusive and lowerInclusive flags are maintained separately to the inclusive flags
    // in the upper and lower bounds because the upper and lower bounds have their inclusivity relaxed
    // if the datatype being filtered is rounded in the index. These flags are used in the post-filtering
    // process to remove values equal to the bounds.
    public boolean upperInclusive, lowerInclusive;

    public Expression(IndexContext indexContext)
    {
        this.context = indexContext;
        this.analyzerFactory = indexContext.getAnalyzerFactory();
        this.validator = indexContext.getValidator();
    }

    /**
     * This adds an operation to the current {@link Expression} instance and
     * returns the current instance.
     *
     * @param op the CQL3 operation
     * @param value the expression value
     * @return the current expression with the added operation
     */
    public Expression add(Operator op, ByteBuffer value)
    {
        boolean lowerInclusive, upperInclusive;
        // If the type supports rounding then we need to make sure that index
        // range search is always inclusive, otherwise we run the risk of
        // missing values that are within the exclusive range but are rejected
        // because their rounded value is the same as the value being queried.
        lowerInclusive = upperInclusive = TypeUtil.supportsRounding(validator);
        switch (op)
        {
            case EQ:
            case NEQ:
            case CONTAINS:
            case CONTAINS_KEY:
            case NOT_CONTAINS:
            case NOT_CONTAINS_KEY:
                lower = new Bound(value, validator, true);
                upper = lower;
                operator = IndexOperator.valueOf(op);
                break;

            case LTE:
                if (context.getDefinition().isReversedType())
                {
                    this.lowerInclusive = true;
                    lowerInclusive = true;
                }
                else
                {
                    this.upperInclusive = true;
                    upperInclusive = true;
                }
            case LT:
                operator = IndexOperator.RANGE;
                if (context.getDefinition().isReversedType())
                    lower = new Bound(value, validator, lowerInclusive);
                else
                    upper = new Bound(value, validator, upperInclusive);
                break;

            case GTE:
                if (context.getDefinition().isReversedType())
                {
                    this.upperInclusive = true;
                    upperInclusive = true;
                }
                else
                {
                    this.lowerInclusive = true;
                    lowerInclusive = true;
                }
            case GT:
                operator = IndexOperator.RANGE;
                if (context.getDefinition().isReversedType())
                    upper = new Bound(value, validator, upperInclusive);
                else
                    lower = new Bound(value, validator, lowerInclusive);
                break;
        }

        assert operator != null;

        return this;
    }

    /**
     * Returns an expression that matches keys not matched by this expression.
     */
    public Expression negated()
    {
        Expression result = new Expression(context);
        result.lower = lower;
        result.upper = upper;

        switch (operator)
        {
            case NEQ:
                result.operator = IndexOperator.EQ;
                break;
            case NOT_CONTAINS_KEY:
                result.operator = IndexOperator.CONTAINS_KEY;
                break;
            case NOT_CONTAINS_VALUE:
                result.operator = IndexOperator.CONTAINS_VALUE;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Negation of operator %s not supported", operator));
        }
        return result;
    }

    /**
     * Used in post-filtering to determine is an indexed value matches the expression
     */
    public boolean isSatisfiedBy(ByteBuffer columnValue)
    {
        if (columnValue == null)
            return false;

        if (!TypeUtil.isValid(columnValue, validator))
        {
            logger.error(context.logMessage("Value is not valid for indexed column {} with {}"), context.getColumnName(), validator);
            return false;
        }

        Value value = new Value(columnValue, validator);

        if (lower != null)
        {
            // suffix check
            if (TypeUtil.isLiteral(validator))
                return validateStringValue(value.raw, lower.value.raw);
            else
            {
                // range or (not-)equals - (mainly) for numeric values
                int cmp = TypeUtil.comparePostFilter(lower.value, value, validator);

                // in case of EQ lower == upper
                if (operator == IndexOperator.EQ || operator == IndexOperator.CONTAINS_KEY || operator == IndexOperator.CONTAINS_VALUE)
                    return cmp == 0;

                if (operator == IndexOperator.NEQ || operator == IndexOperator.NOT_CONTAINS_KEY || operator == IndexOperator.NOT_CONTAINS_VALUE)
                    return cmp != 0;

                if (cmp > 0 || (cmp == 0 && !lowerInclusive))
                    return false;
            }
        }

        if (upper != null && lower != upper)
        {
            // string (prefix or suffix) check
            if (TypeUtil.isLiteral(validator))
                return validateStringValue(value.raw, upper.value.raw);
            else
            {
                // range - mainly for numeric values
                int cmp = TypeUtil.comparePostFilter(upper.value, value, validator);
                return (cmp > 0 || (cmp == 0 && upperInclusive));
            }
        }

        return true;
    }

    public boolean isSatisfiedBy(Iterator<ByteBuffer> values)
    {
        if (values == null)
            values = Collections.emptyIterator();

        boolean success = operator.isNonEquality();
        while (values.hasNext())
        {
            ByteBuffer v = values.next();
            if (isSatisfiedBy(v) ^ success)
                return !success;
        }
        return success;
    }

    private boolean validateStringValue(ByteBuffer columnValue, ByteBuffer requestedValue)
    {
        AbstractAnalyzer analyzer = analyzerFactory.create();
        analyzer.reset(columnValue.duplicate());
        try
        {
            while (analyzer.hasNext())
            {
                final ByteBuffer term = analyzer.next();

                boolean isMatch = false;
                switch (operator)
                {
                    case EQ:
                    case CONTAINS_KEY:
                    case CONTAINS_VALUE:
                        isMatch = validator.compare(term, requestedValue) == 0;
                        break;
                    case NEQ:
                    case NOT_CONTAINS_KEY:
                    case NOT_CONTAINS_VALUE:
                        isMatch = validator.compare(term, requestedValue) != 0;
                        break;
                    case RANGE:
                        isMatch = isLowerSatisfiedBy(term) && isUpperSatisfiedBy(term);
                        break;
                }

                if (isMatch)
                    return true;
            }
            return false;
        }
        finally
        {
            analyzer.end();
        }
    }

    public IndexOperator getOp()
    {
        return operator;
    }

    private boolean hasLower()
    {
        return lower != null;
    }

    private boolean hasUpper()
    {
        return upper != null;
    }

    private boolean isLowerSatisfiedBy(ByteBuffer value)
    {
        if (!hasLower())
            return true;

        int cmp = validator.compare(value, lower.value.raw);
        return cmp > 0 || cmp == 0 && lower.inclusive;
    }

    private boolean isUpperSatisfiedBy(ByteBuffer value)
    {
        if (!hasUpper())
            return true;

        int cmp = validator.compare(value, upper.value.raw);
        return cmp < 0 || cmp == 0 && upper.inclusive;
    }

    @Override
    public String toString()
    {
        return String.format("Expression{name: %s, op: %s, lower: (%s, %s), upper: (%s, %s)}",
                             context.getColumnName(),
                             operator,
                             lower == null ? "null" : validator.getString(lower.value.raw),
                             lower != null && lower.inclusive,
                             upper == null ? "null" : validator.getString(upper.value.raw),
                             upper != null && upper.inclusive);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder().append(context.getColumnName())
                                    .append(operator)
                                    .append(validator)
                                    .append(lower).append(upper).build();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Expression))
            return false;

        if (this == other)
            return true;

        Expression o = (Expression) other;

        return Objects.equals(context.getColumnName(), o.context.getColumnName())
               && validator.equals(o.validator)
               && operator == o.operator
               && Objects.equals(lower, o.lower)
               && Objects.equals(upper, o.upper);
    }

    /**
     * A representation of a column value in its raw and encoded form.
     */
    public static class Value
    {
        public final ByteBuffer raw;
        public final ByteBuffer encoded;

        public Value(ByteBuffer value, AbstractType<?> type)
        {
            this.raw = value;
            this.encoded = TypeUtil.asIndexBytes(value, type);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Value))
                return false;

            Value o = (Value) other;
            return raw.equals(o.raw) && encoded.equals(o.encoded);
        }

        @Override
        public int hashCode()
        {
            HashCodeBuilder builder = new HashCodeBuilder();
            builder.append(raw);
            builder.append(encoded);
            return builder.toHashCode();
        }
    }

    public static class Bound
    {
        public final Value value;
        public final boolean inclusive;

        public Bound(ByteBuffer value, AbstractType<?> type, boolean inclusive)
        {
            this.value = new Value(value, type);
            this.inclusive = inclusive;
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Bound))
                return false;

            Bound o = (Bound) other;
            return value.equals(o.value) && inclusive == o.inclusive;
        }

        @Override
        public int hashCode()
        {
            HashCodeBuilder builder = new HashCodeBuilder();
            builder.append(value);
            builder.append(inclusive);
            return builder.toHashCode();
        }
    }
}
