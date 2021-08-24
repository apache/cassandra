/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Expression
{
    private static final Logger logger = LoggerFactory.getLogger(Expression.class);

    public enum Op
    {
        EQ, MATCH, PREFIX, NOT_EQ, RANGE, CONTAINS_KEY, CONTAINS_VALUE, IN;

        public static Op valueOf(Operator operator)
        {
            switch (operator)
            {
                case EQ:
                    return EQ;

                case NEQ:
                    return NOT_EQ;

                case CONTAINS:
                    return CONTAINS_VALUE; // non-frozen map: value contains term;

                case CONTAINS_KEY:
                    return CONTAINS_KEY; // non-frozen map: value contains key term;

                case LT:
                case GT:
                case LTE:
                case GTE:
                    return RANGE;

                case LIKE_PREFIX:
                    return PREFIX;

                case LIKE_MATCHES:
                    return MATCH;

                case IN:
                    return IN;

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
    }

    public final AbstractAnalyzer.AnalyzerFactory analyzerFactory;

    public final ColumnContext context;
    public final AbstractType<?> validator;

    @VisibleForTesting
    protected Op operation;

    public Bound lower, upper;
    public boolean upperInclusive, lowerInclusive;

    final List<ByteBuffer> exclusions = new ArrayList<>();

    public Expression(ColumnContext columnContext)
    {
        this.context = columnContext;
        this.analyzerFactory = columnContext.getQueryAnalyzerFactory();
        this.validator = columnContext.getValidator();
    }

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
            case LIKE_PREFIX:
            case LIKE_MATCHES:
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
                lower = new Bound(value, validator, true);
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
                    lower = new Bound(value, validator, true);
                    upper = lower;
                }
                else
                    exclusions.add(value);
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
                operation = Op.RANGE;
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
                operation = Op.RANGE;
                if (context.getDefinition().isReversedType())
                    upper = new Bound(value, validator,  upperInclusive);
                else
                    lower = new Bound(value, validator, lowerInclusive);
                break;
        }

        assert operation != null;

        return this;
    }

    public boolean isSatisfiedBy(ByteBuffer columnValue)
    {
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
            {
                if (!validateStringValue(value.raw, lower.value.raw))
                    return false;
            }
            else
            {
                // range or (not-)equals - (mainly) for numeric values
                int cmp = TypeUtil.comparePostFilter(lower.value, value, validator);

                // in case of (NOT_)EQ lower == upper
                if (operation == Op.EQ || operation == Op.CONTAINS_KEY || operation == Op.CONTAINS_VALUE || operation == Op.NOT_EQ)
                    return cmp == 0;

                if (cmp > 0 || (cmp == 0 && !lowerInclusive))
                    return false;
            }
        }

        if (upper != null && lower != upper)
        {
            // string (prefix or suffix) check
            if (TypeUtil.isLiteral(validator))
            {
                if (!validateStringValue(value.raw, upper.value.raw))
                    return false;
            }
            else
            {
                // range - mainly for numeric values
                int cmp = TypeUtil.comparePostFilter(upper.value, value, validator);
                if (cmp < 0 || (cmp == 0 && !upperInclusive))
                    return false;
            }
        }

        // as a last step let's check exclusions for the given field,
        // this covers EQ/RANGE with exclusions.
        for (ByteBuffer term : exclusions)
        {
            if (TypeUtil.isLiteral(validator) && validateStringValue(value.raw, term) ||
                TypeUtil.comparePostFilter(new Value(term, validator), value, validator) == 0)
                return false;
        }

        return true;
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
                switch (operation)
                {
                    case EQ:
                    case MATCH:
                        // Operation.isSatisfiedBy handles conclusion on !=,
                        // here we just need to make sure that term matched it
                    case CONTAINS_KEY:
                    case CONTAINS_VALUE:
                    case NOT_EQ:
                        isMatch = validator.compare(term, requestedValue) == 0;
                        break;
                    case RANGE:
                        isMatch = isLowerSatisfiedBy(term) && isUpperSatisfiedBy(term);
                        break;

                    case PREFIX:
                        isMatch = ByteBufferUtil.startsWith(term, requestedValue);
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

    public Op getOp()
    {
        return operation;
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

    public String toString()
    {
        return String.format("Expression{name: %s, op: %s, lower: (%s, %s), upper: (%s, %s), exclusions: %s}",
                             context.getColumnName(),
                             operation,
                             lower == null ? "null" : validator.getString(lower.value.raw),
                             lower != null && lower.inclusive,
                             upper == null ? "null" : validator.getString(upper.value.raw),
                             upper != null && upper.inclusive,
                             Iterators.toString(Iterators.transform(exclusions.iterator(), validator::getString)));
    }

    public int hashCode()
    {
        return new HashCodeBuilder().append(context.getColumnName())
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

        return Objects.equals(context.getColumnName(), o.context.getColumnName())
                && validator.equals(o.validator)
                && operation == o.operation
                && Objects.equals(lower, o.lower)
                && Objects.equals(upper, o.upper)
                && exclusions.equals(o.exclusions);
    }

    /**
     * A representation of a column value in it's raw and encoded form.
     */
    public static class Value
    {
        public final ByteBuffer raw;
        public final ByteBuffer encoded;

        public Value(ByteBuffer value, AbstractType<?> type)
        {
            this.raw = value;
            this.encoded = TypeUtil.encode(value, type);
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
