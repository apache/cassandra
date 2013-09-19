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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.cql3.*;

/**
 * A restriction/clause on a column.
 * The goal of this class being to group all conditions for a column in a SELECT.
 */
public interface Restriction
{
    public boolean isOnToken();

    public boolean isSlice();
    public boolean isEQ();
    public boolean isIN();

    // Only supported for EQ and IN, but it's convenient to have here
    public List<ByteBuffer> values(List<ByteBuffer> variables) throws InvalidRequestException;

    public static class EQ implements Restriction
    {
        private final Term value;
        private final boolean onToken;

        public EQ(Term value, boolean onToken)
        {
            this.value = value;
            this.onToken = onToken;
        }

        public List<ByteBuffer> values(List<ByteBuffer> variables) throws InvalidRequestException
        {
            return Collections.singletonList(value.bindAndGet(variables));
        }

        public boolean isSlice()
        {
            return false;
        }

        public boolean isEQ()
        {
            return true;
        }

        public boolean isIN()
        {
            return false;
        }

        public boolean isOnToken()
        {
            return onToken;
        }

        @Override
        public String toString()
        {
            return String.format("EQ(%s)%s", value, onToken ? "*" : "");
        }
    }

    public static abstract class IN implements Restriction
    {
        public static IN create(List<Term> values)
        {
            return new WithValues(values);
        }

        public static IN create(Term value) throws InvalidRequestException
        {
            assert value instanceof Lists.Marker; // we shouldn't have got there otherwise
            return new WithMarker((Lists.Marker)value);
        }

        public boolean isSlice()
        {
            return false;
        }

        public boolean isEQ()
        {
            return false;
        }

        public boolean isIN()
        {
            return true;
        }

        // Used when we need to know if it's a IN with just one value before we have
        // the bind variables. This is ugly and only there for backward compatiblity
        // because we used to treate IN with 1 value like an EQ and need to preserve
        // this behavior.
        public abstract boolean canHaveOnlyOneValue();

        public boolean isOnToken()
        {
            return false;
        }

        private static class WithValues extends IN
        {
            private final List<Term> values;

            private WithValues(List<Term> values)
            {
                this.values = values;
            }

            public List<ByteBuffer> values(List<ByteBuffer> variables) throws InvalidRequestException
            {
                List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(values.size());
                for (Term value : values)
                    buffers.add(value.bindAndGet(variables));
                return buffers;
            }

            public boolean canHaveOnlyOneValue()
            {
                return values.size() == 1;
            }

            @Override
            public String toString()
            {
                return String.format("IN(%s)", values);
            }
        }

        private static class WithMarker extends IN
        {
            private final Lists.Marker marker;

            private WithMarker(Lists.Marker marker)
            {
                this.marker = marker;
            }

            public List<ByteBuffer> values(List<ByteBuffer> variables) throws InvalidRequestException
            {
                Lists.Value lval = marker.bind(variables);
                if (lval == null)
                    throw new InvalidRequestException("Invalid null value for IN restriction");
                return lval.elements;
            }

            public boolean canHaveOnlyOneValue()
            {
                return false;
            }

            @Override
            public String toString()
            {
                return "IN ?";
            }
        }
    }

    public static class Slice implements Restriction
    {
        private final Term[] bounds;
        private final boolean[] boundInclusive;
        private final boolean onToken;

        public Slice(boolean onToken)
        {
            this.bounds = new Term[2];
            this.boundInclusive = new boolean[2];
            this.onToken = onToken;
        }

        public boolean isSlice()
        {
            return true;
        }

        public boolean isEQ()
        {
            return false;
        }

        public boolean isIN()
        {
            return false;
        }

        public List<ByteBuffer> values(List<ByteBuffer> variables) throws InvalidRequestException
        {
            throw new UnsupportedOperationException();
        }

        public boolean isOnToken()
        {
            return onToken;
        }

        public boolean hasBound(Bound b)
        {
            return bounds[b.idx] != null;
        }

        public ByteBuffer bound(Bound b, List<ByteBuffer> variables) throws InvalidRequestException
        {
            return bounds[b.idx].bindAndGet(variables);
        }

        public boolean isInclusive(Bound b)
        {
            return bounds[b.idx] == null || boundInclusive[b.idx];
        }

        public Relation.Type getRelation(Bound eocBound, Bound inclusiveBound)
        {
            switch (eocBound)
            {
                case START:
                    return boundInclusive[inclusiveBound.idx] ? Relation.Type.GTE : Relation.Type.GT;
                case END:
                    return boundInclusive[inclusiveBound.idx] ? Relation.Type.LTE : Relation.Type.LT;
            }
            throw new AssertionError();
        }

        public IndexExpression.Operator getIndexOperator(Bound b)
        {
            switch (b)
            {
                case START:
                    return boundInclusive[b.idx] ? IndexExpression.Operator.GTE : IndexExpression.Operator.GT;
                case END:
                    return boundInclusive[b.idx] ? IndexExpression.Operator.LTE : IndexExpression.Operator.LT;
            }
            throw new AssertionError();
        }

        public void setBound(ColumnIdentifier name, Relation.Type type, Term t) throws InvalidRequestException
        {
            Bound b;
            boolean inclusive;
            switch (type)
            {
                case GT:
                    b = Bound.START;
                    inclusive = false;
                    break;
                case GTE:
                    b = Bound.START;
                    inclusive = true;
                    break;
                case LT:
                    b = Bound.END;
                    inclusive = false;
                    break;
                case LTE:
                    b = Bound.END;
                    inclusive = true;
                    break;
                default:
                    throw new AssertionError();
            }

            if (bounds[b.idx] != null)
                throw new InvalidRequestException(String.format("Invalid restrictions found on %s", name));

            bounds[b.idx] = t;
            boundInclusive[b.idx] = inclusive;
        }

        @Override
        public String toString()
        {
            return String.format("SLICE(%s %s, %s %s)%s", boundInclusive[0] ? ">=" : ">",
                                                          bounds[0],
                                                          boundInclusive[1] ? "<=" : "<",
                                                          bounds[1],
                                                          onToken ? "*" : "");
        }
    }
}
