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

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.exceptions.InvalidRequestException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class SingleColumnRestriction implements Restriction
{
    public boolean isMultiColumn()
    {
        return false;
    }

    public static class EQ extends SingleColumnRestriction implements Restriction.EQ
    {
        protected final Term value;
        private final boolean onToken;

        public EQ(Term value, boolean onToken)
        {
            this.value = value;
            this.onToken = onToken;
        }

        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            return Collections.singletonList(value.bindAndGet(options));
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

        public boolean isContains()
        {
            return false;
        }

        public boolean isOnToken()
        {
            return onToken;
        }

        public boolean canEvaluateWithSlices()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return String.format("EQ(%s)%s", value, onToken ? "*" : "");
        }
    }

    public static class InWithValues extends SingleColumnRestriction implements Restriction.IN
    {
        protected final List<? extends Term> values;

        public InWithValues(List<? extends Term> values)
        {
            this.values = values;
        }

        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<>(values.size());
            for (Term value : values)
                buffers.add(value.bindAndGet(options));
            return buffers;
        }

        public boolean canHaveOnlyOneValue()
        {
            return values.size() == 1;
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

        public boolean isContains()
        {
            return false;
        }

        public boolean isOnToken()
        {
            return false;
        }

        public boolean canEvaluateWithSlices()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return String.format("IN(%s)", values);
        }
    }

    public static class InWithMarker extends SingleColumnRestriction implements Restriction.IN
    {
        protected final AbstractMarker marker;

        public InWithMarker(AbstractMarker marker)
        {
            this.marker = marker;
        }

        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            Term.MultiItemTerminal lval = (Term.MultiItemTerminal)marker.bind(options);
            if (lval == null)
                throw new InvalidRequestException("Invalid null value for IN restriction");
            return lval.getElements();
        }

        public boolean canHaveOnlyOneValue()
        {
            return false;
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

        public boolean isContains()
        {
            return false;
        }

        public boolean isOnToken()
        {
            return false;
        }

        public boolean canEvaluateWithSlices()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "IN ?";
        }
    }

    public static class Slice extends SingleColumnRestriction implements Restriction.Slice
    {
        protected final Term[] bounds;
        protected final boolean[] boundInclusive;
        protected final boolean onToken;

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

        public boolean isContains()
        {
            return false;
        }

        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            throw new UnsupportedOperationException();
        }

        public boolean isOnToken()
        {
            return onToken;
        }

        public boolean canEvaluateWithSlices()
        {
            return true;
        }

        /** Returns true if the start or end bound (depending on the argument) is set, false otherwise */
        public boolean hasBound(Bound b)
        {
            return bounds[b.idx] != null;
        }

        public ByteBuffer bound(Bound b, QueryOptions options) throws InvalidRequestException
        {
            return bounds[b.idx].bindAndGet(options);
        }

        /** Returns true if the start or end bound (depending on the argument) is inclusive, false otherwise */
        public boolean isInclusive(Bound b)
        {
            return bounds[b.idx] == null || boundInclusive[b.idx];
        }

        public Operator getRelation(Bound eocBound, Bound inclusiveBound)
        {
            switch (eocBound)
            {
                case START:
                    return boundInclusive[inclusiveBound.idx] ? Operator.GTE : Operator.GT;
                case END:
                    return boundInclusive[inclusiveBound.idx] ? Operator.LTE : Operator.LT;
            }
            throw new AssertionError();
        }

        public Operator getIndexOperator(Bound b)
        {
            switch (b)
            {
                case START:
                    return boundInclusive[b.idx] ? Operator.GTE : Operator.GT;
                case END:
                    return boundInclusive[b.idx] ? Operator.LTE : Operator.LT;
            }
            throw new AssertionError();
        }

        @Override
        public final void setBound(Operator operator, Term t) throws InvalidRequestException
        {
            Bound b;
            boolean inclusive;
            switch (operator)
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

            assert bounds[b.idx] == null;

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

    // This holds both CONTAINS and CONTAINS_KEY restriction because we might want to have both of them.
    public static class Contains extends SingleColumnRestriction
    {
        private List<Term> values; // for CONTAINS
        private List<Term> keys;   // for CONTAINS_KEY

        public boolean hasContains()
        {
            return values != null;
        }

        public boolean hasContainsKey()
        {
            return keys != null;
        }

        public int numberOfValues()
        {
            return values == null ? 0 : values.size();
        }

        public int numberOfKeys()
        {
            return keys == null ? 0 : keys.size();
        }

        public void add(Term t, boolean isKey)
        {
            if (isKey)
                addKey(t);
            else
                addValue(t);
        }

        public void addValue(Term t)
        {
            if (values == null)
                values = new ArrayList<>();
            values.add(t);
        }

        public void addKey(Term t)
        {
            if (keys == null)
                keys = new ArrayList<>();
            keys.add(t);
        }

        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            if (values == null)
                return Collections.emptyList();

            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(values.size());
            for (Term value : values)
                buffers.add(value.bindAndGet(options));
            return buffers;
        }

        public List<ByteBuffer> keys(QueryOptions options) throws InvalidRequestException
        {
            if (keys == null)
                return Collections.emptyList();

            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(keys.size());
            for (Term value : keys)
                buffers.add(value.bindAndGet(options));
            return buffers;
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
            return false;
        }

        public boolean isContains()
        {
            return true;
        }

        public boolean isOnToken()
        {
            return false;
        }

        public boolean canEvaluateWithSlices()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return String.format("CONTAINS(values=%s, keys=%s)", values, keys);
        }
    }
}
