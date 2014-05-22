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
import org.apache.cassandra.thrift.IndexOperator;

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

    public static class InWithValues extends SingleColumnRestriction implements Restriction.IN
    {
        protected final List<Term> values;

        public InWithValues(List<Term> values)
        {
            this.values = values;
        }

        public List<ByteBuffer> values(List<ByteBuffer> variables) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<>(values.size());
            for (Term value : values)
                buffers.add(value.bindAndGet(variables));
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

        public boolean isOnToken()
        {
            return false;
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

        public List<ByteBuffer> values(List<ByteBuffer> variables) throws InvalidRequestException
        {
            Term.MultiItemTerminal lval = (Term.MultiItemTerminal)marker.bind(variables);
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

        public boolean isOnToken()
        {
            return false;
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

        public List<ByteBuffer> values(List<ByteBuffer> variables) throws InvalidRequestException
        {
            throw new UnsupportedOperationException();
        }

        public boolean isOnToken()
        {
            return onToken;
        }

        /** Returns true if the start or end bound (depending on the argument) is set, false otherwise */
        public boolean hasBound(Bound b)
        {
            return bounds[b.idx] != null;
        }

        public ByteBuffer bound(Bound b, List<ByteBuffer> variables) throws InvalidRequestException
        {
            return bounds[b.idx].bindAndGet(variables);
        }

        /** Returns true if the start or end bound (depending on the argument) is inclusive, false otherwise */
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

        public IndexOperator getIndexOperator(Bound b)
        {
            switch (b)
            {
                case START:
                    return boundInclusive[b.idx] ? IndexOperator.GTE : IndexOperator.GT;
                case END:
                    return boundInclusive[b.idx] ? IndexOperator.LTE : IndexOperator.LT;
            }
            throw new AssertionError();
        }

        public void setBound(Relation.Type type, Term t) throws InvalidRequestException
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
                throw new InvalidRequestException(String.format(
                        "More than one restriction was found for the %s bound", b.name().toLowerCase()));

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
