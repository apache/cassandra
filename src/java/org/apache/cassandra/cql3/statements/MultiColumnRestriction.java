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

import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.exceptions.InvalidRequestException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public interface MultiColumnRestriction extends Restriction
{
    public static class EQ extends SingleColumnRestriction.EQ implements MultiColumnRestriction
    {
        public EQ(Term value, boolean onToken)
        {
            super(value, onToken);
        }

        public boolean isMultiColumn()
        {
            return true;
        }

        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            Tuples.Value t = (Tuples.Value)value.bind(options);
            return t.getElements();
        }
    }

    public interface IN extends MultiColumnRestriction
    {
        public List<List<ByteBuffer>> splitValues(QueryOptions options) throws InvalidRequestException;
    }

    /**
     * An IN restriction that has a set of terms for in values.
     * For example: "SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))" or "WHERE (a, b, c) IN (?, ?)"
     */
    public static class InWithValues extends SingleColumnRestriction.InWithValues implements MultiColumnRestriction.IN
    {
        public InWithValues(List<? extends Term> values)
        {
            super(values);
        }

        public boolean isMultiColumn()
        {
            return true;
        }

        public List<List<ByteBuffer>> splitValues(QueryOptions options) throws InvalidRequestException
        {
            List<List<ByteBuffer>> buffers = new ArrayList<>(values.size());
            for (Term value : values)
            {
                Term.MultiItemTerminal term = (Term.MultiItemTerminal)value.bind(options);
                buffers.add(term.getElements());
            }
            return buffers;
        }
    }

    /**
     * An IN restriction that uses a single marker for a set of IN values that are tuples.
     * For example: "SELECT ... WHERE (a, b, c) IN ?"
     */
    public static class InWithMarker extends SingleColumnRestriction.InWithMarker implements MultiColumnRestriction.IN
    {
        public InWithMarker(AbstractMarker marker)
        {
            super(marker);
        }

        public boolean isMultiColumn()
        {
            return true;
        }

        public List<List<ByteBuffer>> splitValues(QueryOptions options) throws InvalidRequestException
        {
            Tuples.InMarker inMarker = (Tuples.InMarker)marker;
            Tuples.InValue inValue = inMarker.bind(options);
            if (inValue == null)
                throw new InvalidRequestException("Invalid null value for IN restriction");
            return inValue.getSplitValues();
        }
    }

    public static class Slice extends SingleColumnRestriction.Slice implements MultiColumnRestriction
    {
        public Slice(boolean onToken)
        {
            super(onToken);
        }

        public boolean isMultiColumn()
        {
            return true;
        }

        public ByteBuffer bound(Bound b, QueryOptions options) throws InvalidRequestException
        {
            throw new UnsupportedOperationException("Multicolumn slice restrictions do not support bound()");
        }

        /**
         * Similar to bounds(), but returns one ByteBuffer per-component in the bound instead of a single
         * ByteBuffer to represent the entire bound.
         */
        public List<ByteBuffer> componentBounds(Bound b, QueryOptions options) throws InvalidRequestException
        {
            Tuples.Value value = (Tuples.Value)bounds[b.idx].bind(options);
            return value.getElements();
        }
    }
}
