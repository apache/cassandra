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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.functions.Function;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkBindValueSet;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

/**
 * Represents either a single marker for IN values, or a list of IN values, like:
 * "SELECT ... WHERE x IN ?"
 * "SELECT ... WHERE x IN (?, ?, ?)"
 * "SELECT ... WHERE x IN (a, b, c)"
 */
public interface MarkerOrTerms
{
    void addFunctionsTo(List<Function> functions);

    List<ByteBuffer> bindAndGet(QueryOptions options, ColumnIdentifier column);

    List<List<ByteBuffer>> bindAndGetTuples(QueryOptions options, Collection<ColumnIdentifier> columns);

    /**
     * {@link MarkerOrTerms} representing a single marker for IN values, as in {@code SELECT ... WHERE x IN ?}
     */
    class Marker implements MarkerOrTerms
    {
        private final AbstractMarker marker;

        public Marker(AbstractMarker marker)
        {
            this.marker = marker;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            // nothing to do here
        }

        @Override
        public List<ByteBuffer> bindAndGet(QueryOptions options, ColumnIdentifier column)
        {
            // (NOT IN | IN) ?,  where ? bound to a list of values
            Term.Terminal term = marker.bind(options);
            checkNotNull(term, "Invalid null value for column %s", column);
            checkBindValueSet(term.get(options.getProtocolVersion()), "Invalid unset value for column %s", column);
            Term.MultiItemTerminal lval = (Term.MultiItemTerminal) term;
            return lval.getElements();
        }

        public List<List<ByteBuffer>> bindAndGetTuples(QueryOptions options, Collection<ColumnIdentifier> columns)
        {
            if (columns.isEmpty())
                throw new IllegalArgumentException("Columns must not be empty");

            // (NOT IN | IN) ?,  where ? bound to a list of tuples
            if (marker instanceof Tuples.InMarker)
            {
                Tuples.InMarker inMarker = (Tuples.InMarker) marker;
                Tuples.InValue inValue = inMarker.bind(options);
                checkNotNull(inValue, "Invalid null value for columns %s", columns);
                return inValue.getSplitValues();
            }
            // (NOT IN | IN) ?,  where ? bound to a list of scalar values, we need to convert the elements to singleton lists
            ColumnIdentifier firstColumn = columns.iterator().next();
            List<ByteBuffer> values = bindAndGet(options, firstColumn);
            return values.stream().map(Collections::singletonList).collect(Collectors.toList());
        }

        @Override
        public String toString()
        {
            return marker.toString();
        }
    }

    /**
     * {@link MarkerOrTerms} representing a list of IN terms, like:
     * {@code SELECT ... WHERE x IN (?, ?, ?)}
     * {@code SELECT ... WHERE x IN (a, b, c)}
     */
    class Terms implements MarkerOrTerms
    {
        private final List<Term> terms;

        public Terms(List<Term> terms)
        {
            this.terms = terms;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            if (terms != null)
            {
                for (Term term : terms)
                {
                    if (term != null)
                        term.addFunctionsTo(functions);
                }
            }
        }

        @Override
        public List<ByteBuffer> bindAndGet(QueryOptions options, ColumnIdentifier column)
        {
            // (NOT IN | IN) (value1, value2, ...)
            List<ByteBuffer> result = new ArrayList<>(terms.size());
            for (Term term : terms)
            {
                ByteBuffer value = term.bindAndGet(options);
                result.add(value);
            }
            return result;
        }

        @Override
        public List<List<ByteBuffer>> bindAndGetTuples(QueryOptions options, Collection<ColumnIdentifier> columns)
        {
            if (columns.isEmpty())
                throw new IllegalArgumentException("Columns must not be empty");

            // (NOT IN | IN) (value1, value2, ...)
            assert terms != null;
            return terms.stream()
                        .map(v -> getTuple(v, options))
                        .collect(Collectors.toList());
        }

        private static List<ByteBuffer> getTuple(Term value, QueryOptions options)
        {
            Term.Terminal terminal = value.bind(options);
            return (terminal instanceof Term.MultiItemTerminal)
                   ? ((Term.MultiItemTerminal) terminal).getElements()
                   : Collections.singletonList(terminal.get(options.getProtocolVersion()));
        }

        @Override
        public String toString()
        {
            return terms.toString();
        }
    }
}
