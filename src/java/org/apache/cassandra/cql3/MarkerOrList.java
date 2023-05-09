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
public class MarkerOrList
{
    final AbstractMarker marker;
    final List<Term> values;

    private MarkerOrList(AbstractMarker marker, List<Term> values)
    {
        assert ((marker != null) ^ (values != null));
        this.marker = marker;
        this.values = values;
    }

    public static MarkerOrList marker(AbstractMarker marker)
    {
        return new MarkerOrList(marker, null);
    }

    public static MarkerOrList list(List<Term> values)
    {
        return new MarkerOrList(null, values);
    }

    public void addFunctionsTo(List<Function> functions)
    {
        if (values != null)
        {
            for (Term term : values)
            {
                if (term != null)
                    term.addFunctionsTo(functions);
            }
        }
    }

    public List<ByteBuffer> bindAndGet(QueryOptions options, ColumnIdentifier column)
    {
        // (NOT IN | IN) ?,  where ? bound to a list of values
        if (marker != null)
        {
            Term.Terminal term = marker.bind(options);
            checkNotNull(term, "Invalid null value for column %s", column);
            checkBindValueSet(term.get(options.getProtocolVersion()), "Invalid unset value for column %s", column);
            Term.MultiItemTerminal lval = (Term.MultiItemTerminal) term;
            return lval.getElements();
        }
        else
        // (NOT IN | IN) (value1, value2, ...)
        {
            assert values != null;
            List<ByteBuffer> result = new ArrayList<>(values.size());
            for (Term term : values)
            {
                ByteBuffer value = term.bindAndGet(options);
                result.add(value);
            }
            return result;
        }
    }

    public List<List<ByteBuffer>> bindAndGetTuples(QueryOptions options, Collection<ColumnIdentifier> columns)
    {
        if (columns.isEmpty())
            throw new IllegalArgumentException("Columns must not be empty");

        // (NOT IN | IN) ?,  where ? bound to a list of tuples
        if (marker != null && marker instanceof Tuples.InMarker)
        {
            Tuples.InMarker inMarker = (Tuples.InMarker) marker;
            Tuples.InValue inValue = inMarker.bind(options);
            checkNotNull(inValue, "Invalid null value for columns %s", columns);
            return inValue.getSplitValues();
        }
        // (NOT IN | IN) ?,  where ? bound to a list of scalar values, we need to convert the elements to singleton lists
        else if (marker != null)
        {
            ColumnIdentifier firstColumn = columns.iterator().next();
            List<ByteBuffer> values = bindAndGet(options, firstColumn);
            return values.stream().map(Collections::singletonList).collect(Collectors.toList());
        }
        // (NOT IN | IN) (value1, value2, ...)
        else
        {
            assert values != null;
            return values.stream()
                         .map(v -> getTuple(v, options))
                         .collect(Collectors.toList());
        }
    }

    private List<ByteBuffer> getTuple(Term value, QueryOptions options)
    {
        Term.Terminal terminal = value.bind(options);
        return (terminal instanceof Term.MultiItemTerminal)
            ? ((Term.MultiItemTerminal) terminal).getElements()
            : Collections.singletonList(terminal.get(options.getProtocolVersion()));
    }

    public String toString()
    {
        return (marker != null) ? marker.toString(): values.toString();
    }
}
