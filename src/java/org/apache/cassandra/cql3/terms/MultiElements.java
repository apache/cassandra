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

package org.apache.cassandra.cql3.terms;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.MultiElementType;
import org.apache.cassandra.serializers.MarshalException;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Static classes for Terms representing individual values of multi-elements data types.
 */
public final class MultiElements
{
    private MultiElements()
    {
    }

    /**
     * The serialized elements of a multi-element type (collection, tuple, udt, ...)
     */
    public static class Value extends Term.Terminal
    {
        /**
         * The type represented by this {@code Value}
         */
        private final MultiElementType<?> type;

        /**
         * The serialized values of the elements composing this value.
         */
        private final List<ByteBuffer> elements;

        /**
         * Creates a {@code Value} from its serialized representation.
         *
         * @param value a serialized value from the specified type
         * @param type the value type
         * @return a {@code Value}
         */
        public static Value fromSerialized(ByteBuffer value, MultiElementType<?> type)
        {
            try
            {
                // We depend for SetType and MapType on the collections being without duplicated keys and sorted.
                return new Value(type, type.filterSortAndValidateElements(type.unpack(value)));
            }
            catch (MarshalException e)
            {
                throw invalidRequest(e.getMessage());
            }
        }

        public Value(MultiElementType<?> type, List<ByteBuffer> elements)
        {
            this.type = type;
            this.elements = elements;
        }

        @Override
        public ByteBuffer get()
        {
            return elements == null ? null : type.pack(elements);
        }

        @Override
        public List<ByteBuffer> getElements()
        {
            return elements;
        }
    }

    /**
     * The terms representing a multi-element value (collection, tuple, udt, ...) where at least one of the terms
     * represents a non-pure function or a bind marker.
     */
    public static class DelayedValue extends Term.NonTerminal
    {
        /**
         * The type of this value.
         */
        private final MultiElementType<?> type;

        /**
         * The terms representing the elements composing this value.
         */
        private final List<Term> elements;

        public DelayedValue(MultiElementType<?> type, List<Term> elements)
        {
            this.type = type;
            this.elements = elements;
        }

        @Override
        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            if (type.supportsElementBindMarkers())
            {
                for (int i = 0, m = elements.size(); i < m; i++)
                    elements.get(i).collectMarkerSpecification(boundNames);
            }
        }

        @Override
        public Terminal bind(QueryOptions options)
        {
            try
            {
                List<ByteBuffer> buffers = new ArrayList<>(elements.size());
                for (Term t : elements)
                {
                    buffers.add(t.bindAndGet(options));
                }

                buffers = type.filterSortAndValidateElements(buffers);
                return new Value(type, buffers);
            }
            catch (MarshalException e)
            {
                throw invalidRequest(e.getMessage());
            }
        }

        @Override
        public boolean containsBindMarker()
        {
            if (type.supportsElementBindMarkers())
                return false;

            for (Term element : elements)
                if (element.containsBindMarker())
                    return true;
            return false;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            Terms.addFunctions(elements, functions);
        }
    }
}
