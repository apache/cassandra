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

import java.util.Comparator;
import java.util.Locale;

/**
 * A schema element (keyspace, udt, udf, uda, table, index, view).
 */
public interface SchemaElement
{
    /**
     * Comparator used to sort {@code Describable} name.
     */
    Comparator<SchemaElement> NAME_COMPARATOR = (o1, o2) -> o1.elementName().compareToIgnoreCase(o2.elementName());

    enum SchemaElementType
    {
        KEYSPACE,
        TYPE,
        FUNCTION,
        AGGREGATE,
        TABLE,
        INDEX,
        MATERIALIZED_VIEW;

        @Override
        public String toString()
        {
            return super.toString().toLowerCase(Locale.US);
        }
    }

    /**
     * Return the schema element type
     *
     * @return the schema element type
     */
    SchemaElementType elementType();

    /**
     * Returns the CQL name of the keyspace to which this schema element belong.
     *
     * @return the keyspace name.
     */
    String elementKeyspace();

    /**
     * Returns the CQL name of this schema element.
     *
     * @return the name of this schema element.
     */
    String elementName();

    default String elementNameQuotedIfNeeded()
    {
        String name = elementName();
        if (elementType() == SchemaElementType.FUNCTION
                || elementType() == SchemaElementType.AGGREGATE)
        {
            int index = name.indexOf('(');
            return ColumnIdentifier.maybeQuote(name.substring(0, index)) + name.substring(index);
        }

        return ColumnIdentifier.maybeQuote(name);
    }

    default String elementKeyspaceQuotedIfNeeded()
    {
        return ColumnIdentifier.maybeQuote(elementKeyspace());
    }

    /**
     * Returns a CQL representation of this element
     *
     * @param withInternals if the internals part of the CQL should be exposed.
     * @param ifNotExists if "IF NOT EXISTS" should be included.
     * @return a CQL representation of this element
     */
    String toCqlString(boolean withInternals, boolean ifNotExists);
}
