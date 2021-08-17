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
package org.apache.cassandra.cql3.functions.types;

import org.apache.cassandra.cql3.ColumnIdentifier;

/**
 * Keeps metadata on the connected cluster, including known nodes and schema definitions.
 */
public class Metadata
{
    /*
     * Deal with case sensitivity for a given element id (keyspace, table, column, etc.)
     *
     * This method is used to convert identifiers provided by the client (through methods such as getKeyspace(String)),
     * to the format used internally by the driver.
     *
     * We expect client-facing APIs to behave like cqlsh, that is:
     * - identifiers that are mixed-case or contain special characters should be quoted.
     * - unquoted identifiers will be lowercased: getKeyspace("Foo") will look for a keyspace named "foo"
     */
    static String handleId(String id)
    {
        // Shouldn't really happen for this method, but no reason to fail here
        if (id == null) return null;

        boolean isAlphanumericLowCase = true;
        boolean isAlphanumeric = true;
        for (int i = 0; i < id.length(); i++)
        {
            char c = id.charAt(i);
            if (c >= 65 && c <= 90)
            { // A-Z
                isAlphanumericLowCase = false;
            }
            else if (!((c >= 48 && c <= 57) // 0-9
                       || (c == 95) // _ (underscore)
                       || (c >= 97 && c <= 122) // a-z
            ))
            {
                isAlphanumeric = false;
                isAlphanumericLowCase = false;
                break;
            }
        }

        if (isAlphanumericLowCase)
        {
            return id;
        }
        if (isAlphanumeric)
        {
            return id.toLowerCase();
        }

        // Check if it's enclosed in quotes. If it is, remove them and unescape internal double quotes
        return ParseUtils.unDoubleQuote(id);
    }

    /**
     * Quotes a CQL identifier if necessary.
     *
     * <p>This is similar to {@link #quote(String)}, except that it won't quote the input string if it
     * can safely be used as-is. For example:
     *
     * <ul>
     * <li>{@code quoteIfNecessary("foo").equals("foo")} (no need to quote).
     * <li>{@code quoteIfNecessary("Foo").equals("\"Foo\"")} (identifier is mixed case so case
     * sensitivity is required)
     * <li>{@code quoteIfNecessary("foo bar").equals("\"foo bar\"")} (identifier contains special
     * characters)
     * <li>{@code quoteIfNecessary("table").equals("\"table\"")} (identifier is a reserved CQL
     * keyword)
     * </ul>
     *
     * @param id the "internal" form of the identifier. That is, the identifier as it would appear in
     *           Cassandra system tables (such as {@code system_schema.tables}, {@code
     *           system_schema.columns}, etc.)
     * @return the identifier as it would appear in a CQL query string. This is also how you need to
     * pass it to public driver methods, such as {@code #getKeyspace(String)}.
     */
    static String quoteIfNecessary(String id)
    {
        return ColumnIdentifier.maybeQuote(id);
    }

    /**
     * Quote a keyspace, table or column identifier to make it case sensitive.
     *
     * <p>CQL identifiers, including keyspace, table and column ones, are case insensitive by default.
     * Case sensitive identifiers can however be provided by enclosing the identifier in double quotes
     * (see the <a href="http://cassandra.apache.org/doc/cql3/CQL.html#identifiers">CQL
     * documentation</a> for details). If you are using case sensitive identifiers, this method can be
     * used to enclose such identifiers in double quotes, making them case sensitive.
     *
     * <p>Note that <a
     * href="https://docs.datastax.com/en/cql/3.0/cql/cql_reference/keywords_r.html">reserved CQL
     * keywords</a> should also be quoted. You can check if a given identifier is a reserved keyword
     * by calling {@code #isReservedCqlKeyword(String)}.
     *
     * @param id the keyspace or table identifier.
     * @return {@code id} enclosed in double-quotes, for use in methods like {@code #getReplicas},
     * {@code #getKeyspace}, {@code KeyspaceMetadata#getTable} or even {@code
     * Cluster#connect(String)}.
     */
    public static String quote(String id)
    {
        return ParseUtils.doubleQuote(id);
    }
}
