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

package org.apache.cassandra.cql3.ast;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ReservedKeywords;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.schema.ColumnMetadata;

public class Symbol implements ReferenceExpression, Comparable<Symbol>
{
    public final String symbol;
    private final AbstractType<?> type;

    public Symbol(ColumnMetadata column)
    {
        this(column.name.toString(), column.type);
    }

    public Symbol(String symbol, AbstractType<?> type)
    {
        this.symbol = Objects.requireNonNull(symbol);
        this.type = Objects.requireNonNull(type);
    }

    public static Symbol from(ColumnMetadata metadata)
    {
        return new Symbol(metadata.name.toString(), metadata.type.unwrap());
    }

    public static Symbol unknownType(String name)
    {
        return new Symbol(name, BytesType.instance);
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        maybeQuote(sb, symbol);
    }

    public static void maybeQuote(StringBuilder sb, String symbol)
    {
        sb.append(maybeQuote(symbol));
    }

    public static String maybeQuote(String symbol)
    {
        if (ReservedKeywords.isReserved(symbol))
        {
            return quote(symbol);
        }
        else
        {
            return ColumnIdentifier.maybeQuote(symbol);
        }
    }

    private static final Pattern PATTERN_DOUBLE_QUOTE = Pattern.compile("\"", Pattern.LITERAL);
    private static final String ESCAPED_DOUBLE_QUOTE = Matcher.quoteReplacement("\"\"");

    //TODO refactor ColumnIdentifier to expose this
    private static String quote(String text)
    {
        return '"' + PATTERN_DOUBLE_QUOTE.matcher(text).replaceAll(ESCAPED_DOUBLE_QUOTE) + '"';
    }

    @Override
    public AbstractType<?> type()
    {
        return type;
    }

    @Override
    public String name()
    {
        return symbol;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Symbol symbol1 = (Symbol) o;
        return Objects.equals(symbol, symbol1.symbol);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(symbol);
    }

    @Override
    public String toString()
    {
        return toCQL();
    }

    @Override
    public int compareTo(Symbol o)
    {
        return toCQL().compareTo(o.toCQL());
    }

    public static class UnquotedSymbol extends Symbol
    {
        public UnquotedSymbol(String symbol, AbstractType<?> type)
        {
            super(symbol, type);
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            sb.append(symbol);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UnquotedSymbol symbol1 = (UnquotedSymbol) o;
            return Objects.equals(symbol, symbol1.symbol);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(symbol);
        }
    }
}
