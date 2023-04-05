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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Utility class to facilitate the creation of the CQL representation of {@code SchemaElements}.
 */
public final class CqlBuilder
{
    @FunctionalInterface
    public static interface Appender<T>
    {
        public void appendTo(CqlBuilder builder, T obj);
    }

    /**
     * The new line character
     */
    private static final char NEW_LINE = '\n';

    private static final String INDENTATION = "    ";

    private final StringBuilder builder;

    private int indent;

    private boolean isNewLine = false;

    public CqlBuilder()
    {
        this(64);
    }

    public CqlBuilder(int capacity)
    {
        builder = new StringBuilder(capacity);
    }

    public CqlBuilder append(String str)
    {
        indentIfNeeded();
        builder.append(str);
        return this;
    }

    public CqlBuilder appendQuotingIfNeeded(String str)
    {
        return append(ColumnIdentifier.maybeQuote(str));
    }

    public CqlBuilder appendWithSingleQuotes(String str)
    {
        indentIfNeeded();

        builder.append('\'')
               .append(str.replaceAll("'", "''"))
               .append('\'');

        return this;
    }

    public CqlBuilder append(char c)
    {
        indentIfNeeded();
        builder.append(c);
        return this;
    }

    public CqlBuilder append(boolean b)
    {
        indentIfNeeded();
        builder.append(b);
        return this;
    }

    public CqlBuilder append(int i)
    {
        indentIfNeeded();
        builder.append(i);
        return this;
    }

    public CqlBuilder append(long l)
    {
        indentIfNeeded();
        builder.append(l);
        return this;
    }

    public CqlBuilder append(float f)
    {
        indentIfNeeded();
        builder.append(f);
        return this;
    }

    public CqlBuilder append(double d)
    {
        indentIfNeeded();
        builder.append(d);
        return this;
    }

    public CqlBuilder newLine()
    {
        builder.append(NEW_LINE);
        isNewLine = true;
        return this;
    }

    public CqlBuilder append(AbstractType<?> type)
    {
        return append(type.asCQL3Type().toString());
    }

    public CqlBuilder append(ColumnIdentifier column)
    {
        return append(column.toCQLString());
    }

    public CqlBuilder append(FunctionName name)
    {
        name.appendCqlTo(this);
        return this;
    }

    public CqlBuilder append(Map<String, String> map)
    {
        return append(map, true);
    }

    public CqlBuilder append(Map<String, String> map, boolean quoteValue)
    {
        indentIfNeeded();

        builder.append('{');

        Iterator<Entry<String, String>> iter = new TreeMap<>(map).entrySet()
                                                                 .iterator();
        while(iter.hasNext())
        {
            Entry<String, String> e = iter.next();
            appendWithSingleQuotes(e.getKey());
            builder.append(": ");
            if (quoteValue)
                appendWithSingleQuotes(e.getValue());
            else
                builder.append(e.getValue());

            if (iter.hasNext())
                builder.append(", ");
        }
        builder.append('}');
        return this;
    }

    public <T> CqlBuilder appendWithSeparators(Iterable<T> iterable, Appender<T> appender, String separator)
    {
        return appendWithSeparators(iterable.iterator(), appender, separator);
    }

    public <T> CqlBuilder appendWithSeparators(Iterator<T> iter, Appender<T> appender, String separator)
    {
        while (iter.hasNext())
        {
            appender.appendTo(this, iter.next());
            if (iter.hasNext())
            {
                append(separator);
            }
        }
        return this;
    }

    public CqlBuilder increaseIndent()
    {
        indent++;
        return this;
    }

    public CqlBuilder decreaseIndent()
    {
        if (indent > 0)
            indent--;

        return this;
    }

    private void indentIfNeeded()
    {
        if (isNewLine)
        {
            for (int i = 0; i < indent; i++)
                builder.append(INDENTATION);
            isNewLine = false;
        }
    }

    public String toString()
    {
        return builder.toString();
    }
}
