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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Utility class to facilitate the creation of the CQL representation of {@code SchemaElements}.
 */
public class CqlBuilder
{
    @FunctionalInterface
    public static interface Appender<T>
    {
        public void appendTo(CqlBuilder builder, T obj);
    }

    /**
     * Max length to display values for debug strings
     **/
    public final static int MAX_VALUE_LEN = Integer.getInteger(Config.PROPERTY_PREFIX + "max_cql_debug_length", 128);

    /**
     * chars that wrap a CQL literal
     */
    private static List<Character> LITERAL_END = Lists.newArrayList('\'', ']', '}', ')');

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

    public CqlBuilder append(Object obj)
    {
        indentIfNeeded();
        builder.append(obj.toString());
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

    public CqlBuilder append(AbstractType<?> type, ByteBuffer buffer)
    {
        return append(type, buffer, false);
    }

    public CqlBuilder append(AbstractType<?> type, ByteBuffer buffer, boolean truncate)
    {
        if (type instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)type;
            ByteBuffer[] values = ct.split(buffer);
            for (int i = 0; i < ct.types.size(); i++)
            {
                String val = ct.types.get(i).asCQL3Type().toCQLLiteral(values[i], ProtocolVersion.CURRENT);
                append(i == 0 ? "" : ", ").append(truncate? truncateCqlLiteral(val) : val);
            }
        }
        else
        {
            String val = type.asCQL3Type().toCQLLiteral(buffer, ProtocolVersion.CURRENT);
            append(truncate? truncateCqlLiteral(val) : val);
        }
        return this;
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

    public static String truncateCqlLiteral(String value)
    {
        return truncateCqlLiteral(value, MAX_VALUE_LEN);
    }

    @VisibleForTesting
    public static String truncateCqlLiteral(final String value, int max)
    {
        if (value.length() <= max)
            return value;

        String result = value;
        // Do not interrupt a escaped '
        int truncateAt = max;
        for (;value.charAt(truncateAt) == '\'' && truncateAt < value.length()-1; truncateAt++);

        result = value.substring(0, truncateAt) + "...";

        // both string literals or blobs are expected to exceed this size, while blobs are fine as is the string
        // literals will be truncating their trailing '. Collections have (), {}, and []'s
        char last = value.charAt(value.length() - 1);
        if (LITERAL_END.contains(last))
        {
            result += last;
        }
        return result;
    }

    public String toString()
    {
        return builder.toString();
    }
}
