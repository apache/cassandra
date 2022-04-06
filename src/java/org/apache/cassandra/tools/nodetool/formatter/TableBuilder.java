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

package org.apache.cassandra.tools.nodetool.formatter;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Build and print table.
 *
 * usage:
 * <pre>
 * {@code
 * TableBuilder table = new TableBuilder();
 * for (String[] row : data)
 * {
 *     table.add(row);
 * }
 * table.print(probe.outStream());
 * }
 * </pre>
 */
public class TableBuilder
{
    // column delimiter
    private final String columnDelimiter;

    private int[] maximumColumnWidth;
    private final List<String[]> rows = new ArrayList<>();

    public TableBuilder()
    {
        this(' ');
    }

    public TableBuilder(char columnDelimiter)
    {
        this(String.valueOf(columnDelimiter));
    }

    public TableBuilder(String columnDelimiter)
    {
        this.columnDelimiter = columnDelimiter;
    }

    private TableBuilder(TableBuilder base, int[] maximumColumnWidth)
    {
        this(base.columnDelimiter);
        this.maximumColumnWidth = maximumColumnWidth;
        this.rows.addAll(base.rows);
    }

    public void add(@Nonnull List<String> row)
    {
        add(row.toArray(new String[0]));
    }

    public void add(@Nonnull String... row)
    {
        Objects.requireNonNull(row);

        if (rows.isEmpty())
        {
            maximumColumnWidth = new int[row.length];
        }

        // expand max column widths if given row has more columns
        if (row.length > maximumColumnWidth.length)
        {
            int[] tmp = new int[row.length];
            System.arraycopy(maximumColumnWidth, 0, tmp, 0, maximumColumnWidth.length);
            maximumColumnWidth = tmp;
        }
        // calculate maximum column width
        int i = 0;
        for (String col : row)
        {
            maximumColumnWidth[i] = Math.max(maximumColumnWidth[i], col != null ? col.length() : 1);
            i++;
        }
        rows.add(row);
    }

    public void printTo(PrintStream out)
    {
        if (rows.isEmpty())
            return;

        for (String[] row : rows)
        {
            for (int i = 0; i < maximumColumnWidth.length; i++)
            {
                String col = i < row.length ? row[i] : "";
                out.print(String.format("%-" + maximumColumnWidth[i] + 's', col != null ? col : ""));
                if (i < maximumColumnWidth.length - 1)
                    out.print(columnDelimiter);
            }
            out.println();
        }
    }

    @Override
    public String toString()
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (PrintStream stream = new PrintStream(os, true, StandardCharsets.UTF_8.displayName()))
        {
            printTo(stream);
            stream.flush();
            return os.toString(StandardCharsets.UTF_8.displayName());
        }
        catch (UnsupportedEncodingException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Share max offsets across multiple TableBuilders
     */
    public static class SharedTable {
        private List<TableBuilder> tables = new ArrayList<>();
        private final String columnDelimiter;

        public SharedTable()
        {
            this(' ');
        }

        public SharedTable(char columnDelimiter)
        {
            this(String.valueOf(columnDelimiter));
        }

        public SharedTable(String columnDelimiter)
        {
            this.columnDelimiter = columnDelimiter;
        }

        public TableBuilder next()
        {
            TableBuilder next = new TableBuilder(columnDelimiter);
            tables.add(next);
            return next;
        }

        public List<TableBuilder> complete()
        {
            if (tables.size() == 0)
                return Collections.emptyList();

            final int columns = tables.stream()
                                      .max(Comparator.comparing(tb -> tb.maximumColumnWidth.length))
                                      .get().maximumColumnWidth.length;

            final int[] maximumColumnWidth = new int[columns];
            for (TableBuilder tb : tables)
            {
                for (int i = 0; i < tb.maximumColumnWidth.length; i++)
                {
                    maximumColumnWidth[i] = Math.max(tb.maximumColumnWidth[i], maximumColumnWidth[i]);
                }
            }
            return tables.stream()
                         .map(tb -> new TableBuilder(tb, maximumColumnWidth))
                         .collect(Collectors.toList());
        }
    }
}
