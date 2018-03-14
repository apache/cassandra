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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
 * table.print(System.out);
 * }
 * </pre>
 */
public class TableBuilder
{
    // column delimiter char
    private final char columnDelimiter;

    private int[] maximumColumnWidth;
    private final List<String[]> rows = new ArrayList<>();

    public TableBuilder()
    {
        this(' ');
    }

    public TableBuilder(char columnDelimiter)
    {
        this.columnDelimiter = columnDelimiter;
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
}
