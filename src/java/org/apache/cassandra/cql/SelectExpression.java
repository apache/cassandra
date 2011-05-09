/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.cql;

import java.util.ArrayList;
import java.util.List;

/**
 * Select expressions are analogous to the projection in a SQL query. They
 * determine which columns will appear in the result set.  SelectExpression
 * instances encapsulate a parsed expression from a <code>SELECT</code>
 * statement.
 * 
 * See: doc/cql/CQL.html#SpecifyingColumns
 */
public class SelectExpression
{
    public static final int MAX_COLUMNS_DEFAULT = 10000;
    
    private int numColumns = MAX_COLUMNS_DEFAULT;
    private boolean reverseColumns = false;
    private final boolean wildcard;
    private Term start, finish;
    private List<Term> columns;
    
    /**
     * Create a new SelectExpression for a range (slice) of columns.
     * 
     * @param start the starting column name
     * @param finish the finishing column name
     * @param count the number of columns to limit the results to
     * @param reverse true to reverse column order
     */
    public SelectExpression(Term start, Term finish, int count, boolean reverse, boolean wildcard)
    {
        this.start = start;
        this.finish = finish;
        numColumns = count;
        reverseColumns = reverse;
        this.wildcard = wildcard;
    }
    
    /**
     * Create a new SelectExpression for a list of columns.
     * 
     * @param first the first (possibly only) column name to select on.
     * @param count the number of columns to limit the results on
     * @param reverse true to reverse column order
     */
    public SelectExpression(Term first, int count, boolean reverse)
    {
        wildcard = false;
        columns = new ArrayList<Term>();
        columns.add(first);
        numColumns = count;
        reverseColumns = reverse;
    }
    
    /**
     * Add an additional column name to a SelectExpression.
     * 
     * @param addTerm
     */
    public void and(Term addTerm)
    {
        assert !isColumnRange();    // Not possible when invoked by parser
        columns.add(addTerm);
    }
    
    public boolean isColumnRange()
    {
        return (start != null);
    }
    
    public boolean isColumnList()
    {
        return !isColumnRange();
    }
    public int getColumnsLimit()
    {
        return numColumns;
    }

    public boolean isColumnsReversed()
    {
        return reverseColumns;
    }
    
    public void setColumnsReversed(boolean reversed)
    {
        reverseColumns = reversed;
    }
    
    public void setColumnsLimit(int limit)
    {
        numColumns = limit;
    }

    public Term getStart()
    {
        return start;
    }

    public Term getFinish()
    {
        return finish;
    }

    public List<Term> getColumns()
    {
        return columns;
    }

    public boolean isWildcard()
    {
        return wildcard;
    }
}
