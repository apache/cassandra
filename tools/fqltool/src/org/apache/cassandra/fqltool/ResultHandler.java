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

package org.apache.cassandra.fqltool;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultHandler implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(ResultHandler.class);
    private final ResultStore resultStore;
    private final ResultComparator resultComparator;
    private final List<String> targetHosts;

    public ResultHandler(List<String> targetHosts, List<File> resultPaths, File queryFilePath)
    {
        this(targetHosts, resultPaths, queryFilePath, null);
    }

    public ResultHandler(List<String> targetHosts, List<File> resultPaths, File queryFilePath, MismatchListener mismatchListener)
    {
        this.targetHosts = targetHosts;
        resultStore = resultPaths != null ? new ResultStore(resultPaths, queryFilePath) : null;
        resultComparator = new ResultComparator(mismatchListener);
    }

    /**
     * Since we can't iterate a ResultSet more than once, and we don't want to keep the entire result set in memory
     * we feed the rows one-by-one to resultComparator and resultStore.
     *
     * results.get(x) should be the results from executing query against targetHosts.get(x)
     */
    public void handleResults(FQLQuery query, List<ComparableResultSet> results)
    {
        for (int i = 0; i < targetHosts.size(); i++)
        {
            if (results.get(i).wasFailed())
                logger.error("Query {} against {} failure: {}", query, targetHosts.get(i), results.get(i).getFailureException().getMessage());
        }

        List<ComparableColumnDefinitions> columnDefinitions = results.stream().map(ComparableResultSet::getColumnDefinitions).collect(Collectors.toList());
        resultComparator.compareColumnDefinitions(targetHosts, query, columnDefinitions);
        if (resultStore != null)
            resultStore.storeColumnDefinitions(query, columnDefinitions);
        List<Iterator<ComparableRow>> iters = results.stream().map(Iterable::iterator).collect(Collectors.toList());

        while (true)
        {
            List<ComparableRow> rows = rows(iters);
            resultComparator.compareRows(targetHosts, query, rows);
            if (resultStore != null)
                resultStore.storeRows(rows);
            // all rows being null marks end of all resultsets, we need to call compareRows
            // and storeRows once with everything null to mark that fact
            if (rows.stream().allMatch(Objects::isNull))
                return;
        }
    }

    /**
     * Get the first row from each of the iterators, if the iterator has run out, null will mark that in the list
     */
    @VisibleForTesting
    public static List<ComparableRow> rows(List<Iterator<ComparableRow>> iters)
    {
        List<ComparableRow> rows = new ArrayList<>(iters.size());
        for (Iterator<ComparableRow> iter : iters)
        {
            if (iter.hasNext())
                rows.add(iter.next());
            else
                rows.add(null);
        }
        return rows;
    }

    public void close() throws IOException
    {
        if (resultStore != null)
            resultStore.close();
    }

    public interface ComparableResultSet extends Iterable<ComparableRow>
    {
        public ComparableColumnDefinitions getColumnDefinitions();
        public boolean wasFailed();
        public Throwable getFailureException();
    }

    public interface ComparableColumnDefinitions extends Iterable<ComparableDefinition>
    {
        public List<ComparableDefinition> asList();
        public boolean wasFailed();
        public Throwable getFailureException();
        public int size();
    }

    public interface ComparableDefinition
    {
        public String getType();
        public String getName();
    }

    public interface ComparableRow
    {
        public ByteBuffer getBytesUnsafe(int i);
        public ComparableColumnDefinitions getColumnDefinitions();
    }

}
