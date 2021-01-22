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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.Test;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.fqltool.commands.Compare;
import org.apache.cassandra.tools.Util;


import static org.psjava.util.AssertStatus.assertTrue;

public class FQLCompareTest
{
    public FQLCompareTest()
    {
        Util.initDatabaseDescriptor();
    }

    @Test
    public void endToEnd() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        File queryDir = Files.createTempDirectory("queries").toFile();
        List<File> resultPaths = generateResultSets(targetHosts, tmpDir, queryDir, true, false);
        Compare.compare(queryDir.toString(), resultPaths.stream().map(File::toString).collect(Collectors.toList()));
    }

    @Test
    public void endToEndQueryFailures() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        File queryDir = Files.createTempDirectory("queries").toFile();
        List<File> resultPaths = generateResultSets(targetHosts, tmpDir, queryDir, true,true);
        Compare.compare(queryDir.toString(), resultPaths.stream().map(File::toString).collect(Collectors.toList()));
    }

    @Test
    public void compareEqual() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        File queryDir = Files.createTempDirectory("queries").toFile();
        List<File> resultPaths = generateResultSets(targetHosts, tmpDir, queryDir, false,false);

        ResultComparator comparator = new ResultComparator();
        List<ChronicleQueue> readQueues = null;
        try
        {
            readQueues = resultPaths.stream().map(s -> SingleChronicleQueueBuilder.single(s).readOnly(true).build()).collect(Collectors.toList());
            List<Iterator<ResultHandler.ComparableResultSet>> its = readQueues.stream().map(q -> new Compare.StoredResultSetIterator(q.createTailer())).collect(Collectors.toList());
            List<ResultHandler.ComparableResultSet> resultSets = Compare.resultSets(its);
            while(resultSets.stream().allMatch(Objects::nonNull))
            {
                assertTrue(comparator.compareColumnDefinitions(targetHosts, query(), resultSets.stream().map(ResultHandler.ComparableResultSet::getColumnDefinitions).collect(Collectors.toList())));
                List<Iterator<ResultHandler.ComparableRow>> rows = resultSets.stream().map(Iterable::iterator).collect(Collectors.toList());

                List<ResultHandler.ComparableRow> toCompare = ResultHandler.rows(rows);

                while (toCompare.stream().allMatch(Objects::nonNull))
                {
                    assertTrue(comparator.compareRows(targetHosts, query(), ResultHandler.rows(rows)));
                    toCompare = ResultHandler.rows(rows);
                }
                resultSets = Compare.resultSets(its);
            }
        }
        finally
        {
            if (readQueues != null)
                readQueues.forEach(Closeable::close);
        }
    }

    private List<File> generateResultSets(List<String> targetHosts, File resultDir, File queryDir, boolean random, boolean includeFailures) throws IOException
    {
        List<File> resultPaths = new ArrayList<>();
        targetHosts.forEach(host -> { File f = new File(resultDir, host); f.mkdir(); resultPaths.add(f);});

        try (ResultHandler rh = new ResultHandler(targetHosts, resultPaths, queryDir))
        {
            for (int i = 0; i < 100; i++)
            {
                ResultHandler.ComparableResultSet resultSet1 = includeFailures && (i % 10 == 0)
                                                               ? StoredResultSet.failed("test failure!")
                                                               : FQLReplayTest.createResultSet(10, 10, random);
                ResultHandler.ComparableResultSet resultSet2 = FQLReplayTest.createResultSet(10, 10, random);
                rh.handleResults(query(), Lists.newArrayList(resultSet1, resultSet2));
            }
        }
        return resultPaths;
    }

    private FQLQuery.Single query()
    {
        return new FQLQuery.Single("abc", QueryOptions.DEFAULT.getProtocolVersion().asInt(), QueryOptions.DEFAULT, 12345, 5555, 6666, "select * from xyz", Collections.emptyList());
    }
}
