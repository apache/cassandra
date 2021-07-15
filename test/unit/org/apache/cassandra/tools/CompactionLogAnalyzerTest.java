/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class CompactionLogAnalyzerTest
{
    private final String UNREPAIRED_SHARD_NAME = "0-Unrepaired-shard_0";
    private final String REPAIRED_SHARD_NAME = "0-Repaired-shard_1";

    private File[] logFiles;

    @Before
    public void before()
    {
        ClassLoader classLoader = getClass().getClassLoader();
        File dir = new File(classLoader.getResource("org/apache/cassandra/tools/compaction_logs").getFile());
        Assert.assertTrue(dir.isDirectory());

        logFiles = dir.listFiles();
        Assert.assertEquals(2, logFiles.length);
        Arrays.sort(logFiles, Comparator.comparing(File::getName));
    }

    @Test
    public void testReadDataPointsWithoutLimit() throws IOException, ParseException
    {
        List<CompactionLogAnalyzer.DataPoint> dataPoints = CompactionLogAnalyzer.readDataPoints(logFiles, null);

        long lineCount0 = Files.lines(logFiles[0].toPath()).count();
        long lineCount1 = Files.lines(logFiles[1].toPath()).count();
        Assert.assertEquals(lineCount0 + lineCount1 - 2, dataPoints.size()); // without headers

        int shard0Cnt = 0;
        int shard1Cnt = 0;
        for (CompactionLogAnalyzer.DataPoint dataPoint : dataPoints)
        {
            if (dataPoint.shardId.equals(REPAIRED_SHARD_NAME))
            {
                shard0Cnt++;
            }
            else if (dataPoint.shardId.equals(UNREPAIRED_SHARD_NAME))
            {
                shard1Cnt++;
            }
            else
            {
                throw new AssertionError("Unexpected shard id");
            }
        }
        Assert.assertEquals(lineCount0 - 1, shard0Cnt);
        Assert.assertEquals(lineCount1 - 1, shard1Cnt);
    }

    @Test
    public void testReadDataPointsWithLimit() throws IOException, ParseException
    {
        int limit = 100;

        List<CompactionLogAnalyzer.DataPoint> dataPoints = CompactionLogAnalyzer.readDataPoints(logFiles, limit);

        Assert.assertEquals(2 * limit, dataPoints.size()); // without headers

        int shard0Cnt = 0;
        int shard1Cnt = 0;
        for (CompactionLogAnalyzer.DataPoint dataPoint : dataPoints)
        {
            if (dataPoint.shardId.equals(REPAIRED_SHARD_NAME))
            {
                shard0Cnt++;
            }
            else if (dataPoint.shardId.equals(UNREPAIRED_SHARD_NAME))
            {
                shard1Cnt++;
            }
            else
            {
                throw new AssertionError("Unexpected shard id");
            }
        }
        Assert.assertEquals(limit, shard0Cnt);
        Assert.assertEquals(limit, shard1Cnt);
    }

    @Test
    public void testProcessDataPoints() throws IOException, ParseException
    {
        List<CompactionLogAnalyzer.DataPoint> dataPoints = CompactionLogAnalyzer.readDataPoints(logFiles, null);
        JSONArray jsonArray = CompactionLogAnalyzer.processData(dataPoints);
        int expectedLevels = dataPoints.stream()
                                       .mapToInt(dp -> dp.bucket)
                                       .max()
                                       .getAsInt() + 1; // + L0
        Assert.assertEquals(expectedLevels + 1, jsonArray.size());  // + Total

        JSONArray[] intervals = new JSONArray[expectedLevels + 1];
        for (int i = 0; i < intervals.length; i++)
        {
            intervals[i] = (JSONArray) ((JSONObject) jsonArray.get(0)).get("intervals");
        }

        for (int i = 1; i < intervals.length; i++)
        {
            Assert.assertEquals(intervals[0].size(), intervals[i].size());
        }
    }
}