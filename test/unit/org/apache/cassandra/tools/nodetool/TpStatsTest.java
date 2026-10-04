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

package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import org.apache.cassandra.cql3.CQLNodetoolProtocolTester;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.net.Verb.ECHO_REQ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class TpStatsTest extends CQLNodetoolProtocolTester
{

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testTpStats() throws Throwable
    {
        ToolRunner.ToolResult tool = invokeNodetool("tpstats");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        assertThat(stdout).containsPattern("Pool Name \\s+ Active Pending Completed Blocked All time blocked");
        assertThat(stdout).contains("Latencies waiting in queue (micros) per dropped message types");

        // Does inserting data alter tpstats?
        String nonZeroedThreadsRegExp = "((?m)\\D.*[1-9].*)";
        ArrayList<String> origStats = getAllGroupMatches(nonZeroedThreadsRegExp, stdout);
        Collections.sort(origStats);

        createTable("CREATE TABLE %s (pk int, c int, PRIMARY KEY(pk))");
        execute("INSERT INTO %s (pk, c) VALUES (?, ?)", 1, 1);
        flush();

        tool = invokeNodetool("tpstats");
        tool.assertOnCleanExit();
        stdout = tool.getStdout();
        ArrayList<String> newStats = getAllGroupMatches(nonZeroedThreadsRegExp, stdout);
        Collections.sort(newStats);

        assertThat(origStats).isNotEqualTo(newStats);

        // Does sending a message alter GossipStage stats?
        // Use relative comparison since stats are cumulative across parameterized runs.
        String origGossip = getAllGroupMatches("((?m)GossipStage.*)", stdout).get(0);

        CountDownLatch latch = new CountDownLatch(1);
        Message<NoPayload> echoMessageOut = Message.out(ECHO_REQ, NoPayload.noPayload);
        MessagingService.instance().sendWithCallback(echoMessageOut, FBUtilities.getBroadcastAddressAndPort(),
                                                     (msg) -> latch.countDown());
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        tool = invokeNodetool("tpstats");
        tool.assertOnCleanExit();
        stdout = tool.getStdout();
        String newGossip = getAllGroupMatches("((?m)GossipStage.*)", stdout).get(0);

        // We intentionally do not assert on ECHO_REQ changes here. The ECHO_REQ line
        // in tpstats reports histogram percentiles (DecayingEstimatedHistogramReservoir) which
        // use fixed bucket boundaries. Adding samples that fall into the same buckets as existing
        // ones does not change the reported percentile values, making string comparison unreliable.
        // The GossipStage completed count (a monotonic counter) is enough to verify that
        // ECHO_REQ messages were processed, since ECHO_REQ is handled on the GOSSIP stage.
        assertThat(origGossip).isNotEqualTo(newGossip);
    }

    @Test
    public void testFormatArg()
    {
        Arrays.asList(Pair.of("-F", "json"), Pair.of("--format", "json")).forEach(arg -> {
            ToolRunner.ToolResult tool = invokeNodetool("tpstats", arg.getLeft(), arg.getRight());
            tool.assertOnCleanExit();
            String json = tool.getStdout();
            assertThat(isJSONString(json)).isTrue();
            assertThat(json).containsPattern("\"WaitLatencies\"\\s*:\\s*\\{\\s*\"");
        });

        Arrays.asList( Pair.of("-F", "yaml"), Pair.of("--format", "yaml")).forEach(arg -> {
            ToolRunner.ToolResult tool = invokeNodetool("tpstats", arg.getLeft(), arg.getRight());
            tool.assertOnCleanExit();
            String yaml = tool.getStdout();
            assertThat(isYAMLString(yaml)).isTrue();
            assertThat(yaml).containsPattern("WaitLatencies:\\s*[A-Z|_]+:\\s+-\\s");
        });
    }

    public static boolean isJSONString(String str)
    {
        try
        {
            JsonUtils.JSON_OBJECT_MAPPER.readTree(str);
            return true;
        }
        catch(IOException e)
        {
            return false;
        }
    }

    public static boolean isYAMLString(String str)
    {
        try
        {
            Yaml yaml = new Yaml();
            yaml.load(str);
            return true;
        }
        catch(Exception e)
        {
            return false;
        }
    }

    private ArrayList<String> getAllGroupMatches(String regExp, String in)
    {
        Pattern pattern = Pattern.compile(regExp);
        Matcher m = pattern.matcher(in);

        ArrayList<String> matches = new ArrayList<>();
        while (m.find())
            matches.add(m.group(1));

        return matches;
    }
}
