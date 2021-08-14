/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.stress;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.cassandra.stress.report.StressMetrics;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.utils.JsonUtils;

public class StressGraph
{
    private StressSettings stressSettings;
    private enum ReadingMode
    {
        START,
        METRICS,
        AGGREGATES,
        NEXTITERATION
    }
    private String[] stressArguments;

    public StressGraph(StressSettings stressSetttings, String[] stressArguments)
    {
        this.stressSettings = stressSetttings;
        this.stressArguments = stressArguments;
    }

    public void generateGraph()
    {
        File htmlFile = new File(stressSettings.graph.file);
        ObjectNode stats;
        if (htmlFile.isFile())
        {
            try
            {
                String html = new String(Files.readAllBytes(Paths.get(htmlFile.toURI())), StandardCharsets.UTF_8);
                stats = parseExistingStats(html);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Couldn't load existing stats html.");
            }
            stats = this.createJSONStats(stats);
        }
        else
        {
            stats = this.createJSONStats(null);
        }

        try
        {
            PrintWriter out = new PrintWriter(htmlFile);
            String statsBlock = "/* stats start */\nstats = " + JsonUtils.writeAsJsonString(stats) + ";\n/* stats end */\n";
            String html = getGraphHTML().replaceFirst("/\\* stats start \\*/\n\n/\\* stats end \\*/\n", statsBlock);
            out.write(html);
            out.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Couldn't write stats html.");
        }
    }

    private ObjectNode parseExistingStats(String html)
    {
        Pattern pattern = Pattern.compile("(?s).*/\\* stats start \\*/\\nstats = (.*);\\n/\\* stats end \\*/.*");
        Matcher matcher = pattern.matcher(html);
        matcher.matches();
        try
        {
            return (ObjectNode) JsonUtils.JSON_OBJECT_MAPPER.readTree(matcher.group(1));
        }
        catch (IOException e)
        {
            throw new RuntimeException("Couldn't parser stats json: "+e.getMessage(), e);
        }
    }

    private String getGraphHTML()
    {
        try (InputStream graphHTMLRes = StressGraph.class.getClassLoader().getResourceAsStream("org/apache/cassandra/stress/graph/graph.html"))
        {
            return new String(ByteStreams.toByteArray(graphHTMLRes));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /** Parse log and append to stats array */
    private ArrayNode parseLogStats(InputStream log, ArrayNode stats) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(log));
        ObjectNode json = JsonUtils.JSON_OBJECT_MAPPER.createObjectNode();
        ArrayNode intervals = JsonUtils.JSON_OBJECT_MAPPER.createArrayNode();
        boolean runningMultipleThreadCounts = false;
        String currentThreadCount = null;
        Pattern threadCountMessage = Pattern.compile("Running ([A-Z]+) with ([0-9]+) threads .*");
        ReadingMode mode = ReadingMode.START;

        try
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                // Detect if we are running multiple thread counts:
                if (line.startsWith("Thread count was not specified"))
                    runningMultipleThreadCounts = true;

                if (runningMultipleThreadCounts)
                {
                    // Detect thread count:
                    Matcher tc = threadCountMessage.matcher(line);
                    if (tc.matches())
                    {
                        currentThreadCount = tc.group(2);
                    }
                }

                // Detect mode changes
                if (line.equals(StressMetrics.HEAD))
                {
                    mode = ReadingMode.METRICS;
                    continue;
                }
                else if (line.equals("Results:"))
                {
                    mode = ReadingMode.AGGREGATES;
                    continue;
                }
                else if (mode == ReadingMode.AGGREGATES && line.equals(""))
                {
                    mode = ReadingMode.NEXTITERATION;
                }
                else if (line.equals("END") || line.equals("FAILURE"))
                {
                    break;
                }

                // Process lines
                if (mode == ReadingMode.METRICS)
                {
                    ArrayNode metrics = JsonUtils.JSON_OBJECT_MAPPER.createArrayNode();
                    String[] parts = line.split(",");
                    if (parts.length != StressMetrics.HEADMETRICS.length)
                    {
                        continue;
                    }
                    for (String m : parts)
                    {
                        try
                        {
                            metrics.add(new BigDecimal(m.trim()));
                        }
                        catch (NumberFormatException e)
                        {
                            metrics.addNull();
                        }
                    }
                    intervals.add(metrics);
                }
                else if (mode == ReadingMode.AGGREGATES)
                {
                    String[] parts = line.split(":",2);
                    if (parts.length != 2)
                    {
                        continue;
                    }
                    // the graphing js expects lower case names
                    json.put(parts[0].trim().toLowerCase(), parts[1].trim());
                }
                else if (mode == ReadingMode.NEXTITERATION)
                {
                    //Wrap up the results of this test and append to the array.
                    ArrayNode metrics = json.putArray("metrics");
                    for (String metric : StressMetrics.HEADMETRICS) {
                        metrics.add(metric);
                    }
                    json.put("test", stressSettings.graph.operation);
                    if (currentThreadCount == null)
                        json.put("revision", stressSettings.graph.revision);
                    else
                        json.put("revision", String.format("%s - %s threads", stressSettings.graph.revision, currentThreadCount));
                    String command = StringUtils.join(stressArguments, " ").replaceAll("password=.*? ", "password=******* ");
                    json.put("command", command);
                    json.set("intervals", intervals);
                    stats.add(json);

                    //Start fresh for next iteration:
                    json = JsonUtils.JSON_OBJECT_MAPPER.createObjectNode();
                    intervals = JsonUtils.JSON_OBJECT_MAPPER.createArrayNode();
                    mode = ReadingMode.START;
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Couldn't read from temporary stress log file");
        }
        if (json.size() != 0) stats.add(json);
        return stats;
    }

    private ObjectNode createJSONStats(ObjectNode json)
    {
        try (InputStream logStream = Files.newInputStream(stressSettings.graph.temporaryLogFile.toPath()))
        {
            ArrayNode stats;
            if (json == null)
            {
                json = JsonUtils.JSON_OBJECT_MAPPER.createObjectNode();
                stats = JsonUtils.JSON_OBJECT_MAPPER.createArrayNode();
            }
            else
            {
                stats = (ArrayNode) json.get("stats");
            }

            stats = parseLogStats(logStream, stats);

            json.put("title", stressSettings.graph.title);
            json.set("stats", stats);
            return json;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
