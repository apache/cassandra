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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class SetCachePeriodTest extends CQLTester
{
    private static CacheService cacheService;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
        cacheService = CacheService.instance;
    }

    @Test
    public void testHelp()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "setcacheperiod");
        tool.assertOnExitCode();

        String help =   "NAME\n" +
                        "        nodetool setcacheperiod - Set global key, row, and counter cache period\n" +
                        "        in second units\n" +
                        "\n" +
                        "SYNOPSIS\n" +
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                        "                [(-u <username> | --username <username>)] setcacheperiod [--]\n" +
                        "                <key-cache-period> <row-cache-period> <counter-cache-period>\n" +
                        "\n" +
                        "OPTIONS\n" +
                        "        -h <host>, --host <host>\n" +
                        "            Node hostname or ip address\n" +
                        "\n" +
                        "        -p <port>, --port <port>\n" +
                        "            Remote jmx agent port number\n" +
                        "\n" +
                        "        -pp, --print-port\n" +
                        "            Operate in 4.0 mode with hosts disambiguated by port number\n" +
                        "\n" +
                        "        -pw <password>, --password <password>\n" +
                        "            Remote jmx agent password\n" +
                        "\n" +
                        "        -pwf <passwordFilePath>, --password-file <passwordFilePath>\n" +
                        "            Path to the JMX password file\n" +
                        "\n" +
                        "        -u <username>, --username <username>\n" +
                        "            Remote jmx agent username\n" +
                        "\n" +
                        "        --\n" +
                        "            This option can be used to separate command-line options from the\n" +
                        "            list of argument, (useful when arguments might be mistaken for\n" +
                        "            command-line options\n" +
                        "\n" +
                        "        <key-cache-period> <row-cache-period> <counter-cache-period>\n" +
                        "            Key cache, row cache, and counter period in second units\n" +
                        "\n" +
                        "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testOptionalParameters()
    {
        int keyPeriodSetting = 333;
        int rowPeriodSetting = 444;
        int counterPeriodSetting = 555;
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setcacheperiod",
                                                               String.valueOf(keyPeriodSetting),
                                                               String.valueOf(rowPeriodSetting),
                                                               String.valueOf(counterPeriodSetting));
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        assertThat(cacheService.getKeyCacheSavePeriodInSeconds()).isEqualTo(keyPeriodSetting);
        assertThat(cacheService.getRowCacheSavePeriodInSeconds()).isEqualTo(rowPeriodSetting);
        assertThat(cacheService.getCounterCacheSavePeriodInSeconds()).isEqualTo(counterPeriodSetting);
    }
}
