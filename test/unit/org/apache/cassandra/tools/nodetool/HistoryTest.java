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

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.Output;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class HistoryTest
{
    public static File tempHistoryFile = FileUtils.createTempFile("test_nodetool", "history");
    public static File emptyHistoryFile = FileUtils.createTempFile("test_nodetool_empty", "history");
    public static List<String> listOfCommands = List.of("oldest command",
                                                        "older command",
                                                        "newer command",
                                                        "newest command");

    @BeforeClass
    public static void populateHistoryFile()
    {
        FileUtils.write(tempHistoryFile, listOfCommands);
    }

    @Test
    public void testHistory()
    {
        History history = new History()
        {
            @Override
            File getHistoryFile()
            {
                return HistoryTest.tempHistoryFile;
            }
        };

        history.logger(Output.CONSOLE);
        File historyFile = history.getHistoryFile();

        assertThat(history.commandsToPrint(historyFile)).containsExactly(listOfCommands.toArray(new String[0]));

        history.commands = 2;
        assertThat(history.commandsToPrint(historyFile)).containsExactly(listOfCommands.subList(2, listOfCommands.size()).toArray(new String[0]));

        history.commands = 4;
        assertThat(history.commandsToPrint(historyFile)).containsExactly(listOfCommands.toArray(new String[0]));
    }

    @Test
    public void testEmptyHistory()
    {
        History history = new History()
        {
            @Override
            File getHistoryFile()
            {
                return HistoryTest.emptyHistoryFile;
            }
        };

        history.logger(Output.CONSOLE);
        File historyFile = history.getHistoryFile();

        assertThat(history.commandsToPrint(historyFile)).isEmpty();
    }


    @Test
    public void testHistoryOutputOnNonExistingHistoryFile()
    {
        History history = new History()
        {
            @Override
            File getHistoryFile()
            {
                return new File("/does/not/exist");
            }
        };

        history.logger(Output.CONSOLE);

        assertThatThrownBy(() -> history.execute(null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("History file /does/not/exist does not exist.\n");
    }

    @Test
    public void testInvalidNumberOfCommandsToPrint()
    {
        History history = new History();
        history.commands = 0;

        assertThatThrownBy(() -> history.execute(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Number of commands to display has to be at least 1.");
    }
}
