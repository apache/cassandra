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

import com.google.common.base.Throwables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.fqltool.commands.Compare;
import org.apache.cassandra.fqltool.commands.Dump;
import org.apache.cassandra.fqltool.commands.Replay;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import static com.google.common.base.Throwables.getStackTraceAsString;

@Command(name = "fqltool",
         description = "Manipulate the contents of full query log files",
         subcommands = { CommandLine.HelpCommand.class, Dump.class, Replay.class, Compare.class })
public class FullQueryLogTool implements Runnable
{
    public static void main(String... args)
    {
        DatabaseDescriptor.clientInitialization();
        CommandLine commandLine = new CommandLine(FullQueryLogTool.class);
        commandLine.setExecutionExceptionHandler((ex, c, arg) -> {
                       // Used for backward compatibility, some commands are validated when a command is run.
                       if (ex instanceof IllegalArgumentException | ex instanceof IllegalStateException)
                       {
                           badUse(ex);
                           return 1;
                       }
                       err(Throwables.getRootCause(ex));
                       return 2;
                   })
                   .setParameterExceptionHandler((ex, arg) -> {
                       badUse(ex);
                       return 1;
                   });

        System.exit(commandLine.execute(args));
    }

    @Override
    public void run()
    {
        CommandLine.usage(this, System.out);
    }

    private static void badUse(Exception e)
    {
        System.out.println("fqltool: " + e.getMessage());
        System.out.println("See 'fqltool help' or 'fqltool help <command>'.");
    }

    private static void err(Throwable e)
    {
        System.err.println("error: " + e.getMessage());
        System.err.println("-- StackTrace --");
        System.err.println(getStackTraceAsString(e));
    }
}
