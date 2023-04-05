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

import java.util.List;

import com.google.common.base.Throwables;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseCommandMissingException;
import io.airlift.airline.ParseCommandUnrecognizedException;
import io.airlift.airline.ParseOptionConversionException;
import io.airlift.airline.ParseOptionMissingException;
import io.airlift.airline.ParseOptionMissingValueException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.fqltool.commands.Compare;
import org.apache.cassandra.fqltool.commands.Dump;
import org.apache.cassandra.fqltool.commands.Replay;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.Lists.newArrayList;

public class FullQueryLogTool
{
    public static void main(String... args)
    {
        DatabaseDescriptor.clientInitialization();
        List<Class<? extends Runnable>> commands = newArrayList(
                Help.class,
                Dump.class,
                Replay.class,
                Compare.class
        );

        Cli.CliBuilder<Runnable> builder = Cli.builder("fqltool");

        builder.withDescription("Manipulate the contents of full query log files")
                 .withDefaultCommand(Help.class)
                 .withCommands(commands);

        Cli<Runnable> parser = builder.build();

        int status = 0;
        try
        {
            parser.parse(args).run();
        } catch (IllegalArgumentException |
                IllegalStateException |
                ParseArgumentsMissingException |
                ParseArgumentsUnexpectedException |
                ParseOptionConversionException |
                ParseOptionMissingException |
                ParseOptionMissingValueException |
                ParseCommandMissingException |
                ParseCommandUnrecognizedException e)
        {
            badUse(e);
            status = 1;
        } catch (Throwable throwable)
        {
            err(Throwables.getRootCause(throwable));
            status = 2;
        }

        System.exit(status);
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
