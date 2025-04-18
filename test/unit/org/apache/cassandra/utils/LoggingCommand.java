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

package org.apache.cassandra.utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Property;
import accord.utils.Property.Command;

import static accord.utils.Property.multistep;

public class LoggingCommand<State, SystemUnderTest, Result> extends Property.ForwardingCommand<State, SystemUnderTest, Result>
{
    private static final Logger logger = LoggerFactory.getLogger(LoggingCommand.class);

    public LoggingCommand(Command<State, SystemUnderTest, Result> delegate)
    {
        super(delegate);
    }

    public static <State, SystemUnderTest> BiFunction<State, Gen<Command<State, SystemUnderTest, ?>>, Gen<Command<State, SystemUnderTest, ?>>> factory()
    {
        return (state, commandGen) -> rs -> {
            Command<State, SystemUnderTest, ?> c = commandGen.next(rs);
            if (!(c instanceof Property.MultistepCommand))
                return new LoggingCommand<>(c);
            Property.MultistepCommand<State, SystemUnderTest> multistep = (Property.MultistepCommand<State, SystemUnderTest>) c;
            List<Command<State, SystemUnderTest, ?>> subcommands = new ArrayList<>();
            for (var sub : multistep)
                subcommands.add(new LoggingCommand<>(sub));
            return multistep(subcommands);
        };
    }

    @Override
    public Result apply(State s) throws Throwable
    {
        String name = detailed(s);
        long startNanos = Clock.Global.nanoTime();
        try
        {
            logger.info("Starting command: {}", name);
            Result o = super.apply(s);
            logger.info("Command {} was success after {}", name, Duration.ofNanos(Clock.Global.nanoTime() - startNanos));
            return o;
        }
        catch (Throwable t)
        {
            logger.warn("Command {} failed after {}: {}", name, Duration.ofNanos(Clock.Global.nanoTime() - startNanos), t.toString()); // don't want stack trace, just type/msg
            throw t;
        }
    }
}
