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

package org.apache.cassandra.management.picocli;

import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.tools.nodetool.AbstractCommand;

/**
 * Utility class for extracting command metadata from picocli-annotated command classes.
 */
public class PicocliMetadataExtractor
{
    /**
     * Extract CommandMetadata from an AbstractCommand class.
     *
     * @param commandClass The command class annotated with picocli @Command
     * @return CommandMetadata extracted from the command class
     */
    public static CommandMetadata extract(Class<? extends AbstractCommand> commandClass)
    {
        return PicocliCommandMetadata.from(commandClass);
    }

    /**
     * Extract CommandMetadata from a command instance.
     *
     * @param commandInstance The command instance
     * @return CommandMetadata extracted from the command instance
     */
    public static CommandMetadata extract(Object commandInstance)
    {
        if  (commandInstance instanceof AbstractCommand)
            return PicocliCommandMetadata.from(commandInstance);
        throw new IllegalArgumentException("Unsupported command instance type: " + commandInstance.getClass().getName());
    }
}

