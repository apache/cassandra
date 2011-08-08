/**
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
package org.apache.cassandra.cli;

import jline.SimpleCompletor;

public class CliCompleter extends SimpleCompletor
{
    private static String[] commands = {
            "connect",
            "describe keyspace",
            "exit",
            "help",
            "quit",
            "show cluster name",
            "show keyspaces",
            "show schema",
            "show api version",
            "create keyspace",
            "create column family",
            "drop keyspace",
            "drop column family",
            "rename keyspace",
            "rename column family",
            "consistencylevel",
            
            "help connect",
            "help describe keyspace",
            "help exit",
            "help help",
            "help quit",
            "help show cluster name",
            "help show keyspaces",
            "help show schema",
            "help show api version",
            "help create keyspace",
            "help create column family",
            "help drop keyspace",
            "help drop column family",
            "help rename keyspace",
            "help rename column family",
            "help get",
            "help set",
            "help del",
            "help count",
            "help list",
            "help truncate",
            "help consistencylevel"
    };
    private static String[] keyspaceCommands = {
            "get",
            "set",
            "count",
            "del",
            "list",
            "truncate",
            "incr",
            "decr"
    };

    public CliCompleter()
    {
        super(commands);
    }
    
    String[] getKeyspaceCommands()
    {
        return keyspaceCommands;
    }
}
