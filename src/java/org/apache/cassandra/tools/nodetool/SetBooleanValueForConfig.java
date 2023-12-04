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

import java.util.ArrayList;
import java.util.List;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setbooleanvalueforconfig",
         description = "Set boolean value for single boolean field for org.apache.cassandra.config.Config object, note:" +
                       " This cmd is for C* source code expert only. Only use this command on the field when you are " +
                       "absolutely sure about the consequence of simply modifying the boolean value in DatabaseDescriptor.java")
public class SetBooleanValueForConfig extends NodeTool.NodeToolCmd
{
    @Arguments(title = "<configname> <value>", usage = "<configname> <value>",
               description = "The field name to be changed and target value to set the field to", required = true)
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2,
                      "setbooleanvalueforconfig requires config-name, and boolean value args.");
        String fieldName = args.get(0);
        String booleanValue = args.get(1);
        boolean value;
        switch (booleanValue)
        {
            case "true":
                value = true;
                break;
            case "false":
                value = false;
                break;
            default:
                System.out.println("Unknown boolean flag: " + booleanValue);
                return;
        }
        System.out.println(probe.setBooleanValueForConfig(fieldName, value));
    }
}
