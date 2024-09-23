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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setvalueforconfig",
         description = "Set value for single field for org.apache.cassandra.config.Config object, note:" +
                       " This cmd is for C* source code expert only. Only use this command on the field when you are " +
                       "absolutely sure about the consequence of simply modifying the boolean value in DatabaseDescriptor.java")
public class SetValueForConfig extends NodeTool.NodeToolCmd
{
    @Arguments(title = "<configname> <type> <value>", usage = "<configname> <type> <value>",
               description = "The field name to be changed, the field type and target value to set the field to", required = true)
    protected List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 3,
                      "setvalueforconfig requires config-name, field type and the value args.");
        String fieldName = args.get(0);
        String type = args.get(1);
        String valueString = args.get(2);
        Object value;
        switch (type)
        {
            case "boolean":
                if (valueString.equals("true"))
                {
                    value = true;
                } else if (valueString.equals("false"))
                {
                    value = false;
                } else {
                    System.out.println("Unknown boolean flag: " + valueString);
                    return;
                }
                break;
            case "int":
                try
                {
                    value = Integer.valueOf(valueString);
                }
                catch (Exception e)
                {
                    System.out.println("Unknow integer value: " + valueString);
                    return;
                }
                break;
            case "long":
                try
                {
                    value = Long.valueOf(valueString);
                }
                catch (Exception e)
                {
                    System.out.println("Unknow long value: " + valueString);
                    return;
                }
                break;
            case "double":
                try
                {
                    value = Double.valueOf(valueString);
                }
                catch (Exception e)
                {
                    System.out.println("Unknow double value: " + valueString);
                    return;
                }
                break;
            case "durationspec":
                try
                {
                    Matcher matcher = DurationSpec.UNITS_PATTERN.matcher(valueString);
                    if (matcher.find())
                    {
                        long quantity = Long.parseLong(matcher.group(1));
                        String unit = matcher.group(2);
                        // Only support the long type for ns, ms abd s, and int type for m
                        if (unit.equals("ns"))
                        {
                            value = new DurationSpec.LongNanosecondsBound(quantity);
                        } else if (unit.equals("ms"))
                        {
                            value = new DurationSpec.LongMillisecondsBound(quantity);
                        } else if (unit.equals("s"))
                        {
                            value = new DurationSpec.LongSecondsBound(quantity);
                        } else if (unit.equals("m"))
                        {
                            value = new DurationSpec.IntMinutesBound(quantity);
                        } else {
                            // For all the left unit will use the LongMillisecondsBound
                            value = new DurationSpec.LongMillisecondsBound(quantity, DurationSpec.fromSymbol(unit));
                        }
                    } else
                    {
                        System.out.println("Unknow durationspec value: " + valueString + " Accepted units:" +
                                           DurationSpec.UNITS_PATTERN + " where case matters and only non-negative values");
                        return;
                    }
                }
                catch (Exception e)
                {
                    System.out.println("Unknow durationspec value: " + valueString);
                    return;
                }
                break;
            default:
                System.out.println("Unknow type value: " + type);
                return;
        }
        System.out.println(probe.setValueForConfig(fieldName, value));
    }
}
