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

import java.lang.reflect.Field;
import javax.inject.Inject;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.Output;
import picocli.CommandLine;

public class NodetoolClassHierarchyTest extends CQLTester
{
    @Test
    public void testNoDuplicatesForInjectableFields() throws Exception
    {
        checkInjectableDuplicates(NodeTool.createCommandLine(CommandLine.defaultFactory()));
    }

    private void checkInjectableDuplicates(CommandLine command)
    {
        for (CommandLine sub : command.getSubcommands().values())
            checkInjectableDuplicates(sub);

        if (command.getCommandSpec().userObject() instanceof AbstractCommand)
        {
            AbstractCommand userObject = (AbstractCommand) command.getCommandSpec().userObject();
            int nodeProbeFactoryCount = 0;
            int outputCount = 0;
            Class<?> beanClass = userObject.getClass();
            do
            {
                Field[] fields = beanClass.getDeclaredFields();
                for (Field field : fields)
                {
                    if (!field.isAnnotationPresent(Inject.class))
                        continue;
                    if (field.getType().equals(INodeProbeFactory.class))
                        nodeProbeFactoryCount++;
                    else if (field.getType().equals(Output.class))
                        outputCount++;
                    else
                        throw new AssertionError("Unexpected injectable field type: " + field.getType());
                }
            }
            while ((beanClass = beanClass.getSuperclass()) != null);

            if (nodeProbeFactoryCount > 1 || outputCount > 1)
                throw new AssertionError("Multiple injectable fields in the command class hierarchy (should be exactly 1 for each type): " +
                                         userObject.getClass().getCanonicalName());
        }
    }
}
