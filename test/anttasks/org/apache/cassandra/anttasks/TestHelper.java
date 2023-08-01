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

package org.apache.cassandra.anttasks;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.taskdefs.MacroInstance;
import org.apache.tools.ant.taskdefs.Sequential;

public class TestHelper extends Task
{
    private String property;

    public void setProperty(String property)
    {
        this.property = property;
    }

    public void execute()
    {
        Project project = getProject();

        String sep = project.getProperty("path.separator");
        String[] allTestClasses = project.getProperty("all-test-classes").split(sep);

        Sequential seqTask = (Sequential) project.createTask("sequential");
        for (int i=0; i< allTestClasses.length; i++)
        {
            if (allTestClasses[i] == null)
                continue;

            MacroInstance task = (MacroInstance) project.createTask(property);
            task.setDynamicAttribute("test.file.list", ' ' + allTestClasses[i]);
            seqTask.addTask(task);
        }

        seqTask.perform();
    }
}
