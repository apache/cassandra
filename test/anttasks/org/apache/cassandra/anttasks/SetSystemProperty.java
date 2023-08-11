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

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.PropertyHelper;
import org.apache.tools.ant.Task;

public class SetSystemProperty extends Task
{
    private String name;
    private String value;

    public void setName(String name)
    {
        this.name = name;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    @Override
    public void execute() throws BuildException
    {
        if (name == null || name.isEmpty())
        {
            throw new BuildException("propertyName attribute is missing or empty.");
        }

        if (value == null || value.isEmpty())
        {
            throw new BuildException("propertyValue attribute is missing or empty.");
        }

        PropertyHelper propertyHelper = PropertyHelper.getPropertyHelper(getProject());
        String evaluatedValue = propertyHelper.replaceProperties(value);

        if (evaluatedValue == null)
            System.getProperties().remove(name);
        else
            // checkstyle: suppress below 'blockSystemPropertyUsage'
            System.setProperty(name, evaluatedValue);
    }
}