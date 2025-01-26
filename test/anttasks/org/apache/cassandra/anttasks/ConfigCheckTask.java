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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.cassandra.config.Config;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

public class ConfigCheckTask extends Task
{
    private String configToCheck;

    private int failThreshold = Integer.MAX_VALUE;

    public void setConfigToCheck(String configToCheck)
    {
        this.configToCheck = configToCheck;
    }

    public void setFailThreshold(int failThreshold)
    {
        this.failThreshold = failThreshold;
    }

    public void execute()
    {
        try
        {
            Field[] allFields = Config.class.getFields();
            List<String> topLevelPropertyNames = new ArrayList<>();
            for (Field field : allFields)
            {
                if (!Modifier.isStatic(field.getModifiers()))
                {
                    topLevelPropertyNames.add(field.getName());
                }
            }

            List<String> lines = Files.readAllLines(Paths.get(configToCheck));

            int missedCount = 0;
            log("The following Config.java properties are not described in " + configToCheck);
            for (String propertyName : topLevelPropertyNames)
            {
                Pattern propertyRegexp = Pattern.compile("^#?\\s*" + propertyName + ":.*");
                boolean found = false;
                for (String line : lines)
                {
                    if (propertyRegexp.matcher(line).find())
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    missedCount++;
                    log(propertyName);
                }
            }
            log("Total missed properties: " + missedCount);
            if (missedCount > failThreshold)
                throw new BuildException(missedCount + " properties are not described in " + configToCheck);
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
