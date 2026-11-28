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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.tools.ant.BuildException;
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

        String selectedClass = project.getProperty("test.configured.class");
        String classes = project.getProperty("all-test-classes");
        String[] allTestClasses = null != selectedClass
                                  ? new String[] { selectedClass }
                                  : classes.split(Pattern.quote(project.getProperty("path.separator")));

        Sequential seqTask = (Sequential) project.createTask("sequential");
        for (String test : allTestClasses)
        {
            if (test.isEmpty())
                continue;

            if (!"testlist".equals(property))
            {
                MacroInstance task = (MacroInstance) project.createTask(property);
                task.setDynamicAttribute("test.file.list", ' ' + test);
                seqTask.addTask(task);
                continue;
            }

            Path sourceRoot = project.resolveFile(project.getProperty("test.dir") + '/' +
                                                  project.getProperty("test.classlistprefix")).toPath();
            String className = null != selectedClass ? selectedClass
                               : sourceRoot.relativize(Path.of(test)).toString().replace(java.io.File.separatorChar, '.')
                                           .replaceFirst("\\.java$", "");
            try (InputStream bytecode = Files.newInputStream(project.resolveFile(project.getProperty("test.classes"))
                                                                   .toPath().resolve(className.replace('.', '/') + ".class")))
            {
                List<String> configurations = TestConfiguration.declaredConfigurations(bytecode);
                String override = project.getProperty("test.configuration");
                boolean configured = null != override || !configurations.isEmpty();
                if (null != override)
                    configurations = Collections.singletonList(override);
                else if (configurations.isEmpty())
                    configurations = Collections.singletonList("default");
                for (String configuration : configurations)
                {
                    String configargs = "";
                    if (configured)
                    {
                        Path yaml = TestConfiguration.yaml(project.resolveFile(project.getProperty("test.conf")).toPath(),
                                                           project.resolveFile(project.getProperty("build.test.dir")).toPath(),
                                                           configuration);
                        configargs = "-Dcassandra.config=" + yaml.toUri();
                    }
                    MacroInstance task = (MacroInstance) project.createTask("testlist-configured");
                    task.setDynamicAttribute("test.class.name", className);
                    task.setDynamicAttribute("configuration", configured ? configuration : "");
                    task.setDynamicAttribute("configargs", configargs);
                    seqTask.addTask(task);
                }
            }
            catch (IOException | IllegalArgumentException e)
            {
                throw new BuildException("Cannot configure " + className, e);
            }
        }
        seqTask.perform();
        if (null != project.getProperty("testfailed") && null == project.getProperty("ant.test.failure.ignore"))
            throw new BuildException("Some test(s) failed.");
    }
}
