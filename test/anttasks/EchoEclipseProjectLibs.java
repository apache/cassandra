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

import org.apache.cassandra.io.util.File;

import org.apache.commons.io.FilenameUtils;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.taskdefs.Echo;

public class EchoEclipseProjectLibs extends Task
{
    public void execute()
    {
        Project project = getProject();
        StringBuilder msg = buildMsg();

        Echo echo = (Echo) project.createTask("echo");
        echo.setMessage(msg.toString());
        echo.setFile(new File(".classpath").toJavaIOFile());
        echo.setAppend(true);
        echo.perform();
    }

    public StringBuilder buildMsg()
    {
        Project project = getProject();
        String[] jars = project.getProperty("eclipse-project-libs")
                               .split(project.getProperty("path.separator"));

        StringBuilder msg = new StringBuilder();
        for (int i=0; i< jars.length; i++)
        {
            String srcJar = FilenameUtils.getBaseName(jars[i]) + "-sources.jar";
            String srcDir = FilenameUtils.concat(project.getProperty("build.test.dir"), "sources");
            File srcFile = new File(FilenameUtils.concat(srcDir, srcJar));

            msg.append("\" <classpathentry kind=\"lib\" path=\"").append(jars[i]).append('"');
            if (srcFile.exists())
                msg.append(" sourcepath=\"").append(srcFile.path()).append('"');
            msg.append("/>\n");
        }

        msg.append("</classpath>");

        return msg;
    }
}
