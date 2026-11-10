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

import org.apache.commons.io.FilenameUtils;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

import org.apache.cassandra.io.util.File;

/**
 * Sets the eclipse-libs-list property to the library entries of the Eclipse .classpath file, built from the jars in
 * the eclipse-project-libs property. An entry has the sources of a jar attached when they were retrieved.
 */
public class EchoEclipseProjectLibs extends Task
{
    public void execute()
    {
        getProject().setNewProperty("eclipse-libs-list", buildMsg().toString());
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
            // the sources of the jars in a "jars" directory are in the "sources" directory next to it
            String jarDir = FilenameUtils.getFullPathNoEndSeparator(jars[i]);
            String srcDir = FilenameUtils.concat(FilenameUtils.getFullPath(jarDir), "sources");
            File srcFile = new File(FilenameUtils.concat(srcDir, srcJar));

            if (i > 0)
                msg.append('\n');
            msg.append("  <classpathentry kind=\"lib\" path=\"").append(jars[i]).append('"');
            if (srcFile.exists())
                msg.append(" sourcepath=\"").append(srcFile.path()).append('"');
            msg.append("/>");
        }

        return msg;
    }
}
