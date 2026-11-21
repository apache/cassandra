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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.ProjectHelper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.TestConfigurations;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestConfigurationTest
{
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @TestConfigurations({"default", "latest", "bti"})
    public static class Annotated
    {
        static
        {
            if (true)
                throw new AssertionError("Discovery must not initialize tests");
        }
    }

    @TestConfigurations({"default"})
    public static class Default
    {
    }

    @TestConfigurations({})
    public static class Empty
    {
    }

    @TestConfigurations({"default", "default"})
    public static class Duplicate
    {
    }

    @Test
    public void readsBytecodeWithoutInitialization() throws Exception
    {
        try (InputStream in = Annotated.class.getResourceAsStream("TestConfigurationTest$Annotated.class"))
        {
            assertEquals(Arrays.asList("default", "latest", "bti"), TestConfiguration.declaredConfigurations(in));
        }
        try (InputStream in = getClass().getResourceAsStream("TestConfigurationTest.class"))
        {
            assertTrue(TestConfiguration.declaredConfigurations(in).isEmpty());
        }
    }

    @Test
    public void rejectsEmptyAndDuplicateConfigurations() throws Exception
    {
        for (String name : Arrays.asList("Empty", "Duplicate"))
        {
            try (InputStream in = getClass().getResourceAsStream("TestConfigurationTest$" + name + ".class"))
            {
                assertThatThrownBy(() -> TestConfiguration.declaredConfigurations(in)).isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    @Test
    public void schedulesConfigurationsAndPreservesUnannotatedJvmArguments() throws Exception
    {
        List<String> annotated = schedule(Annotated.class, null);
        assertEquals(3, annotated.size());
        assertTrue(annotated.get(0).startsWith("default|-Dcassandra.config=file:"));
        assertTrue(annotated.get(1).startsWith("latest|-Dcassandra.config=file:"));
        assertTrue(annotated.get(2).startsWith("bti|-Dcassandra.config=file:"));
        assertEquals(Collections.singletonList("|"), schedule(TestConfigurationTest.class, null));
        assertTrue(schedule(Default.class, null).get(0).startsWith("default|-Dcassandra.config=file:"));
        List<String> override = schedule(Annotated.class, "bti");
        assertEquals(1, override.size());
        assertTrue(override.get(0).startsWith("bti|-Dcassandra.config=file:"));
    }

    private List<String> schedule(Class<?> test, String override) throws Exception
    {
        Path root = temporary.newFolder().toPath();
        Path output = root.resolve("calls.txt");
        Path build = root.resolve("build.xml");
        Files.writeString(build,
                          "<project><macrodef name='testlist-configured'>" +
                          "<attribute name='test.class.name'/><attribute name='configuration'/><attribute name='configargs'/>" +
                          "<sequential><echo file='calls.txt' append='true' message='@{configuration}|@{configargs}${line.separator}'/>" +
                          "</sequential></macrodef></project>");
        Files.writeString(root.resolve("cassandra.yaml"), "base: true\n");
        Files.writeString(root.resolve("latest_diff.yaml"), "latest: true\n");
        Files.writeString(root.resolve("storage_compatibility_mode_none.yaml"), "compatibility: NONE\n");
        Files.createDirectory(root.resolve("configurations"));
        Files.writeString(root.resolve("configurations/bti.yaml"), "bti: true\n");

        Project project = new Project();
        project.init();
        ProjectHelper.configureProject(project, build.toFile());
        project.setProperty("test.configured.class", test.getName());
        project.setProperty("test.classes", Path.of(getClass().getProtectionDomain().getCodeSource().getLocation().toURI()).toString());
        project.setProperty("test.dir", root.toString());
        project.setProperty("test.classlistprefix", ".");
        project.setProperty("test.conf", root.toString());
        project.setProperty("build.test.dir", root.resolve("output").toString());
        if (null != override)
            project.setProperty("test.configuration", override);
        TestHelper helper = new TestHelper();
        helper.setProject(project);
        helper.setProperty("testlist");
        helper.execute();
        return Files.readAllLines(output);
    }

    @Test
    public void propagatesTestFailuresUnlessExplicitlyIgnored()
    {
        Project project = new Project();
        project.init();
        project.setProperty("path.separator", java.io.File.pathSeparator);
        project.setProperty("all-test-classes", "");
        project.setProperty("testfailed", "true");
        TestHelper helper = new TestHelper();
        helper.setProject(project);
        helper.setProperty("testlist");
        assertThatThrownBy(helper::execute).isInstanceOf(BuildException.class);
        project.setProperty("ant.test.failure.ignore", "true");
        helper.execute();
    }

    @Test
    public void buildsIndependentOverlaysAndRejectsUnknownNames() throws Exception
    {
        Path conf = temporary.newFolder("conf").toPath();
        Path output = temporary.newFolder("output").toPath();
        Files.writeString(conf.resolve("cassandra.yaml"), "base: true\n");
        Files.writeString(conf.resolve("latest_diff.yaml"), "latest: true\n");
        Files.writeString(conf.resolve("storage_compatibility_mode_none.yaml"), "compatibility: NONE\n");
        Files.createDirectory(conf.resolve("configurations"));
        Files.writeString(conf.resolve("configurations/custom.yaml"), "custom: true\n");

        assertEquals(conf.resolve("cassandra.yaml"), TestConfiguration.yaml(conf, output, "default"));
        String latest = Files.readString(TestConfiguration.yaml(conf, output, "latest"));
        assertTrue(latest.contains("base: true"));
        assertTrue(latest.indexOf("latest: true") < latest.indexOf("compatibility: NONE"));
        String custom = Files.readString(TestConfiguration.yaml(conf, output, "custom"));
        assertTrue(custom.contains("custom: true"));
        assertFalse(custom.contains("latest: true"));
        assertEquals("base: true\n", Files.readString(conf.resolve("cassandra.yaml")));
        assertThatThrownBy(() -> TestConfiguration.yaml(conf, output, "missing")).isInstanceOf(java.io.IOException.class);
        assertThatThrownBy(() -> TestConfiguration.yaml(conf, output, "../cassandra")).isInstanceOf(IllegalArgumentException.class);
    }
}
