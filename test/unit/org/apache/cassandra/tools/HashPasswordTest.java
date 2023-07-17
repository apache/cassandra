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
package org.apache.cassandra.tools;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.mindrot.jbcrypt.BCrypt;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class HashPasswordTest extends CQLTester
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String plaintextPassword = "foobar";
    private static final String hashPasswordTool = "tools/bin/hash_password";

    /* If help changed you also need to change the docs*/
    @Test
    public void testHelpAndShouldChangeDocs()
    {
        ToolResult tool = ToolRunner.invoke(hashPasswordTool, "-h");
        tool.assertOnCleanExit();
        String help = "usage: hash_password [options]\n" +
                       "--\n" +
                       "Hashes a plain text password and prints the hashed password.\n" +
                       "Options are:\n" +
                       " -e,--environment-var <arg>   Use value of the specified environment\n" +
                       "                              variable as the password\n" +
                       " -h,--help                    Display this help message\n" +
                       " -i,--input <arg>             Input is a file (or - for stdin) to read the\n" +
                       "                              password from. Make sure that the whole\n" +
                       "                              input including newlines is considered. For\n" +
                       "                              example, the shell command 'echo -n foobar |\n" +
                       "                              hash_password -i -' will work as intended\n" +
                       "                              and just hash 'foobar'.\n" +
                       " -p,--plain <arg>             Argument is the plain text password\n" +
                       " -r,--logrounds <arg>         Number of hash rounds (default: 10).\n";
        assertEquals(help, tool.getStdout());
    }

    @Test
    public void testPlain()
    {
        ToolResult tool = ToolRunner.invoke(hashPasswordTool, "--plain", plaintextPassword);
        tool.assertOnCleanExit();
        String hashed = tool.getStdout();
        assertTrue("Hashed password does not validate: " + hashed, BCrypt.checkpw(plaintextPassword, hashed));
    }

    @Test
    public void testStdIn()
    {
        ToolResult tool = ToolRunner.invoke(Collections.emptyMap(),
                                            new ByteArrayInputStream(plaintextPassword.getBytes()),
                                            Arrays.asList(hashPasswordTool, "--input", "-"));
        tool.assertOnCleanExit();
        String hashed = tool.getStdout();
        assertTrue("Hashed password does not validate: " + hashed, BCrypt.checkpw(plaintextPassword, hashed));
    }

    @Test
    public void testFile() throws IOException
    {
        File file = temporaryFolder.newFile();
        Files.write(file.toPath(), plaintextPassword.getBytes());

        ToolResult tool = ToolRunner.invoke(hashPasswordTool, "--input", file.getAbsolutePath());
        tool.assertOnCleanExit();
        String hashed = tool.getStdout();
        assertTrue("Hashed password does not validate: " + hashed, BCrypt.checkpw(plaintextPassword, hashed));
    }

    @Test
    public void testEnvVar()
    {
        ToolResult tool = ToolRunner.invoke(Collections.singletonMap("THE_PASSWORD", plaintextPassword),
                                            null,
                                            Arrays.asList(hashPasswordTool, "--environment-var", "THE_PASSWORD"));
        tool.assertOnCleanExit();
        String hashed = tool.getStdout();
        assertTrue("Hashed password does not validate: " + hashed, BCrypt.checkpw(plaintextPassword, hashed));
    }

    @Test
    public void testLogRounds()
    {
        ToolResult tool = ToolRunner.invoke(hashPasswordTool, "--plain", plaintextPassword, "-r", "10");
        tool.assertOnCleanExit();
        String hashed = tool.getStdout();
        assertTrue("Hashed password does not validate: " + hashed, BCrypt.checkpw(plaintextPassword, hashed));
    }

    @Test
    public void testShortPass()
    {
        ToolResult tool = ToolRunner.invoke(hashPasswordTool, "--plain", "A");
        tool.assertOnExitCode();
        assertThat(tool.getStderr(), containsString("password is very short"));
    }

    @Test
    public void testErrorMessages()
    {
        assertToolError("One of the options --environment-var, --plain or --input must be used.", hashPasswordTool);
        assertToolError("Environment variable 'non_existing_environment_variable_name' is undefined.",
                        hashPasswordTool,
                        "--environment-var",
                        "non_existing_environment_variable_name");
        assertToolError("Failed to read from '/foo/bar/baz/blah/yadda': ",
                        hashPasswordTool,
                        "--input",
                        "/foo/bar/baz/blah/yadda");
    }

    private static void assertToolError(String expectedMessage, String... args)
    {
        ToolResult tool = ToolRunner.invoke(args);
        assertEquals(1, tool.getExitCode());
        assertThat(tool.getStderr(), containsString(expectedMessage));
    }
}
