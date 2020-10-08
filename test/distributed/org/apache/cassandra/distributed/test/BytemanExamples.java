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

package org.apache.cassandra.distributed.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.shared.Byteman;

public class BytemanExamples
{
    @Test
    public void rewriteFromText() throws IOException
    {
        Byteman byteman = Byteman.createFromText("RULE example\n" +
                                                 "CLASS org.apache.cassandra.utils.FBUtilities\n" +
                                                 "METHOD setBroadcastInetAddress\n" +
                                                 "IF true\n" +
                                                 "DO\n" +
                                                 "   throw new java.lang.RuntimeException(\"Will not allow init!\")\n" +
                                                 "ENDRULE\n");

        try
        {
            Cluster.build(1)
                   .withInstanceInitializer((cl, ignore) -> byteman.install(cl))
                   .createWithoutStarting();
            Assert.fail("Instance init was rewritten to fail right away, so make sure the rewrite happens");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals("Will not allow init!", e.getMessage());
        }
    }

    @Test
    public void rewriteFromScript() throws IOException
    {
        // scripts are normally located at test/resources/byteman, but generated in the test
        // for documentation purposes only
        File script = File.createTempFile("byteman", ".btm");
        script.deleteOnExit();
        Files.asCharSink(script, StandardCharsets.UTF_8).write("RULE example\n" +
                                                               "CLASS org.apache.cassandra.utils.FBUtilities\n" +
                                                               "METHOD setBroadcastInetAddress\n" +
                                                               "IF true\n" +
                                                               "DO\n" +
                                                               "   throw new java.lang.RuntimeException(\"Will not allow init!\")\n" +
                                                               "ENDRULE\n");

        Byteman byteman = Byteman.createFromScripts(script.getAbsolutePath());

        try
        {
            Cluster.build(1)
                   .withInstanceInitializer((cl, ignore) -> byteman.install(cl))
                   .createWithoutStarting();
            Assert.fail("Instance init was rewritten to fail right away, so make sure the rewrite happens");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals("Will not allow init!", e.getMessage());
        }
    }
}
