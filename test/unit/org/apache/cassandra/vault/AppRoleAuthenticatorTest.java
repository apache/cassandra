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

package org.apache.cassandra.vault;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;

public class AppRoleAuthenticatorTest
{
    @Test(expected = ConfigurationException.class)
    public void testInstanciationMissingParam()
    {
        new AppRoleAuthenticator(Collections.emptyMap());
    }

    @Test(expected = ConfigurationException.class)
    public void testInstanciationMissingFile()
    {
        new AppRoleAuthenticator(ImmutableMap.of("id_file_path", "doesnotexists"));
    }

    @Test(expected = ConfigurationException.class)
    public void testInstanciationMissingRoleId() throws IOException
    {
        File file = createTmpFile(null);
        new AppRoleAuthenticator(ImmutableMap.of("id_file_path", file.getPath()));
    }

    @Test
    public void testPayload() throws IOException
    {
        File file = createTmpFile("role_id: e3145e07-f8ed-4697-d0ca-a1c93d5bc4f1");
        AppRoleAuthenticator authenticator = new AppRoleAuthenticator(ImmutableMap.of("id_file_path", file.getPath()));
        ByteBuf buf = authenticator.createPayload();
        String payload = buf.toString(StandardCharsets.US_ASCII);
        assertEquals(payload, "{\"role_id\": \"e3145e07-f8ed-4697-d0ca-a1c93d5bc4f1\"}");
    }

    @Test
    public void testLogin() throws IOException, ExecutionException, InterruptedException
    {
        SchemaLoader.prepareServer();
        VaultIO.instance.setup(true);
        try(LocalHttpTestServer ignored = new LocalHttpTestServer())
        {
            File file = createTmpFile("role_id: e3145e07-f8ed-4697-d0ca-a1c93d5bc4f1");
            AppRoleAuthenticator authenticator = new AppRoleAuthenticator(ImmutableMap.of("id_file_path", file.getPath()));
            CompletableFuture<String> token = authenticator.getClientToken();
            // see login.json for result
            assertEquals("5b1a0318-679c-9c45-e5c6-d1b9a9035d49", token.get());
        }
    }

    private File createTmpFile(String content) throws IOException
    {
        File file = File.createTempFile(AppRoleAuthenticatorTest.class.getSimpleName(), ".properties");
        file.deleteOnExit();
        if (content != null)
            Files.write(file.toPath(), content.getBytes());
        return file;
    }
}