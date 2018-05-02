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
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.async.NettyHttpClient;

import static org.apache.cassandra.vault.LocalHttpTestServer.FIXTURES_DIR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VaultIOTest
{
    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        VaultIO.instance.setup(true);
    }

    @Test
    public void testHttpServerUnavailable() throws InterruptedException
    {
        CompletableFuture<VaultIO.VaultResponse> resp = VaultIO.instance.get(VaultIO.URI_LOGIN, null);
        try
        {
            resp.get();
            fail("Exception expected");
        }
        catch (ExecutionException e)
        {
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void testHttpGet() throws InterruptedException, ExecutionException, TimeoutException, IOException
    {
        try(LocalHttpTestServer ignored = new LocalHttpTestServer())
        {
            CompletableFuture<VaultIO.VaultResponse> fresp = VaultIO.instance.get(VaultIO.URI_LOGIN, null);
            assertResponse(fresp.get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testHttpPost() throws InterruptedException, ExecutionException, TimeoutException, IOException
    {
        try(LocalHttpTestServer ignored = new LocalHttpTestServer())
        {
            ByteBuf payload = Unpooled.buffer();
            payload.writeBytes("test".getBytes());
            CompletableFuture<VaultIO.VaultResponse> fresp = VaultIO.instance.post(VaultIO.URI_LOGIN, payload, null);
            assertResponse(fresp.get(1, TimeUnit.SECONDS));
        }
    }

    private void assertResponse(VaultIO.VaultResponse vaultResponse) throws IOException
    {
        NettyHttpClient.NettyHttpResponse httpResponse = vaultResponse.getHttpResponse();
        assertEquals(httpResponse.getStatus(), HttpResponseStatus.OK);
        byte[] content = Files.readAllBytes(new File(FIXTURES_DIR, "vault/login.json").toPath());
        assertEquals(httpResponse.getContent(), new String(content, CharsetUtil.UTF_8));

        assertNull(vaultResponse.warnings);
        assertNull(vaultResponse.errors);
        assertNull(vaultResponse.data);
        assertNull(vaultResponse.wrap_info);
        assertEquals("", vaultResponse.lease_id);
        assertFalse(vaultResponse.renewable);
        assertEquals(0, vaultResponse.lease_duration);
        assertNotNull(vaultResponse.auth);
        assertTrue(vaultResponse.auth.renewable);
        assertEquals(1200, vaultResponse.auth.lease_duration);
        assertNull(vaultResponse.auth.metadata);
        assertArrayEquals(new String[] {"default"}, vaultResponse.auth.policies);
        assertEquals("fd6c9a00-d2dc-3b11-0be5-af7ae0e1d374", vaultResponse.auth.accessor);
        assertEquals("5b1a0318-679c-9c45-e5c6-d1b9a9035d49", vaultResponse.auth.client_token);
    }
}