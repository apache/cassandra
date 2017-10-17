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
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Implementation of {@link VaultAuthenticator} based on Vault
 * <a href="https://www.vaultproject.io/docs/auth/approle.html">AppRole</a> backend. A separate config file with
 * role_id and secret_id values must be provided as id_file_path parameter.
 */
public class AppRoleAuthenticator implements VaultAuthenticator
{
    private static final Logger logger = LoggerFactory.getLogger(AppRoleAuthenticator.class);

    private final File propertiesFile;
    private final Pattern tokenPattern = Pattern.compile("[a-zA-Z0-9\\-]+");

    public AppRoleAuthenticator(Map<String, String> params)
    {
        String propertiesPath = params.get("id_file_path");
        if (propertiesPath == null)
            throw new ConfigurationException("Missing id_file_path value");
        propertiesFile = new File(propertiesPath);
        if (!propertiesFile.isFile() || !propertiesFile.canRead())
            throw new ConfigurationException("Not a readable file: " + propertiesPath);
        try
        {
            if (!properties().containsKey("role_id"))
                throw new ConfigurationException("Missing role_id value in " + propertiesPath);
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Invalid id_file_path file", e);
        }
    }

    @Override
    public CompletableFuture<String> getClientToken()
    {
        try
        {
            logger.info("Authenticating using AppRole");
            CompletableFuture<VaultIO.VaultResponse> loginResult = VaultIO.instance.post(VaultIO.URI_LOGIN, createPayload(), null);
            return loginResult.handle((response, t) ->
                                      {
                                          if (t != null)
                                          {
                                              logger.error("Failed to authenticate", t);
                                              throw new RuntimeException(t);
                                          }
                                          else if (response != null && response.auth != null)
                                          {
                                            logger.debug("Authentication successful (token renewable: {},  lease duration: {}, policies: {}", response.auth.renewable, response.auth.lease_duration, Arrays.toString(response.auth.policies));
                                            return response.auth.client_token;
                                          }
                                          throw new RuntimeException("Missing authentication response");
                                      });
        }
        catch (IOException e)
        {
            CompletableFuture<String> ret = new CompletableFuture<>();
            ret.completeExceptionally(e);
            return ret;
        }
    }

    @VisibleForTesting
    ByteBuf createPayload() throws IOException
    {
        // Re-read IDs every time as authentication should rarely take place and we don't want to
        // expose secrets in any heap inspections. Secret_id values may also get updated dynamically through a
        // configuration management solution.
        Properties props = properties();
        StringBuilder builder = new StringBuilder();
        builder.append("{\"role_id\": \"");
        builder.append(checkSafety(props.getProperty("role_id")));
        builder.append('"');
        if (props.containsKey("secret_id"))
        {
            builder.append(", \"secret_id\": \"");
            builder.append(checkSafety(props.getProperty("secret_id")));
            builder.append('"');
        }
        builder.append('}');

        byte[] bytes = builder.toString().getBytes(StandardCharsets.US_ASCII);
        return Unpooled.wrappedBuffer(bytes);
    }

    /* Try not to be an attack vector on Vault */
    private String checkSafety(String id) throws IOException
    {
        if (!tokenPattern.matcher(id).matches())
            throw new IOException("Invalid format for id");
        return id;
    }

    private Properties properties() throws IOException
    {
        Properties props = new Properties();
        try (Reader reader = Files.newBufferedReader(propertiesFile.toPath()))
        {
            props.load(reader);
        }
        return props;
    }
}