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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.async.NettyHttpClient;
import org.apache.cassandra.net.async.NettyHttpClient.NettyHttpResponse;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Nexus for communicating with Vault. Requests will establish a HTTP connection based on Netty.
 * Returned JSON content is parsed and wrapped in a {@link CompletableFuture} of {@link VaultResponse}.
 */
public class VaultIO
{
    private final Logger logger = LoggerFactory.getLogger(VaultIO.class);

    public final static VaultIO instance = new VaultIO();

    private boolean initialized = false;
    private URI vaultAddress;
    private SslContext sslContext;

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    static
    {
        jsonMapper.disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    final static String URI_LOGIN = "/v1/auth/approle/login";

    public CompletableFuture<VaultResponse> get(String uri, @Nullable VaultAuthenticator auth)
    {
        return request(uri, null, auth);
    }

    public CompletableFuture<VaultResponse> post(String uri, ByteBuf payload, @Nullable VaultAuthenticator auth)
    {
        return request(uri, payload, auth);
    }

    private CompletableFuture<VaultResponse> request(String uri, @Nullable ByteBuf payload, @Nullable VaultAuthenticator auth)
    {
        URI requestUrl = vaultAddress.resolve(uri);
        Function<String, CompletableFuture<VaultResponse>> f;
        if (payload != null)
            f = (token) -> NettyHttpClient.httpPost(requestUrl,
                                                    payload,
                                                    "application/json",
                                                    token == null ? Collections.emptyMap() : ImmutableMap.of("X-Vault-Token", token),
                                                    sslContext)
                                          .thenApply(this::decodeVaultResponse);
        else
            f = (token) -> NettyHttpClient.httpGet(requestUrl,
                                                   token == null ? Collections.emptyMap() : ImmutableMap.of("X-Vault-Token", token),
                                                   sslContext)
                                          .thenApply(this::decodeVaultResponse);

        return auth != null ? auth.getClientToken().thenCompose(f) : f.apply(null);
    }

    private VaultResponse decodeVaultResponse(NettyHttpResponse response)
    {
        int code = response.getStatus().code();

        try
        {
            String content = response.getContent();
            if (content == null)
                throw new VaultIOException(response);

            VaultResponse ret = jsonMapper.readValue(content, VaultResponse.class);
            ret.setHttpResponse(response);
            if (ret.errors != null)
                for (String err : ret.errors)
                    logger.error(err);
            if (ret.warnings != null)
                for (String warn : ret.warnings)
                    logger.warn(warn);
            return ret;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (code >= 400)
                throw new VaultIOException(response);
        }
    }

    public synchronized void setup(boolean testing)
    {
        if (initialized)
            return;

        this.vaultAddress = DatabaseDescriptor.getVaultAddress();
        boolean useSsl = false;
        if (vaultAddress != null)
        {
            useSsl = vaultAddress.getScheme().equalsIgnoreCase("https");
        }
        else if (testing)
        {
            try
            {
                this.vaultAddress = new URI("http://localhost:8200");
            }
            catch (URISyntaxException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            throw new ConfigurationException("Missing vault_address value in cassandra.yaml");
        }

        File certFile = DatabaseDescriptor.getVaultCertificateFile();
        if (certFile == null)
        {
            if (useSsl)
                logger.info("Warning: no certificate provided for Vault TLS endpoint verification. Using platform default truststore.");
        }
        else
        {
            try
            {
                this.sslContext = SslContextBuilder.forClient().trustManager(certFile).build();
            }
            catch (SSLException e)
            {
                throw new ConfigurationException("Error creating netty SSLContext for Vault", e);
            }
        }

        initialized = true;
    }

    public static class VaultResponse
    {
        private NettyHttpResponse httpResponse;

        public VaultAuth auth;
        public boolean renewable;
        public long lease_duration;
        public String[] warnings;
        public String[] errors;
        public Map<String, Object> wrap_info;
        public Map<String, Object> data;
        public String lease_id;

        public NettyHttpResponse getHttpResponse()
        {
            return httpResponse;
        }

        public void setHttpResponse(NettyHttpResponse httpResponse)
        {
            this.httpResponse = httpResponse;
        }
    }

    public static class VaultAuth
    {
        public boolean renewable;
        public long lease_duration;
        public String[] policies;
        public Map<String, Object> metadata;
        public String accessor;
        public String client_token;
    }
}
