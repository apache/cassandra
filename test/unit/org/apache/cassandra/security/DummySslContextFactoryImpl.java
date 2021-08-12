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

package org.apache.cassandra.security;

import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.SslContext;

/**
 * TEST ONLY Class. DON'T use it for anything else.
 */
public class DummySslContextFactoryImpl implements ISslContextFactory
{
    public DummySslContextFactoryImpl(Map<String,Object> parameters) {}

    @Override
    public SSLContext createJSSESslContext(boolean buildTruststore) throws SSLException
    {
        return null;
    }

    @Override
    public SslContext createNettySslContext(boolean buildTruststore, SocketType socketType, boolean useOpenSsl,
                                            CipherSuiteFilter cipherFilter) throws SSLException
    {
        return null;
    }

    @Override
    public void initHotReloading() throws SSLException
    {

    }

    @Override
    public boolean shouldReload()
    {
        return false;
    }

    @Override
    public List<String> getAcceptedProtocols()
    {
        return null;
    }

    @Override
    public List<String> getCipherSuites()
    {
        return null;
    }
}
