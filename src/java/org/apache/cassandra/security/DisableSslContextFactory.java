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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

public class DisableSslContextFactory extends AbstractSslContextFactory
{
    @Override
    protected KeyManagerFactory buildKeyManagerFactory() throws SSLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected TrustManagerFactory buildTrustManagerFactory() throws SSLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected KeyManagerFactory buildOutboundKeyManagerFactory() throws SSLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasKeystore()
    {
        return false;
    }

    @Override
    public boolean hasOutboundKeystore()
    {
        return false;
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
}
