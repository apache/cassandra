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

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * Data structure for holding a subjects private key and certificate chain.
 */
public class X509Credentials
{
    public PrivateKey privateKey;
    public X509Certificate[] chain;

    public X509Credentials(PrivateKey privateKey, X509Certificate[] chain)
    {
        this.privateKey = privateKey;
        this.chain = chain;
    }

    /**
     * Returns certificate issued to subject as represented by first certificate on head of chain.
     */
    public X509Certificate cert()
    {
        return chain.length > 0 ? chain[0] : null;
    }

    /**
     * Returns the last certificate in chain, which is supposed to be the CA certificate.
     */
    public X509Certificate ca()
    {
        return chain.length > 1 ? chain[chain.length-1] : null;
    }
}