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

import java.util.concurrent.CompletableFuture;

/**
 * Implementations of the interface are able to generate new {@link X509Credentials} on request. It is expected that
 * this will be based on either external APIs or processes. It's also expected that these calls are expensive and long
 * lived, therefor the result will be represented as a future.
 *
 * Currently it is not possible to generate certificates using a CSR based workflow. But this functionality may be added
 * in the future, e.g. by defining and additional interface for that.
 */
public interface CertificateIssuer
{
    /**
     * Requests creation of new credentials.
     */
    public CompletableFuture<X509Credentials> generateCredentials();

    /**
     * Indicates whether credentials obtained through this issuer may be stored in a local keystore.
     */
    public boolean useKeyStore();

    /**
     * Number of days in advanced to start trying to renew a certificate before it expires.
     */
    public int renewDaysBeforeExpire();

    /**
     * Indicates if generated credentials can be shared to the same purpose as if they would have been generated
     * using the provided issuer. This will be the case if both this and the other issuer share identical settings
     * and would thus also create identical credentials, even when called separately.
     */
    public boolean shareClientAndServer(CertificateIssuer issuer);
}