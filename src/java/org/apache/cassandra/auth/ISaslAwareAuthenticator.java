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
package org.apache.cassandra.auth;

import org.apache.cassandra.exceptions.AuthenticationException;

public interface ISaslAwareAuthenticator extends IAuthenticator
{
    /**
     * Provide a SaslAuthenticator to be used by the CQL binary protocol server. If
     * the configured IAuthenticator requires authentication but does not implement this
     * interface we refuse to start the binary protocol server as it will have no way
     * of authenticating clients.
     * @return SaslAuthenticator implementation
     * (see {@link PasswordAuthenticator.PlainTextSaslAuthenticator})
     */
    SaslAuthenticator newAuthenticator();


    public interface SaslAuthenticator
    {
        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException;
        public boolean isComplete();
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException;
    }
}
