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

import org.apache.cassandra.net.async.NettyHttpClient;

/**
 * Indicator for Vault <a href="https://www.vaultproject.io/api/index.html#http-status-codes">status codes</a>.
 */
public class VaultIOException extends RuntimeException
{
    private final int code;
    private final boolean recoverable;

    public VaultIOException(NettyHttpClient.NettyHttpResponse response)
    {
        this.code = response.getStatus().code();
        this.recoverable = code >= 400 && code < 500;
        response.getContent();
    }

    public int getCode()
    {
        return code;
    }

    public boolean isRecoverable()
    {
        return recoverable;
    }

    @Override
    public String toString()
    {
        switch (this.code)
        {
            case 400: return "Invalid request, missing or invalid data.";
            case 403: return "Forbidden, your authentication details are either incorrect, you don't have access to this feature, or - if CORS is enabled - you made a cross-origin request from an origin that is not allowed to make such requests.";
            case 404: return "Invalid path. This can both mean that the path truly doesn't exist or that you don't have permission to view a specific path. We use 404 in some cases to avoid state leakage.";
            case 429: return "Default return code for health status of standby nodes, indicating a warning.";
            case 500: return "Internal server error. An internal error has occurred, try again later. If the error persists, report a bug.";
            case 503: return "Vault is down for maintenance or is currently sealed. Try again later.";
            default: return "Vault HTTP error";
        }
    }
}
