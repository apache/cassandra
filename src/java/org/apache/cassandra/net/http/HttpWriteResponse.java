/**
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

/*
 * This class writes the HTTP 1.1 responses back to the client.
 */

package org.apache.cassandra.net.http;

import java.net.*;
import java.nio.ByteBuffer;
import java.io.*;

/**
 *
 * @author kranganathan
 */
public class HttpWriteResponse
{
    private HttpRequest httpRequest_ = null;
    private StringBuilder body_ = new StringBuilder();

    public HttpWriteResponse(HttpRequest httpRequest)
    {
        httpRequest_ = httpRequest;
    }

    public void println(String responseLine)
    {
        if(responseLine != null)
        {
            body_.append(responseLine);
            body_.append( System.getProperty("line.separator"));
        }
    }

    public ByteBuffer flush() throws Exception
    {
        StringBuilder sb = new StringBuilder();
        // write out the HTTP response headers first
        sb.append(httpRequest_.getVersion() + " 200 OK\r\n");
        sb.append("Content-Type: text/html\r\n");
        if(body_.length() > 0)
        	sb.append("Content-Length: " + body_.length() + "\r\n");
        sb.append("Cache-Control: no-cache\r\n");
        sb.append("Pragma: no-cache\r\n");

        // terminate the headers
        sb.append("\r\n");

        // now write out the HTTP response body
        if(body_.length() > 0)
            sb.append(body_.toString());

        // terminate the body
        //sb.append("\r\n");
        //sb.append("\r\n");
        ByteBuffer buffer = ByteBuffer.wrap(sb.toString().getBytes());
        return buffer;
    }
}
