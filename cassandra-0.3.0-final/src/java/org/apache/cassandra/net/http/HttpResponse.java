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
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.apache.cassandra.net.http;

import java.util.*;
import java.io.*;
import java.nio.*;

/**
 *
 * @author kranganathan
 */
public class HttpResponse
{
    private Map<String, String> headersMap_ = new HashMap<String, String>();;
    private String sBody_ = null;
    private String method_ = null;
    private String path_ = null;
    private String version_ = null;


    public String getMethod()
    {
        return method_;
    }
    
    public String getPath()
    {
        return path_;
    }
    
    public String getVersion()
    {
        return "HTTP/1.1";
    }

    public void addHeader(String name, String value)
    {
        headersMap_.put(name, value);
    }

    public String getHeader(String name)
    {
        return headersMap_.get(name).toString();
    }

    public void setBody(List<ByteBuffer> bodyBuffers)
    {
        StringBuffer sb = new StringBuffer();
        while(bodyBuffers.size() > 0)
        {
            sb.append(bodyBuffers.remove(0).asCharBuffer().toString());
        }
        sBody_ = sb.toString();
    }
    
    public String getBody()
    {
        return sBody_;
    }
    
    public void setStartLine(String method, String path, String version)
    {
        method_ = method;
        path_ = path;
        version_ = version;
    }    

    public String toString()
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        
        pw.println("HttpResponse-------->");
        pw.println("method = " + method_ + ", path = " + path_ + ", version = " + version_);
        pw.println("Headers: " + headersMap_.toString());
        pw.println("Body: " + sBody_);
        pw.println("<--------HttpResponse");
        
        return sw.toString();
    }
}
