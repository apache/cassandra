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
 * Encapsulates a HTTP request.
 */

package org.apache.cassandra.net.http;

import java.util.*;
import java.io.*;
import java.net.URLDecoder;
import java.nio.*;

/**
 *
 * @author kranganathan
 */
public class HttpRequest
{
    private Map<String, String> headersMap_ = new HashMap<String, String>();
    private Map<String, String> paramsMap_ = new HashMap<String, String>();
    private String sBody_ = "";
    private String method_;
    private String path_;
    private String query_;
    private String version_;

    /*
     * Returns the type of method - GET, POST, etc.
    */
    public String getMethod()
    {
        return method_;
    }

    /*
     * Gets the request path referenced by the request.
     * For example, if the URL is of the form:
     *  http://somedomain:PORT/some/path?param=value
     * this function will return
     *  "/some/path"
     */
    public String getPath()
    {
        return path_;
    }

    /*
     * Gets the query in the request.
     * For example, if the URL is of the form:
     *  http://somedomain:PORT/some/path?param=value
     * this function will return
     *  "/some/path"
     */
    public String getQuery()
    {
        return query_;
    }

    /*
     * Returns the supported HTTP protocol version.
     */
    public String getVersion()
    {
        return "HTTP/1.1";
    }

    /*
     * This function add to the map of header name-values
     * in the HTTP request.
     */
    public void addHeader(String name, String value)
    {
        headersMap_.put(name, value);
    }

    /*
     * For a gives name, returns the value if it was in the
     * headers. Returns the empty string otherwise.
     */
    public String getHeader(String name)
    {
        if(headersMap_.get(name) == null)
            return "";
        return headersMap_.get(name).toString();
    }

    public void setParameter(String name, String value)
    {
    	// first decode the data then store it in the map using the standard UTF-8 encoding
    	String decodedValue = value;
    	try
    	{
    		decodedValue = URLDecoder.decode(value, "UTF-8");
    	}
    	catch (Exception e)
    	{
    		// do nothing
    	}
        paramsMap_.put(name, decodedValue);
    }

    /*
     * This function get the parameters from the body of the HTTP message.
     * Returns the value for the parameter if one exists or returns null.
     *
z    * For example, if the body is of the form:
     *  a=b&c=d
     * this function will:
     * return "b" when called as getParameter("a")
     */
    public String getParameter(String name)
    {
        if(paramsMap_.get(name) != null)
            return paramsMap_.get(name).toString();
        return null;
    }

    /*
     * Get the string representation of the byte buffers passed in and put
     * them in sBody_ variable. Then parse all the parameters in the body.
     */
    public void setBody(List<ByteBuffer> bodyBuffers)
    {
        if(bodyBuffers == null)
            return;
        try
        {
            // get the byte buffers that the body should be composed of
            // and collect them in the string builder
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < bodyBuffers.size(); ++i)
            {
                ByteBuffer bb = bodyBuffers.get(i);
                if(bb.remaining() <= 0)
                {
                    continue;
                }
                byte[] byteStr = new byte[bb.remaining()];
                bb.get(byteStr);
                String s = new String(byteStr);
                sb.append(s);
            }

            // add the string to the body
            if(sb.toString() != null)
            {
                sBody_ += sb.toString();
            }

            // once we are done with the body, parse the parameters
            String[] sParamValues = sBody_.split("&");
            for(int i = 0; i < sParamValues.length; ++i)
            {
                String[] paramVal = sParamValues[i].split("=");
                if ( paramVal[0] != null && paramVal[1] != null )
                {
                    setParameter(paramVal[0], paramVal[1]);
                }
            }
        }
        catch(Exception e)
        {
        }
    }

    public String getBody()
    {
        return sBody_;
    }

    public void setStartLine(String method, String path, String query, String version)
    {
        method_ = method;
        path_ = path;
        query_ = query;
        version_ = version;
    }

    public String toString()
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("HttpRequest-------->");
        pw.println("method = " + method_ + ", path = " + path_ + ", query = " + query_ + ", version = " + version_);
        pw.println("Headers: " + headersMap_.toString());
        pw.println("Body: " + sBody_);
        pw.println("<--------HttpRequest");

        return sw.toString();
    }
}

