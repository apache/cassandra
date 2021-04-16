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
package org.apache.cassandra.utils;

import javax.management.ObjectName;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.MX4JADDRESS;
import static org.apache.cassandra.config.CassandraRelevantProperties.MX4JPORT;

/**
 * If mx4j-tools is in the classpath call maybeLoad to load the HTTP interface of mx4j.
 *
 * The default port is 8081. To override that provide e.g. -Dmx4jport=8082
 * The default listen address is the broadcast_address. To override that provide -Dmx4jaddress=127.0.0.1
 */
public class Mx4jTool
{
    private static final Logger logger = LoggerFactory.getLogger(Mx4jTool.class);

    /**
     * Starts a JMX over http interface if and mx4j-tools.jar is in the classpath.
     * @return true if successfully loaded.
     */
    public static boolean maybeLoad()
    {
        try
        {
            logger.trace("Will try to load mx4j now, if it's in the classpath");
            MBeanWrapper mbs = MBeanWrapper.instance;
            ObjectName processorName = new ObjectName("Server:name=XSLTProcessor");

            Class<?> httpAdaptorClass = Class.forName("mx4j.tools.adaptor.http.HttpAdaptor");
            Object httpAdaptor = httpAdaptorClass.newInstance();
            httpAdaptorClass.getMethod("setHost", String.class).invoke(httpAdaptor, getAddress());
            httpAdaptorClass.getMethod("setPort", Integer.TYPE).invoke(httpAdaptor, getPort());

            ObjectName httpName = new ObjectName("system:name=http");
            mbs.registerMBean(httpAdaptor, httpName);

            Class<?> xsltProcessorClass = Class.forName("mx4j.tools.adaptor.http.XSLTProcessor");
            Object xsltProcessor = xsltProcessorClass.newInstance();
            httpAdaptorClass.getMethod("setProcessor", Class.forName("mx4j.tools.adaptor.http.ProcessorMBean")).
                    invoke(httpAdaptor, xsltProcessor);
            mbs.registerMBean(xsltProcessor, processorName);
            httpAdaptorClass.getMethod("start").invoke(httpAdaptor);
            logger.info("mx4j successfuly loaded");
            return true;
        }
        catch (ClassNotFoundException e)
        {
            logger.trace("Will not load MX4J, mx4j-tools.jar is not in the classpath");
        }
        catch(Exception e)
        {
            logger.warn("Could not start register mbean in JMX", e);
        }
        return false;
    }

    private static String getAddress()
    {
        String sAddress = MX4JADDRESS.getString();
        if (StringUtils.isEmpty(sAddress))
            sAddress = FBUtilities.getBroadcastAddressAndPort().getAddress().getHostAddress();
        return sAddress;
    }

    private static int getPort()
    {
        int port = 8081;
        String sPort = MX4JPORT.getString();
        if (StringUtils.isNotEmpty(sPort))
            port = Integer.parseInt(sPort);
        return port;
    }
}
