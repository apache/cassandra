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
package org.apache.cassandra.jmx.rmi;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.RuntimeMBeanException;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerProvider;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;

import org.apache.cassandra.utils.JMXServerUtils;

/**
 * Replicates the same logic as {@link com.sun.jmx.remote.protocol.rmi.ServerProvider} but overrides the {@link MBeanServer}
 * by calling {@link JMXServerUtils#fixException(RuntimeMBeanException)} to avoid leaking non java/javax exception types.
 * <p>
 * To enable, add -Djmx.remote.protocol.provider.pkgs=org.apache.cassandra.jmx to system properties.
 */
public class ServerProvider implements JMXConnectorServerProvider
{
    @Override
    public JMXConnectorServer newJMXConnectorServer(JMXServiceURL serviceURL, Map<String, ?> environment, MBeanServer mbeanServer) throws IOException
    {
        // have to copy/paste com.sun.jmx.remote.protocol.rmi.ServerProvider.newJMXConnectorServer due to not
        // being able to call com.sun.jmx classes due to java 9 modules
        if (!serviceURL.getProtocol().equals("rmi"))
        {
            throw new MalformedURLException("Protocol not rmi: " +
                                            serviceURL.getProtocol());
        }
        return new RMIConnectorServer(serviceURL, environment, JMXServerUtils.fixExceptions(mbeanServer));
    }
}
