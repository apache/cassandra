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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.utils.Hex;

public final class Resources
{
    /**
     * Construct a chain of resource parents starting with the resource and ending with the root.
     *
     * @param resource The staring point.
     * @return list of resource in the chain form start to the root.
     */
    public static List<? extends IResource> chain(IResource resource)
    {
        List<IResource> chain = new ArrayList<IResource>();
        while (true)
        {
           chain.add(resource);
           if (!resource.hasParent())
               break;
           resource = resource.getParent();
        }
        return chain;
    }

    @Deprecated
    public final static String ROOT = "cassandra";
    @Deprecated
    public final static String KEYSPACES = "keyspaces";

    @Deprecated
    public static String toString(List<Object> resource)
    {
        StringBuilder buff = new StringBuilder();
        for (Object component : resource)
        {
            buff.append("/");
            if (component instanceof byte[])
                buff.append(Hex.bytesToHex((byte[])component));
            else
                buff.append(component.toString());
        }
        return buff.toString();
    }
}
