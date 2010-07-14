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

package org.apache.cassandra.config;

import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndPointSnitch;
import org.apache.cassandra.utils.FBUtilities;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public final class KSMetaData
{
    public final String name;
    public final Class<? extends AbstractReplicationStrategy> repStratClass;
    public final int replicationFactor;
    public final IEndPointSnitch epSnitch;
    public final Map<String, CFMetaData> cfMetaData = new HashMap<String, CFMetaData>();

    KSMetaData(String name, Class<? extends AbstractReplicationStrategy> repStratClass, int replicationFactor, IEndPointSnitch epSnitch)
    {
        this.name = name;
        this.repStratClass = repStratClass;
        this.replicationFactor = replicationFactor;
        this.epSnitch = epSnitch;
    }
    
    public boolean equals(Object obj)
    {
        if (obj == null)
            return false;
        if (!(obj instanceof KSMetaData))
            return false;
        KSMetaData other = (KSMetaData)obj;
        return other.name.equals(name)
                && FBUtilities.equals(other.repStratClass, repStratClass)
                && other.replicationFactor == replicationFactor
                && sameEpSnitch(other, this)
                && other.cfMetaData.size() == cfMetaData.size()
                && other.cfMetaData.equals(cfMetaData);
    }

    // epsnitches generally have no state, so comparing class names is sufficient.
    private static boolean sameEpSnitch(KSMetaData a, KSMetaData b)
    {
        if (a.epSnitch == null && b.epSnitch == null)
            return true;
        else if (a.epSnitch == null && b.epSnitch != null)
            return false;
        else if (a.epSnitch != null && b.epSnitch == null)
            return false;
        else
            return a.epSnitch.getClass().getName().equals(b.epSnitch.getClass().getName());
    }
}
