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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class KSMetaData
{
    public final String name;
    public final Class<? extends AbstractReplicationStrategy> strategyClass;
    public final int replicationFactor;
    public final IEndPointSnitch snitch;
    private final Map<String, CFMetaData> cfMetaData;

    KSMetaData(String name, Class<? extends AbstractReplicationStrategy> strategyClass, int replicationFactor, IEndPointSnitch snitch, CFMetaData... cfDefs)
    {
        this.name = name;
        this.strategyClass = strategyClass;
        this.replicationFactor = replicationFactor;
        this.snitch = snitch;
        Map<String, CFMetaData> cfmap = new HashMap<String, CFMetaData>();
        for (CFMetaData cfm : cfDefs)
            cfmap.put(cfm.cfName, cfm);
        this.cfMetaData = Collections.<String, CFMetaData>unmodifiableMap(cfmap);
    }
    
    public boolean equals(Object obj)
    {
        if (obj == null)
            return false;
        if (!(obj instanceof KSMetaData))
            return false;
        KSMetaData other = (KSMetaData)obj;
        return other.name.equals(name)
                && FBUtilities.equals(other.strategyClass, strategyClass)
                && other.replicationFactor == replicationFactor
                && sameEpSnitch(other, this)
                && other.cfMetaData.size() == cfMetaData.size()
                && other.cfMetaData.equals(cfMetaData);
    }

    // epsnitches generally have no state, so comparing class names is sufficient.
    private static boolean sameEpSnitch(KSMetaData a, KSMetaData b)
    {
        if (a.snitch == null && b.snitch == null)
            return true;
        else if (a.snitch == null && b.snitch != null)
            return false;
        else if (a.snitch != null && b.snitch == null)
            return false;
        else
            return a.snitch.getClass().getName().equals(b.snitch.getClass().getName());
    }

    public Map<String, CFMetaData> cfMetaData()
    {
        return cfMetaData;
    }
    
    public static byte[] serialize(KSMetaData ksm) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeUTF(ksm.name);
        dout.writeBoolean(ksm.strategyClass != null);
        if (ksm.strategyClass != null)
            dout.writeUTF(ksm.strategyClass.getName());
        dout.writeInt(ksm.replicationFactor);
        dout.writeBoolean(ksm.snitch != null);
        if (ksm.snitch != null)
            dout.writeUTF(ksm.snitch.getClass().getName());
        dout.writeInt(ksm.cfMetaData.size());
        for (CFMetaData cfm : ksm.cfMetaData.values())
            dout.write(CFMetaData.serialize(cfm));
        dout.close();
        return bout.toByteArray();
    }

    public static KSMetaData deserialize(InputStream in) throws IOException
    {
        DataInputStream din = new DataInputStream(in);
        String name = din.readUTF();
        Class<AbstractReplicationStrategy> repStratClass = null;
        try
        {
            repStratClass = din.readBoolean() ? (Class<AbstractReplicationStrategy>)Class.forName(din.readUTF()) : null;
        }
        catch (Exception ex)
        {
            throw new IOException(ex);
        }
        int replicationFactor = din.readInt();
        IEndPointSnitch epSnitch = null;
        try
        {
            epSnitch = din.readBoolean() ? (IEndPointSnitch)Class.forName(din.readUTF()).newInstance() : null;
        }
        catch (Exception ex)
        {
            throw new IOException(ex);
        }
        int cfsz = din.readInt();
        CFMetaData[] cfMetaData = new CFMetaData[cfsz];
        for (int i = 0; i < cfsz; i++)
        {
            try
            {
                cfMetaData[i] = CFMetaData.deserialize(din);
            }
            catch (IOException ex)
            {
                System.err.println(name);
                throw ex;
            }
        }

        KSMetaData ksm = new KSMetaData(name, repStratClass, replicationFactor, epSnitch, cfMetaData);
        return ksm;
    }
}
