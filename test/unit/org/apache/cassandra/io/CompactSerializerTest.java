package org.apache.cassandra.io;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import org.apache.cassandra.CleanupHelper;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CompactSerializerTest extends CleanupHelper
{
    private static Set<String> expectedClassNames;
    private static List<String> discoveredClassNames;
    
    @BeforeClass
    public static void scanClasspath()
    {
        expectedClassNames = new HashSet<String>();
        expectedClassNames.add("RangeSliceCommandSerializer");
        expectedClassNames.add("ReadCommandSerializer");
        expectedClassNames.add("ReadResponseSerializer");
        expectedClassNames.add("RowSerializer");
        expectedClassNames.add("RowMutationSerializer");
        expectedClassNames.add("SliceByNamesReadCommandSerializer");
        expectedClassNames.add("SliceFromReadCommandSerializer");
        expectedClassNames.add("TruncateResponseSerializer");
        expectedClassNames.add("TruncationSerializer");
        expectedClassNames.add("WriteResponseSerializer");
        expectedClassNames.add("EndpointStateSerializer");
        expectedClassNames.add("GossipDigestSerializer");
        expectedClassNames.add("GossipDigestAck2MessageSerializer");
        expectedClassNames.add("GossipDigestAckMessageSerializer");
        expectedClassNames.add("GossipDigestSynMessageSerializer");
        expectedClassNames.add("HeartBeatStateSerializer");
        expectedClassNames.add("VersionedValueSerializer");
        expectedClassNames.add("HeaderSerializer");
        expectedClassNames.add("MessageSerializer");
        expectedClassNames.add("PendingFileSerializer");
        expectedClassNames.add("StreamHeaderSerializer");
        expectedClassNames.add("FileStatusSerializer");
        expectedClassNames.add("StreamRequestMessageSerializer");
        expectedClassNames.add("CounterMutationSerializer");
        expectedClassNames.add("HashableSerializer");
        expectedClassNames.add("StreamingRepairTaskSerializer");
        
        discoveredClassNames = new ArrayList<String>();
        String cp = System.getProperty("java.class.path");
        assert cp != null;
        String[] parts = cp.split(File.pathSeparator, -1);
        class DirScanner 
        {
            void scan(File f, String ctx) 
            {
                String newCtx = ctx == null ? f.getName().equals("org") ? f.getName() : null : ctx + "." + f.getName();
                if (f.isDirectory())
                {
                    for (File child : f.listFiles())
                    {
                        scan(child, newCtx);
                    }
                }
                else if (f.getName().endsWith(".class"))
                {
                    String fName = f.getName();
                    String className = ctx + "." + fName.substring(0, fName.lastIndexOf('.'));
                    try
                    {
                        Class cls = Class.forName(className);
                        String simpleName = cls.getSimpleName();
                        classTraversal: while (cls != null)
                        {
                            Type[] interfaces = cls.getGenericInterfaces();
                            for (Type t : interfaces)
                            {
                                if(t instanceof ParameterizedType)
                                {
                                    ParameterizedType pt = (ParameterizedType)t;
                                    if (((Class)pt.getRawType()).getSimpleName().equals("IVersionedSerializer"))
                                    {
                                        discoveredClassNames.add(simpleName);
                                        break classTraversal;
                                    }
                                }
                            }
                            cls = cls.getSuperclass();
                        }
                    }
                    catch (ClassNotFoundException ex) 
                    {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        
        DirScanner dirScanner = new DirScanner();
        for (String cpItem : parts)
        {
            File f = new File(cpItem);
            if (f.exists() && f.isDirectory())
                dirScanner.scan(f, null);
        }
    }
    
    /** look for classes I expect to find. */
    @Test
    public void verifyAllSimpleNamesTest()
    {
        for (String clsName : expectedClassNames)
            assert discoveredClassNames.contains(clsName) : clsName + " was not discovered";
    }
    
    /** look for classes I do not expect to find. */
    @Test
    public void noOthersTest()
    {
        for (String clsName : discoveredClassNames)
            assert expectedClassNames.contains(clsName) : clsName + " was discovered";
        assert true;
    }
}
