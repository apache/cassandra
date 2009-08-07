/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import org.junit.Test;
import org.junit.Assert;
import org.apache.cassandra.net.io.StreamContextManager;

import java.util.List;
import java.util.ArrayList;

public class StorageServiceTest {


    @Test
    public void testImpossibleCast() {
        List<StreamContextManager.StreamContext> streamContexts = new ArrayList<StreamContextManager.StreamContext>();
        try {
            StreamContextManager.StreamContext[] arr = (StreamContextManager.StreamContext[]) streamContexts.toArray();
            Assert.fail("expected ClassCastException from Object[] to StreamContextManager.StreamContext[]");

        } catch (ClassCastException e) {
            Assert.assertTrue(true);
        }
    }


    @Test
    public void testPossibleCast() {

        List<StreamContextManager.StreamContext> streamContexts = new ArrayList<StreamContextManager.StreamContext>();
        StreamContextManager.StreamContext[] contexts = streamContexts.toArray(new StreamContextManager.StreamContext[streamContexts.size()]);
        Assert.assertTrue(contexts.length == 0);

        StreamContextManager.StreamContext streamContext = new StreamContextManager.StreamContext("foofile", 0, "fooTable");
        streamContexts.add(streamContext);
        contexts = streamContexts.toArray(new StreamContextManager.StreamContext[streamContexts.size()]);
        Assert.assertTrue(contexts.length == 1);
    }
}
