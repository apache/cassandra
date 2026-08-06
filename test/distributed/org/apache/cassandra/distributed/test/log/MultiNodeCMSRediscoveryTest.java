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

package org.apache.cassandra.distributed.test.log;

/**
 * Tests a full-cluster restart with every node's broadcast address changing while down.
 * The cluster is configured with 6 nodes, 3 of which are CMS members.
 */
public class MultiNodeCMSRediscoveryTest extends CMSRediscoveryTestBase
{
    @Override
    int clusterSize()
    {
        return 6;
    }

    @Override
    int cmsSize()
    {
        return 3;
    }
}
