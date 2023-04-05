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

package org.apache.cassandra.db;

import org.junit.Assert;
import org.junit.Test;

public class ClusteringBoundTest
{
    @Test
    public void arrayTopAndBottom()
    {
        Assert.assertTrue(ArrayClusteringBound.BOTTOM.isBottom());
        Assert.assertFalse(ArrayClusteringBound.BOTTOM.isTop());
        Assert.assertTrue(ArrayClusteringBound.TOP.isTop());
        Assert.assertFalse(ArrayClusteringBound.TOP.isBottom());
    }

    @Test
    public void bufferTopAndBottom()
    {
        Assert.assertTrue(BufferClusteringBound.BOTTOM.isBottom());
        Assert.assertFalse(BufferClusteringBound.BOTTOM.isTop());
        Assert.assertTrue(BufferClusteringBound.TOP.isTop());
        Assert.assertFalse(BufferClusteringBound.TOP.isBottom());
    }
}
