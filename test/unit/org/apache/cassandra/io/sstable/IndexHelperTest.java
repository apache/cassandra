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
package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.IntegerType;
import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class IndexHelperTest
{
    @Test
    public void testIndexHelper()
    {
        List<IndexInfo> indexes = new ArrayList<IndexInfo>();
        indexes.add(new IndexInfo(bytes(0L), bytes(5L), 0, 0));
        indexes.add(new IndexInfo(bytes(10L), bytes(15L), 0, 0));
        indexes.add(new IndexInfo(bytes(20L), bytes(25L), 0, 0));

        AbstractType comp = IntegerType.instance;

        assert 0 == IndexHelper.indexFor(bytes(-1L), indexes, comp, false);
        assert 0 == IndexHelper.indexFor(bytes(5L), indexes, comp, false);
        assert 1 == IndexHelper.indexFor(bytes(12L), indexes, comp, false);
        assert 2 == IndexHelper.indexFor(bytes(17L), indexes, comp, false);
        assert 3 == IndexHelper.indexFor(bytes(100L), indexes, comp, false);

        assert -1 == IndexHelper.indexFor(bytes(-1L), indexes, comp, true);
        assert 0 == IndexHelper.indexFor(bytes(5L), indexes, comp, true);
        assert 1 == IndexHelper.indexFor(bytes(12L), indexes, comp, true);
        assert 1 == IndexHelper.indexFor(bytes(17L), indexes, comp, true);
        assert 2 == IndexHelper.indexFor(bytes(100L), indexes, comp, true);
    }
}
