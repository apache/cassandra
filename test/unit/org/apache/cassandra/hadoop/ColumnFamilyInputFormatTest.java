package org.apache.cassandra.hadoop;
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


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ColumnFamilyInputFormatTest
{
    @Test
    public void testSlicePredicate()
    {
        long columnValue = 1271253600000l;
        ByteBuffer columnBytes = ByteBufferUtil.bytes(columnValue);

        List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        columnNames.add(columnBytes);
        SlicePredicate originalPredicate = new SlicePredicate().setColumn_names(columnNames);

        Configuration conf = new Configuration();
        ConfigHelper.setInputSlicePredicate(conf, originalPredicate);

        SlicePredicate rtPredicate = ConfigHelper.getInputSlicePredicate(conf);
        assert rtPredicate.column_names.size() == 1;
        assert originalPredicate.column_names.get(0).equals(rtPredicate.column_names.get(0));
    }
}
