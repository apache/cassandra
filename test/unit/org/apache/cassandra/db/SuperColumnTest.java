/*
* Licensed to the Apache Software Foundation (ASF) under one * or more contributor license agreements.  See the NOTICE file
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
package org.apache.cassandra.db;

import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.apache.cassandra.Util.getBytes;
import org.apache.cassandra.db.marshal.LongType;

public class SuperColumnTest
{   
    @Test
    public void testMissingSubcolumn() {
    	SuperColumn sc = new SuperColumn("sc1".getBytes(), LongType.instance, ClockType.Timestamp);
    	sc.addColumn(new Column(getBytes(1), "value".getBytes(), new TimestampClock(1)));
    	assertNotNull(sc.getSubColumn(getBytes(1)));
    	assertNull(sc.getSubColumn(getBytes(2)));
    }
}
