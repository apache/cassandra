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

package org.apache.cassandra;

import org.junit.Test;
import org.apache.cassandra.utils.ByteConvertor;
import static junit.framework.Assert.assertEquals;

public class ByteConvertorTest
{
   @Test
   public void testByteToMegaByte()
   {
       int test1 = 100000000;
       long test2= 200000000;
       long test1_ans = 95;
       long test2_ans = 190;
       assertEquals(ByteConvertor.bytesToMeg(test1),test1_ans);
       assertEquals(ByteConvertor.bytesToMeg(test2),test2_ans);
   }
}
