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
package org.apache.cassandra.db.marshal;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumnContainer;

public abstract class AbstractCommutativeType extends AbstractType
{
    public boolean isCommutative()
    {
        return true;
    }

    /**
     * create commutative column
     */
    public abstract Column createColumn(ByteBuffer name, ByteBuffer value, long timestamp);

    /**
     * remove target node from commutative columns
     */
    public abstract void cleanContext(IColumnContainer cc, InetAddress node);
}
