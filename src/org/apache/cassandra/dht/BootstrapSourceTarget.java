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

package org.apache.cassandra.dht;

import org.apache.cassandra.net.EndPoint;

/**
 * This class encapsulates who is the source and the
 * target of a bootstrap for a particular range.
 */
class BootstrapSourceTarget
{
    protected EndPoint source_;
    protected EndPoint target_;
    
    BootstrapSourceTarget(EndPoint source, EndPoint target)
    {
        source_ = source;
        target_ = target;
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        sb.append("SOURCE: ");
        sb.append(source_);
        sb.append(" ----> ");
        sb.append("TARGET: ");
        sb.append(target_);
        sb.append(" ");
        return sb.toString();
    }
}
