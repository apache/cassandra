package org.apache.cassandra.triggers;
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


import java.util.Collection;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;

/**
 * Trigger interface, For every partition update received by the coordinator {@link #augment(Partition)}
 * is called.<p>
 *
 * <b> Contract:</b><br>
 * 1) Implementation of this interface should only have a constructor without parameters <br>
 * 2) ITrigger implementation can be instantiated multiple times during the server life time.
 *      (Depends on the number of times trigger folder is updated.)<br>
 * 3) ITrigger implementation should be state-less (avoid dependency on instance variables).<br>
 *
 * <br><b>The API is still beta and can change.</b>
 */
public interface ITrigger
{
    /**
     * Called exactly once per CF update, returned mutations are atomically updated.
     *
     * @param update - update received for the CF
     * @return additional modifications to be applied along with the supplied update
     */
    public Collection<Mutation> augment(Partition update);
}
