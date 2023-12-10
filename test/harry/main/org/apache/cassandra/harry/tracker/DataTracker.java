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

package org.apache.cassandra.harry.tracker;

import java.util.function.LongConsumer;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;

public interface DataTracker
{
    void onLtsStarted(LongConsumer onLts);
    void onLtsFinished(LongConsumer onLts);

    void beginModification(long lts);
    void endModification(long lts);

    default void beginValidation(long pd) {}
    default void endValidation(long pd) {}

    long maxStarted();
    boolean isFinished(long lts);

    Configuration.DataTrackerConfiguration toConfig();

    interface DataTrackerFactory
    {
        DataTracker make(OpSelectors.PdSelector pdSelector, SchemaSpec schemaSpec);
    }

    DataTracker NO_OP = new NoOpDataTracker();

    class NoOpDataTracker implements DataTracker
    {
        private NoOpDataTracker() {}

        public void onLtsStarted(LongConsumer onLts){}
        public void onLtsFinished(LongConsumer onLts){}

        public void beginModification(long lts){}
        public void endModification(long lts){}

        public long maxStarted() { return 0; }
        public boolean isFinished(long lts) { return false; }

        public Configuration.DataTrackerConfiguration toConfig(){ return null; }
    }

}
