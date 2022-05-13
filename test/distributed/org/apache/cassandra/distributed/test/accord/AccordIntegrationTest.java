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

package org.apache.cassandra.distributed.test.accord;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.messages.Commit;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;

@SuppressWarnings("Convert2MethodRef")
public class AccordIntegrationTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordIntegrationTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @Test
    public void testRecovery() throws Exception
    {
        test(cluster -> {
            IMessageFilters.Filter lostApply = cluster.filters().verbs(Verb.ACCORD_APPLY_REQ.id).drop();
            IMessageFilters.Filter lostCommit = cluster.filters().verbs(Verb.ACCORD_COMMIT_REQ.id).to(2).drop();

            String query = "BEGIN TRANSACTION\n" +
                           "  LET row1 = (SELECT v FROM " + currentTable + " WHERE k=0 AND c=0);\n" +
                           "  SELECT row1.v;\n" +
                           "  IF row1 IS NULL THEN\n" +
                           "    INSERT INTO " + currentTable + " (k, c, v) VALUES (0, 0, 1);\n" +
                           "  END IF\n" +
                           "COMMIT TRANSACTION";
            // row1.v shouldn't have existed when the txn's SELECT was executed
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] { null }, query);

            lostApply.off();
            lostCommit.off();

            // Querying again should trigger recovery...
            query = "BEGIN TRANSACTION\n" +
                    "  LET row1 = (SELECT v FROM " + currentTable + " WHERE k=0 AND c=0);\n" +
                    "  SELECT row1.v;\n" +
                    "  IF row1.v = 1 THEN\n" +
                    "    UPDATE " + currentTable + " SET v=2 WHERE k = 0 AND c = 0;\n" +
                    "  END IF\n" +
                    "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 1 }, query);

            String check = "BEGIN TRANSACTION\n" +
                    "  SELECT * FROM " + currentTable + " WHERE k = ? AND c = ?;\n" +
                    "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, 0, 2}, check, 0, 0);

            query = "BEGIN TRANSACTION\n" +
                    "  LET row1 = (SELECT v FROM " + currentTable + " WHERE k=0 AND c=0);\n" +
                    "  SELECT row1.v;\n" +
                    "  IF row1 IS NULL THEN\n" +
                    "    INSERT INTO " + currentTable + " (k, c, v) VALUES (0, 0, 3);\n" +
                    "  END IF\n" +
                    "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 2 }, query);

            assertRowEqualsWithPreemptedRetry(cluster, new Object[] {0, 0, 2}, check, 0, 0);
        });
    }

    @Test
    public void testLostCommitReadTriggersFallbackRead() throws Exception
    {
        test(cluster -> {
            // It's expected that the required Read will happen regardless of whether this fails to return a read
            cluster.filters().verbs(Verb.ACCORD_COMMIT_REQ.id).messagesMatching((from, to, iMessage) -> cluster.get(from).callOnInstance(() -> {
                Message<?> msg = Instance.deserializeMessage(iMessage);
                if (msg.payload instanceof Commit)
                    return ((Commit) msg.payload).read != null;
                return false;
            })).drop();

            String query = "BEGIN TRANSACTION\n" +
                           "  LET row1 = (SELECT * FROM " + currentTable + " WHERE k = 0 AND c = 0);\n" +
                           "  SELECT row1.v;\n" +
                           "  IF row1 IS NULL THEN\n" +
                           "    INSERT INTO " + currentTable + " (k, c, v) VALUES (0, 0, 1);\n" +
                           "  END IF\n" +
                           "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] { null }, query);

            String check = "BEGIN TRANSACTION\n" +
                           "  SELECT * FROM " + currentTable + " WHERE k = ? AND c = ?;\n" +
                           "COMMIT TRANSACTION";
            assertRowEqualsWithPreemptedRetry(cluster, new Object[] { 0, 0, 1 }, check, 0, 0);
        });
    }
}
