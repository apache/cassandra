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
package org.apache.cassandra.utils.concurrent;

import org.junit.Ignore;
import org.junit.Test;

import junit.framework.Assert;

@Ignore
public abstract class AbstractTransactionalTest
{

    protected abstract TestableTransaction newTest() throws Exception;

    @Test
    public void testNoPrepare() throws Exception
    {
        TestableTransaction txn;

        txn = newTest();
        txn.assertInProgress();
        txn.testing.close();
        txn.assertAborted();

        txn = newTest();
        txn.assertInProgress();
        Assert.assertNull(txn.testing.abort(null));
        txn.assertAborted();
    }

    @Test
    public void testPrepare() throws Exception
    {
        TestableTransaction txn;
        txn = newTest();
        txn.assertInProgress();
        txn.testing.prepareToCommit();
        txn.assertPrepared();
        txn.testing.close();
        txn.assertAborted();

        txn = newTest();
        txn.assertInProgress();
        txn.testing.prepareToCommit();
        txn.assertPrepared();
        Assert.assertNull(txn.testing.abort(null));
        txn.assertAborted();
    }

    @Test
    public void testCommit() throws Exception
    {
        TestableTransaction txn = newTest();
        txn.assertInProgress();
        txn.testing.prepareToCommit();
        txn.assertPrepared();
        Assert.assertNull(txn.testing.commit(null));
        txn.assertCommitted();
        txn.testing.close();
        txn.assertCommitted();
        Throwable t = txn.testing.abort(null);
        Assert.assertTrue(t instanceof IllegalStateException);
        txn.assertCommitted();
    }

    @Test
    public void testThrowableReturn() throws Exception
    {
        TestableTransaction txn;
        txn = newTest();
        Throwable t = new RuntimeException();
        txn.testing.prepareToCommit();

        if (txn.commitCanThrow())
        {
            try
            {
                txn.testing.commit(t);
            }
            catch (Throwable tt)
            {
                Assert.assertEquals(t, tt);
            }

            Assert.assertEquals(t, txn.testing.abort(t));
            Assert.assertEquals(0, t.getSuppressed().length);
        }
        else
        {
            Assert.assertEquals(t, txn.testing.commit(t));
            Assert.assertEquals(t, txn.testing.abort(t));
            Assert.assertTrue(t.getSuppressed()[0] instanceof IllegalStateException);
        }


    }

    @Test
    public void testBadCommit() throws Exception
    {
        TestableTransaction txn;
        txn = newTest();
        try
        {
            txn.testing.commit(null);
            Assert.assertTrue(false);
        }
        catch (IllegalStateException t)
        {
        }
        txn.assertInProgress();
        Assert.assertNull(txn.testing.abort(null));
        txn.assertAborted();
        try
        {
            txn.testing.commit(null);
            Assert.assertTrue(false);
        }
        catch (IllegalStateException t)
        {
        }
        txn.assertAborted();
    }


    public static abstract class TestableTransaction
    {
        final Transactional testing;
        public TestableTransaction(Transactional transactional)
        {
            this.testing = transactional;
        }

        protected abstract void assertInProgress() throws Exception;
        protected abstract void assertPrepared() throws Exception;
        protected abstract void assertAborted() throws Exception;
        protected abstract void assertCommitted() throws Exception;

        protected boolean commitCanThrow()
        {
            return false;
        }
    }
}
