/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Unit test for {@code AbstractIterator}.
 *
 * @author Kevin Bourrillion
 */
@SuppressWarnings("serial") // No serialization is used in this test
// TODO(cpovirk): why is this slow (>1m/test) under GWT when fully optimized?
public class AbstractIteratorTest
{
    @Test
    public void testDefaultBehaviorOfNextAndHasNext()
    {

        // This sample AbstractIterator returns 0 on the first call, 1 on the
        // second, then signals that it's reached the end of the data
        Iterator<Integer> iter = new AbstractIterator<Integer>()
        {
            private int rep;

            @Override
            public Integer computeNext()
            {
                switch (rep++)
                {
                    case 0:
                        return 0;
                    case 1:
                        return 1;
                    case 2:
                        return endOfData();
                    default:
                        Assert.fail("Should not have been invoked again");
                        return null;
                }
            }
        };

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(0, (int) iter.next());

        // verify idempotence of hasNext()
        Assert.assertTrue(iter.hasNext());
        Assert.assertTrue(iter.hasNext());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, (int) iter.next());

        Assert.assertFalse(iter.hasNext());

        // Make sure computeNext() doesn't get invoked again
        Assert.assertFalse(iter.hasNext());

        try
        {
            iter.next();
            Assert.fail("no exception thrown");
        }
        catch (NoSuchElementException expected)
        {
        }
    }

    @Test
    public void testDefaultBehaviorOfPeek()
    {
    /*
     * This sample AbstractIterator returns 0 on the first call, 1 on the
     * second, then signals that it's reached the end of the data
     */
        AbstractIterator<Integer> iter = new AbstractIterator<Integer>()
        {
            private int rep;

            @Override
            public Integer computeNext()
            {
                switch (rep++)
                {
                    case 0:
                        return 0;
                    case 1:
                        return 1;
                    case 2:
                        return endOfData();
                    default:
                        Assert.fail("Should not have been invoked again");
                        return null;
                }
            }
        };

        Assert.assertEquals(0, (int) iter.peek());
        Assert.assertEquals(0, (int) iter.peek());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(0, (int) iter.peek());
        Assert.assertEquals(0, (int) iter.next());

        Assert.assertEquals(1, (int) iter.peek());
        Assert.assertEquals(1, (int) iter.next());

        try
        {
            iter.peek();
            Assert.fail("peek() should throw NoSuchElementException at end");
        }
        catch (NoSuchElementException expected)
        {
        }

        try
        {
            iter.peek();
            Assert.fail("peek() should continue to throw NoSuchElementException at end");
        }
        catch (NoSuchElementException expected)
        {
        }

        try
        {
            iter.next();
            Assert.fail("next() should throw NoSuchElementException as usual");
        }
        catch (NoSuchElementException expected)
        {
        }

        try
        {
            iter.peek();
            Assert.fail("peek() should still throw NoSuchElementException after next()");
        }
        catch (NoSuchElementException expected)
        {
        }
    }

    @Test
    public void testFreesNextReference() throws InterruptedException
    {
        Iterator<Object> itr = new AbstractIterator<Object>()
        {
            @Override
            public Object computeNext()
            {
                return new Object();
            }
        };
        WeakReference<Object> ref = new WeakReference<Object>(itr.next());
        while (ref.get() != null)
        {
            System.gc();
            Thread.sleep(1);
        }
    }

    @Test
    public void testDefaultBehaviorOfPeekForEmptyIteration()
    {

        AbstractIterator<Integer> empty = new AbstractIterator<Integer>()
        {
            private boolean alreadyCalledEndOfData;

            @Override
            public Integer computeNext()
            {
                if (alreadyCalledEndOfData)
                {
                    Assert.fail("Should not have been invoked again");
                }
                alreadyCalledEndOfData = true;
                return endOfData();
            }
        };

        try
        {
            empty.peek();
            Assert.fail("peek() should throw NoSuchElementException at end");
        }
        catch (NoSuchElementException expected)
        {
        }

        try
        {
            empty.peek();
            Assert.fail("peek() should continue to throw NoSuchElementException at end");
        }
        catch (NoSuchElementException expected)
        {
        }
    }

    @Test
    public void testException()
    {
        final SomeUncheckedException exception = new SomeUncheckedException();
        Iterator<Integer> iter = new AbstractIterator<Integer>()
        {
            @Override
            public Integer computeNext()
            {
                throw exception;
            }
        };

        // It should pass through untouched
        try
        {
            iter.hasNext();
            Assert.fail("No exception thrown");
        }
        catch (SomeUncheckedException e)
        {
            Assert.assertSame(exception, e);
        }
    }

    @Test
    public void testExceptionAfterEndOfData()
    {
        Iterator<Integer> iter = new AbstractIterator<Integer>()
        {
            @Override
            public Integer computeNext()
            {
                endOfData();
                throw new SomeUncheckedException();
            }
        };
        try
        {
            iter.hasNext();
            Assert.fail("No exception thrown");
        }
        catch (SomeUncheckedException expected)
        {
        }
    }

    @Test
    public void testCantRemove()
    {
        Iterator<Integer> iter = new AbstractIterator<Integer>()
        {
            boolean haveBeenCalled;

            @Override
            public Integer computeNext()
            {
                if (haveBeenCalled)
                {
                    endOfData();
                }
                haveBeenCalled = true;
                return 0;
            }
        };

        Assert.assertEquals(0, (int) iter.next());

        try
        {
            iter.remove();
            Assert.fail("No exception thrown");
        }
        catch (UnsupportedOperationException expected)
        {
        }
    }

    @Test
    public void testSneakyThrow() throws Exception
    {
        Iterator<Integer> iter = new AbstractIterator<Integer>()
        {
            boolean haveBeenCalled;

            @Override
            public Integer computeNext()
            {
                if (haveBeenCalled)
                {
                    Assert.fail("Should not have been called again");
                }
                else
                {
                    haveBeenCalled = true;
                    sneakyThrow(new SomeCheckedException());
                }
                return null; // never reached
            }
        };

        // The first time, the sneakily-thrown exception comes out
        try
        {
            iter.hasNext();
            Assert.fail("No exception thrown");
        }
        catch (Exception e)
        {
            if (!(e instanceof SomeCheckedException))
            {
                throw e;
            }
        }

        // But the second time, AbstractIterator itself throws an ISE
        try
        {
            iter.hasNext();
            Assert.fail("No exception thrown");
        }
        catch (IllegalStateException expected)
        {
        }
    }

    @Test
    public void testReentrantHasNext()
    {
        Iterator<Integer> iter = new AbstractIterator<Integer>()
        {
            @Override
            protected Integer computeNext()
            {
                hasNext();
                return null;
            }
        };
        try
        {
            iter.hasNext();
            Assert.fail();
        }
        catch (IllegalStateException expected)
        {
        }
    }

    /**
     * Throws a undeclared checked exception.
     */
    private static void sneakyThrow(Throwable t)
    {
        class SneakyThrower<T extends Throwable>
        {
            @SuppressWarnings("unchecked")
                // not really safe, but that's the point
            void throwIt(Throwable t) throws T
            {
                throw (T) t;
            }
        }
        new SneakyThrower<Error>().throwIt(t);
    }

    private static class SomeCheckedException extends Exception
    {
    }

    private static class SomeUncheckedException extends RuntimeException
    {
    }
}
