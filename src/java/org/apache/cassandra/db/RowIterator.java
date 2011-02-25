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
package org.apache.cassandra.db;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.utils.ReducingIterator;

/**
 * Row iterator that allows us to close the underlying iterators.
 */
public class RowIterator implements Closeable, Iterator<Row>
{
    private ReducingIterator<IColumnIterator, Row> reduced;
    private List<Iterator<IColumnIterator>> iterators;

    /**
     * @param reduced   Reducing iterator that takes multiple iterators and provides us with
     *                  one row at the time.
     * @param iterators The underlying iterators that we will close when done.
     */
    public RowIterator(ReducingIterator<IColumnIterator, Row> reduced, List<Iterator<IColumnIterator>> iterators)
    {
        this.reduced = reduced;
        this.iterators = iterators;
    }

    public boolean hasNext()
    {
        return reduced.hasNext();
    }

    public Row next()
    {
        return reduced.next();
    }

    public void remove()
    {
        reduced.remove();
    }

    public void close() throws IOException
    {
        for (Iterator<IColumnIterator> iter : iterators)
        {
            if (iter instanceof Closeable)
            {
                ((Closeable)iter).close();
            }
        }
	}
}
