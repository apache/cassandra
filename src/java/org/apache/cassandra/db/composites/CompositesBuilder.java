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
package org.apache.cassandra.db.composites;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.composites.Composite.EOC;

import static java.util.Collections.singletonList;

/**
 * Builder that allow to build multiple composites at the same time.
 */
public final class CompositesBuilder
{
    /**
     * The composite type.
     */
    private final CType ctype;

    /**
     * The elements of the composites
     */
    private final List<List<ByteBuffer>> elementsList = new ArrayList<>();

    /**
     * The number of elements that have been added.
     */
    private int size;

    /**
     * <code>true</code> if the composites have been build, <code>false</code> otherwise.
     */
    private boolean built;

    /**
     * <code>true</code> if the composites contains some <code>null</code> elements.
     */
    private boolean containsNull;

    /**
     * <code>true</code> if some empty collection have been added.
     */
    private boolean hasMissingElements;

    public CompositesBuilder(CType ctype)
    {
        this.ctype = ctype;
    }

    /**
     * Adds the specified element to all the composites.
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add D will result in the composites:
     * A-B-D and A-C-D.
     * </p>
     *
     * @param value the value of the next element
     * @return this <code>CompositeBuilder</code>
     */
    public CompositesBuilder addElementToAll(ByteBuffer value)
    {
        checkUpdateable();

        if (isEmpty())
            elementsList.add(new ArrayList<ByteBuffer>());

        for (int i = 0, m = elementsList.size(); i < m; i++)
        {
            if (value == null)
                containsNull = true;

            elementsList.get(i).add(value);
        }
        size++;
        return this;
    }

    /**
     * Adds individually each of the specified elements to the end of all of the existing composites.
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add D and E will result in the 4
     * composites: A-B-D, A-B-E, A-C-D and A-C-E.
     * </p>
     *
     * @param values the elements to add
     * @return this <code>CompositeBuilder</code>
     */
    public CompositesBuilder addEachElementToAll(List<ByteBuffer> values)
    {
        checkUpdateable();

        if (isEmpty())
            elementsList.add(new ArrayList<ByteBuffer>());

        if (values.isEmpty())
        {
            hasMissingElements = true;
        }
        else
        {
            for (int i = 0, m = elementsList.size(); i < m; i++)
            {
                List<ByteBuffer> oldComposite = elementsList.remove(0);

                for (int j = 0, n = values.size(); j < n; j++)
                {
                    List<ByteBuffer> newComposite = new ArrayList<>(oldComposite);
                    elementsList.add(newComposite);

                    ByteBuffer value = values.get(j);

                    if (value == null)
                        containsNull = true;

                    newComposite.add(values.get(j));
                }
            }
        }
        size++;
        return this;
    }


    /**
     * Adds individually each of the specified list of elements to the end of all of the existing composites.
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add [[D, E], [F, G]] will result in the 4
     * composites: A-B-D-E, A-B-F-G, A-C-D-E and A-C-F-G.
     * </p>
     *
     * @param values the elements to add
     * @return this <code>CompositeBuilder</code>
     */
    public CompositesBuilder addAllElementsToAll(List<List<ByteBuffer>> values)
    {
        checkUpdateable();

        if (isEmpty())
            elementsList.add(new ArrayList<ByteBuffer>());

        if (values.isEmpty())
        {
            hasMissingElements = true;
        }
        else
        {
            for (int i = 0, m = elementsList.size(); i < m; i++)
            {
                List<ByteBuffer> oldComposite = elementsList.remove(0);

                for (int j = 0, n = values.size(); j < n; j++)
                {
                    List<ByteBuffer> newComposite = new ArrayList<>(oldComposite);
                    elementsList.add(newComposite);

                    List<ByteBuffer> value = values.get(j);

                    if (value.isEmpty())
                        hasMissingElements = true;

                    if (value.contains(null))
                        containsNull = true;

                    newComposite.addAll(value);
                }
            }
            size += values.get(0).size();
        }
        return this;
    }

    /**
     * Returns the number of elements that can be added to the composites.
     *
     * @return the number of elements that can be added to the composites.
     */
    public int remainingCount()
    {
        return ctype.size() - size;
    }

    /**
     * Checks if some elements can still be added to the composites.
     *
     * @return <code>true</code> if it is possible to add more elements to the composites, <code>false</code> otherwise.
     */
    public boolean hasRemaining()
    {
        return remainingCount() > 0;
    }

    /**
     * Checks if this builder is empty.
     *
     * @return <code>true</code> if this builder is empty, <code>false</code> otherwise.
     */
    public boolean isEmpty()
    {
        return elementsList.isEmpty();
    }

    /**
     * Checks if the composites contains null elements.
     *
     * @return <code>true</code> if the composites contains <code>null</code> elements, <code>false</code> otherwise.
     */
    public boolean containsNull()
    {
        return containsNull;
    }

    /**
     * Checks if some empty list of values have been added
     * @return <code>true</code> if the composites have some missing elements, <code>false</code> otherwise.
     */
    public boolean hasMissingElements()
    {
        return hasMissingElements;
    }

    /**
     * Builds the <code>Composites</code>.
     *
     * @return the composites
     */
    public List<Composite> build()
    {
        return buildWithEOC(EOC.NONE);
    }

    /**
     * Builds the <code>Composites</code> with the specified EOC.
     *
     * @return the composites
     */
    public List<Composite> buildWithEOC(EOC eoc)
    {
        built = true;

        if (hasMissingElements)
            return Collections.emptyList();

        CBuilder builder = ctype.builder();

        if (elementsList.isEmpty())
            return singletonList(builder.build().withEOC(eoc));

        // Use a Set to sort if needed and eliminate duplicates
        Set<Composite> set = newSet();

        for (int i = 0, m = elementsList.size(); i < m; i++)
        {
            List<ByteBuffer> elements = elementsList.get(i);
            set.add(builder.buildWith(elements).withEOC(eoc));
        }

        return new ArrayList<>(set);
    }

    /**
     * Returns a new <code>Set</code> instance that will be used to eliminate duplicates and sort the results.
     *
     * @return a new <code>Set</code> instance.
     */
    private Set<Composite> newSet()
    {
        return new TreeSet<>(ctype);
    }

    private void checkUpdateable()
    {
        if (!hasRemaining() || built)
            throw new IllegalStateException("this CompositesBuilder cannot be updated anymore");
    }
}
