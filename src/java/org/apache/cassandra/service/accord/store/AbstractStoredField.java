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

package org.apache.cassandra.service.accord.store;

import org.apache.cassandra.service.accord.AccordState;

public abstract class AbstractStoredField
{
    private static final int LOADED_FLAG = 0x01;
    private static final int EMPTY_FLAG = 0x01 << 1;
    private static final int CHANGED_FLAG = 0x01 << 2;
    private static final int CLEARED_FLAG = 0x01 << 3;
    private static final int WRITE_ONLY_FLAG = 0x01 << 4;
    private static final int READ_ONLY_FLAG = 0x01 << 5;

    private byte flag;

    public AbstractStoredField(AccordState.Kind kind)
    {
        this.flag = 0;
        if (kind == AccordState.Kind.WRITE_ONLY)
            set(WRITE_ONLY_FLAG);
        if (kind == AccordState.Kind.READ_ONLY)
            set(READ_ONLY_FLAG);
    }

    @Override
    public String toString()
    {
        if (!hasValue())
            return "<empty>";
        if (check(WRITE_ONLY_FLAG))
            return "<write-only>";
        preGet();
        if (hasModifications())
            return '*' + valueString();
        return valueString();
    }

    private void clear(int v)
    {
        flag &= ~v;
    }

    private boolean check(int v)
    {
        return (flag & v) != 0;
    }

    private void set(int v)
    {
        flag |= v;
    }

    public boolean hasValue()
    {
        return isLoaded() && !isEmpty();
    }

    public boolean isLoaded()
    {
        return check(LOADED_FLAG);
    }

    public void setEmpty()
    {
        if (check(0xFF))
            throw new IllegalStateException("Cannot set previously loaded/initialized commands to empty");
        set(LOADED_FLAG | EMPTY_FLAG);
    }

    public boolean isEmpty()
    {
        return check(EMPTY_FLAG);
    }

    void preUnload()
    {
        if (hasModifications())
            throw new IllegalStateException("Cannot unload a field with unsaved changes");
        flag = 0;
    }

    void preLoad()
    {
        if (hasModifications())
            throw new IllegalStateException("Cannot load into a field with unsaved changes");
        clear(EMPTY_FLAG);
        set(LOADED_FLAG);
    }

    void preChange()
    {
        if (check(READ_ONLY_FLAG))
            throw new IllegalStateException("Cannot update a read only field");
        clear(EMPTY_FLAG);
        set(LOADED_FLAG | CHANGED_FLAG);
    }

    void preBlindChange()
    {
        set(CHANGED_FLAG);
    }

    void preGet()
    {
        if (!check(LOADED_FLAG))
            throw new IllegalStateException("Cannot read unloaded fields");
        if (check(EMPTY_FLAG))
            throw new IllegalStateException("Cannot read empty fields");
        if (check(WRITE_ONLY_FLAG))
            throw new IllegalStateException("Cannot read write only fields");
    }

    void preClear()
    {
        set(CLEARED_FLAG | LOADED_FLAG | CHANGED_FLAG);
    }

    public boolean hasModifications()
    {
        return check(CHANGED_FLAG);
    }

    public void clearModifiedFlag()
    {
        clear(CHANGED_FLAG);
    }

    public boolean wasCleared()
    {
        return check(CLEARED_FLAG);
    }

    public abstract String valueString();
}
