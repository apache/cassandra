package org.apache.cassandra;
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


import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OrderedJUnit4ClassRunner extends BlockJUnit4ClassRunner
{

    public OrderedJUnit4ClassRunner(Class aClass) throws InitializationError
    {
        super(aClass);
    }

    @Override
    protected List<FrameworkMethod> computeTestMethods()
    {
        final List<FrameworkMethod> list = super.computeTestMethods();
        try
        {
            final List<FrameworkMethod> copy = new ArrayList<FrameworkMethod>(list);
            Collections.sort(copy, MethodComparator.getFrameworkMethodComparatorForJUnit4());
            return copy;
        }
        catch (Throwable throwable)
        {
            return list;
        }
    }
}
