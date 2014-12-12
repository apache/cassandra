package org.apache.cassandra.stress.settings;
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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class OptionEnumProbabilities<T> extends OptionMulti
{
    final List<OptMatcher<T>> options;

    public static class Opt<T>
    {
        final T option;
        final String defaultValue;

        public Opt(T option, String defaultValue)
        {
            this.option = option;
            this.defaultValue = defaultValue;
        }
    }

    private static final class OptMatcher<T> extends OptionSimple
    {
        final T opt;
        OptMatcher(T opt, String defaultValue)
        {
            super(opt.toString().toLowerCase() + "=", "[0-9]+(\\.[0-9]+)?", defaultValue, "Performs this many " + opt + " operations out of total", false);
            this.opt = opt;
        }
    }

    public OptionEnumProbabilities(List<Opt<T>> universe, String name, String description)
    {
        super(name, description, false);
        List<OptMatcher<T>> options = new ArrayList<>();
        for (Opt<T> option : universe)
            options.add(new OptMatcher<T>(option.option, option.defaultValue));
        this.options = options;
    }

    @Override
    public List<? extends Option> options()
    {
        return options;
    }

    Map<T, Double> ratios()
    {
        List<? extends Option> ratiosIn = setByUser() ? optionsSetByUser() : defaultOptions();
        Map<T, Double> ratiosOut = new HashMap<>();
        for (Option opt : ratiosIn)
        {
            OptMatcher<T> optMatcher = (OptMatcher<T>) opt;
            double d = Double.parseDouble(optMatcher.value());
            ratiosOut.put(optMatcher.opt, d);
        }
        return ratiosOut;
    }
}

