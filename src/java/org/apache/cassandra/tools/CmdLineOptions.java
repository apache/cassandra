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

package org.apache.cassandra.tools;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CmdLineOptions extends Options
{
    /**
     * Add option with argument and argument name
     *
     * @param opt         shortcut for option name
     * @param longOpt     complete option name
     * @param argName     argument name
     * @param description description of the option
     * @return updated Options object
     */
    public Options addOption(String opt, String longOpt, String argName, String description)
    {
        Option option = new Option(opt, longOpt, true, description);
        option.setArgName(argName);

        return addOption(option);
    }

    /**
     * Add option with argument and argument name that accepts being defined multiple times as a list
     *
     * @param opt         shortcut for option name
     * @param longOpt     complete option name
     * @param argName     argument name
     * @param description description of the option
     * @return updated Options object
     */
    public Options addOptionList(String opt, String longOpt, String argName, String description)
    {
        Option option = new Option(opt, longOpt, true, description);
        option.setArgName(argName);
        option.setArgs(Option.UNLIMITED_VALUES);

        return addOption(option);
    }

    /**
     * Add option without argument
     *
     * @param opt         shortcut for option name
     * @param longOpt     complete option name
     * @param description description of the option
     * @return updated Options object
     */
    public Options addOption(String opt, String longOpt, String description)
    {
        return addOption(new Option(opt, longOpt, false, description));
    }
}
