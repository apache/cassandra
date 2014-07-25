#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Function Find-Conf
{
    $file = "";
    # Order of preference on grabbing environment settings:
    #   1:  %CASSANDRA_INCLUDE%
    #   2a: %USERPROFILE%/cassandra-env.ps1 (cmd-prompt)
    #   2b: $HOME/cassandra-env.ps1 (cygwin)
    #   3:  %CASSANDRA_HOME%/conf/cassandra-env.ps1
    #   4:  Relative to current working directory (../conf)
    if (Test-Path Env:\CASSANDRA_INCLUDE)
    {
        $file = "$env:CASSANDRA_INCLUDE"
    }
    elseif (Test-Path "$env:USERPROFILE/cassandra-env.ps1")
    {
        $file = "$env:USERPROFILE/cassandra-env.ps1"
    }
    elseif (Test-Path "$env:HOME/cassandra-env.ps1")
    {
        $file = "$env:HOME/cassandra-env.ps1"
    }
    elseif (Test-Path Env:\CASSANDRA_HOME)
    {
        $file = "$env:CASSANDRA_HOME/conf/cassandra-env.ps1"
    }
    else
    {
        $file = [System.IO.Directory]::GetCurrentDirectory() + "/../conf/cassandra-env.ps1"
    }
    $file = $file -replace "\\", "/"

    if (Test-Path $file)
    {
        return $file
    }
    else
    {
        echo "Error with environment file resolution.  Path: [$file] not found."
        exit
    }
}
