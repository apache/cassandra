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
param (
    [string]$p,
    [string]$batchpid,
    [switch]$f,
    [switch]$silent,
    [switch]$help
)

#-----------------------------------------------------------------------------
Function ValidateArguments
{
    if (!$p)
    {
        PrintUsage
    }
    if ($help)
    {
        PrintUsage
    }
}

#-----------------------------------------------------------------------------
Function PrintUsage
{
    echo @"

usage: stop-server.ps1 -p pidfile -f[-help]
    -p      pidfile tracked by server and removed on close.
    -s      Silent.  Don't print success/failure data.
    -f      force kill.
"@
    exit
}

#-----------------------------------------------------------------------------
Function KillProcess
{
    if (-Not (Test-Path $p))
    {
        if (-Not ($silent))
        {
            echo "Error - pidfile not found.  Aborting."
        }
        exit
    }

    $t = @"
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Threading;

    namespace PowerStopper
    {
        public static class Stopper
        {
            delegate bool ConsoleCtrlDelegate(CtrlTypes CtrlType);

            [DllImport("kernel32.dll", SetLastError = true)]
            static extern bool AttachConsole(uint dwProcessId);

            [DllImport("kernel32.dll", SetLastError = true, ExactSpelling = true)]
            static extern bool FreeConsole();

            enum CtrlTypes : uint
            {
                CTRL_C_EVENT = 0,
                CTRL_BREAK_EVENT,
                CTRL_CLOSE_EVENT,
                CTRL_LOGOFF_EVENT = 5,
                CTRL_SHUTDOWN_EVENT
            }

            [DllImport("kernel32.dll")]
            [return: MarshalAs(UnmanagedType.Bool)]
            private static extern bool GenerateConsoleCtrlEvent(CtrlTypes dwCtrlEvent, uint dwProcessGroupId);

            [DllImport("kernel32.dll")]
            static extern bool SetConsoleCtrlHandler(ConsoleCtrlDelegate HandlerRoutine, bool Add);

            // Our output gets swallowed on ms-dos as we can't re-attach our console to the output of the cmd
            // running the batch file.
            public static void StopProgram(int pidToKill, int consolePid, bool silent)
            {
                Process proc = null;
                try
                {
                    proc = Process.GetProcessById(pidToKill);
                }
                catch (ArgumentException)
                {
                    if (!silent)
                        System.Console.WriteLine("Process " + pidToKill + " not found.  Aborting.");
                    return;
                }

                if (!FreeConsole())
                {
                    if (!silent)
                        System.Console.WriteLine("Failed to FreeConsole to attach to running cassandra process.  Aborting.");
                    return;
                }

                if (AttachConsole((uint)pidToKill))
                {
                    //Disable Ctrl-C handling for our program
                    SetConsoleCtrlHandler(null, true);
                    GenerateConsoleCtrlEvent(CtrlTypes.CTRL_C_EVENT, 0);

                    // Must wait here. If we don't and re-enable Ctrl-C
                    // handling below too fast, we might terminate ourselves.
                    proc.WaitForExit(2000);
                    FreeConsole();

                    // Re-attach to current console to write output
                    if (consolePid >= 0)
                        AttachConsole((uint)consolePid);

                    // Re-enable Ctrl-C handling or any subsequently started
                    // programs will inherit the disabled state.
                    SetConsoleCtrlHandler(null, false);

                    if (!silent)
                        System.Console.WriteLine("Successfully sent ctrl+c to process with id: " + pidToKill + ".");
                }
                else
                {
                    if (!silent)
                    {
                        string errorMsg = new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error()).Message;
                        System.Console.WriteLine("Error attaching to pid: " + pidToKill + ": " + Marshal.GetLastWin32Error() + " - " + errorMsg);
                    }
                }
            }
        }
    }
"@
    # cygwin assumes environment variables are case sensitive which causes problems when
    # the type dictionary references 'tmp' or 'temp' and throws a System.ArgumentException
    $oldTmp = $env:TMP
    $oldTemp = $env:Temp
    $env:TMP=''
    $env:TEMP=''
    Add-Type -TypeDefinition $t
    $env:TMP = $oldTmp
    $env:TEMP = $oldTemp

    $pidToKill = Get-Content $p
    # If run in cygwin, we don't get the TITLE / pid combo in stop-server.bat but also don't need
    # to worry about reattaching console output as it gets stderr/stdout even after the C#/C++
    # FreeConsole calls.
    if ($batchpid -eq "No")
    {
        $batchpid = -1
    }

    if ($f)
    {
        taskkill /f /pid $pidToKill
    }
    else
    {
        [PowerStopper.Stopper]::StopProgram($pidToKill, $batchpid, $silent)
    }
}

#-----------------------------------------------------------------------------
ValidateArguments
KillProcess
