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

package org.apache.cassandra.anttasks;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.PropertyHelper;
import org.apache.tools.ant.Task;

/**
 * The task parses the dependency-check-report.json file and generates a report of all the dependencies
 * that are not allowed.
 * <p>
 * TL;DR It extracts jar -> license mapping from the dependency-check-report.json file, and then it tries to find
 * a matching license in the allowed-licenses.properties file. The licenses for specified jars can be overridden in the
 * license-overrides.properties file. The format of the file is: jar=license. It is instrumental in the case of
 * jars for which the OWASP scanner cannot detect the license.
 */
public class CheckLicensesTask extends Task
{
    private String allowedLicensesPath;
    private String licenseOverridesPath;
    private String dependencyCheckReportPath;
    private String outputProperty;

    public void setAllowedLicensesPath(String allowedLicensesPath)
    {
        this.allowedLicensesPath = allowedLicensesPath;
    }

    public void setLicenseOverridesPath(String licenseOverridesPath)
    {
        this.licenseOverridesPath = licenseOverridesPath;
    }

    public void setDependencyCheckReportPath(String dependencyCheckReportPath)
    {
        this.dependencyCheckReportPath = dependencyCheckReportPath;
    }

    public void setOutputProperty(String outputProperty)
    {
        this.outputProperty = outputProperty;
    }

    @Override
    public void execute() throws BuildException
    {
        PropertyHelper propertyHelper = PropertyHelper.getPropertyHelper(getProject());

        Properties allowedLicenses = new Properties();
        try (FileInputStream in = new FileInputStream(propertyHelper.replaceProperties(allowedLicensesPath)))
        {
            allowedLicenses.load(in);
        }
        catch (Exception e)
        {
            throw new BuildException("Failed to load allowed licenses from " + allowedLicensesPath, e);
        }

        Properties licenseOverrides = new Properties();
        try (FileInputStream in = new FileInputStream(propertyHelper.replaceProperties(licenseOverridesPath)))
        {
            licenseOverrides.load(in);
        }
        catch (Exception e)
        {
            throw new BuildException("Failed to load license overrides from " + licenseOverridesPath, e);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode dependencies;
        try (FileInputStream in = new FileInputStream(propertyHelper.replaceProperties(dependencyCheckReportPath)))
        {
            JsonNode reportJson = objectMapper.readTree(in);
            dependencies = reportJson.get("dependencies");
        }
        catch (IOException | RuntimeException ex)
        {
            throw new BuildException("Failed to load dependency check report from " + dependencyCheckReportPath, ex);
        }

        boolean fail = false;
        for (JsonNode dependency : dependencies)
        {
            String fileName = dependency.get("fileName").asText();
            String license = licenseOverrides.getProperty(fileName, null);
            if (license == null)
                license = dependency.get("license").asText();

            if (license == null)
            {
                System.out.println("No license found for " + fileName);
                fail = true;
                continue;
            }

            license = license.replaceAll("\n", " ");
            boolean found = false;
            for (String licenseName : allowedLicenses.stringPropertyNames())
            {
                String pattern = allowedLicenses.getProperty(licenseName);
                if (pattern != null && pattern.length() > 0)
                {
                    Pattern p = Pattern.compile(pattern);
                    if (p.matcher(license).matches())
                    {
                        found = true;
                        break;
                    }
                }
            }

            if (!found)
            {
                System.out.println("Unapproved license of " + fileName + ": " + license);
                fail = true;
            }
        }

        if (fail)
            getProject().setProperty(outputProperty, "true");
        else
            System.out.println("License check passed");
    }
}
