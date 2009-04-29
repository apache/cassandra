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

/*
 * Helper function to write some basic HTML.
 */

package org.apache.cassandra.net.http;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

import java.util.List;
import java.util.Set;

/**
 *
 * @author kranganathan
 */
public class HTMLFormatter
{
    protected StringBuilder sb_ = null;
    private boolean writeBody_;

    public HTMLFormatter()
    {
        sb_ = new StringBuilder();
    }

    public HTMLFormatter(StringBuilder sb)
    {
        sb_ = sb;
    }

    public void startBody()
    {
    	startBody(false, "", true, true);
    }

    public void startBody(boolean writeJSCallback, String jsCallbackFunction, boolean writeCSS, boolean writeBody)
    {
    	writeBody_ = writeBody;

        sb_.append("<html>\n");
        if(writeCSS || writeJSCallback)
        {
	        sb_.append("<head>\n");
	        if(writeJSCallback)
	        	addJSCallback(jsCallbackFunction);
	        if(writeCSS)
	        	addCSS();
	        sb_.append("</head>\n");
        }

        if(writeBody)
        {
        	sb_.append("<body bgcolor=black>\n");
        }
    }

    public void endBody()
    {
    	if(writeBody_)
    	{
    		sb_.append("</body>\n");
    	}
        sb_.append("</html>\n");
    }

    public void appendLine(String s)
    {
        sb_.append(s);
        sb_.append("<br>\n");
    }

    public void append(String s)
    {
        sb_.append(s);
    }

    public void addJScript(String jscript)
    {
    	append("<script language=\"text/javascript\">\n");
    	append(jscript + "\n");
    	append("</script>\n");
    }

    public void startTable()
    {
        sb_.append("<table>\n");
    }

    public void addHeaders(String[] sTableHeaders)
    {
        sb_.append("<tr style=\"border: 2px solid #333333\"	>\n");
        for (int i = 0; i < sTableHeaders.length; ++i)
        {
            sb_.append("<th><div class=\"tmenubar\">");
            sb_.append("<b>" + sTableHeaders[i] + "</b>");
            sb_.append("</div></th>\n");
        }
        sb_.append("\n</tr>\n\n");
    }

    public void addHeader(String sTableHeader)
    {
        sb_.append("<tr style=\"border: 2px solid #333333\"	>\n");
        sb_.append("<th><div class=\"tmenubar\">");
        sb_.append("<b>" + sTableHeader + "</b>");
        sb_.append("</div></th>\n");
        sb_.append("\n</tr>\n\n");
    }

    public void startRow()
    {
        sb_.append("<tr style=\"border: 2px solid #333333\">\n");
    }

    public void addCol(String sData)
    {
        sb_.append("<td style=\"border: 2px solid #333333\">");
        sb_.append(sData);
        sb_.append("</td>");
    }

    public void endRow()
    {
        sb_.append("</tr>\n");
    }

    public void endTable()
    {
        sb_.append("</table>\n");
    }

    public void addCombobox(Set<String> comboBoxEntries, String htmlElementName)
    {
    	addCombobox(comboBoxEntries, htmlElementName, -1);
    }

    public void addCombobox(Set<String> comboBoxEntries, String htmlElementName, int defaultSelected)
    {
    	sb_.append("  <select name=" + htmlElementName + " size=1>\n");
    	if(defaultSelected == -1)
    	{
    		sb_.append("    <option value=\"\" SELECTED>Select an option \n");
    	}

    	int i = 0;
    	for(String colFamName : comboBoxEntries)
    	{
    		if(defaultSelected == i)
    		{
    			sb_.append("    <option value=\"" + colFamName + "\" SELECTED>" + colFamName + "\n");
    		}
    		else
    		{
    			sb_.append("    <option value=\"" + colFamName + "\">" + colFamName + "\n");
    		}
    	}
    	sb_.append("  </select>\n");
    }

    public void addDivElement(String divId, String value)
    {
    	sb_.append("<div id = \"" + divId + "\">");
    	if(value != null)
    		sb_.append(value);
    	sb_.append("</div>\n");
    }

    public void createTable(String[] sTableHeaders, String[][] sTable)
    {
        if (sTable == null || sTable.length == 0)
            return;

        sb_.append("<table style=\"border: 2px solid #333333\">\n");

        sb_.append("<tr style=\"border: 2px solid #333333\">\n");
        for (int i = 0; i < sTableHeaders.length; ++i)
        {
            sb_.append("<td style=\"border: 2px solid #333333\">");
            sb_.append("<b>" + sTableHeaders[i] + "</b>");
            sb_.append("</td>\n");
        }
        sb_.append("\n</tr>\n\n");

        for (int i = 0; i < sTable.length; ++i)
        {
            sb_.append("<tr style=\"border: 2px solid #333333\">\n");
            for (int j = 0; j < sTable[i].length; ++j)
            {
                sb_.append("<td style=\"border: 2px solid #333333\">");
                sb_.append(sTable[i][j]);
                sb_.append("</td>\n");
            }
            sb_.append("\n</tr>\n\n");
        }
        sb_.append("</table>\n");
    }

    public void addJSCallback(String jsCallbackFunction)
    {
    	sb_.append("<script type=\"text/javascript\">\n");

    	addJSForTabs();

    	sb_.append(jsCallbackFunction +"\n");
    	sb_.append("</script>\n");
    }

    public void addCSS()
    {
        sb_.append("<style type=\"text/css\">\n");
        sb_.append("body\n");
        sb_.append("{\n");
        sb_.append("  color:white;\n");
        sb_.append("  font-family:Arial Unicode MS,Verdana, Arial, Sans-serif;\n");
        sb_.append("  font-size:10pt;\n");
        sb_.append("}\n");

        sb_.append(".tmenubar\n");
        sb_.append("{\n");
        sb_.append("  background-color:green;\n");
        sb_.append("  font-family:Verdana, Arial, Sans-serif;\n");
        sb_.append("  font-size:10pt;\n");
        sb_.append("  font-weight:bold;\n");
        sb_.append("}\n");

        sb_.append("th\n");
        sb_.append("{\n");
        sb_.append(" 	 color:white;\n");
        sb_.append("}\n");

        sb_.append("td\n");
        sb_.append("{\n");
        sb_.append(" 	 color:white;\n");
        sb_.append("}\n");
        sb_.append("a:link {color:#CAF99B;font-size:10pt;font-weight:bold;font-family:Arial Unicode MS,Lucida-grande,Verdana}\n");
        sb_.append("a:visited {color:red}\n");
        sb_.append("a:hover{color:yellow;font-size:10pt;font-weight:bold;font-family:Arial Unicode MS,Lucida-grande,Verdana;background-color:green}\n");

        addCSSForTabs();

        sb_.append("</style>\n");

    }

    public void addCSSForTabs()
    {
    	sb_.append("#header ul {\n");
    	sb_.append("	list-style: none;\n");
    	sb_.append("	padding: 0;\n");
    	sb_.append("	margin: 0;\n");
    	sb_.append("	}\n");
    	sb_.append("\n");
    	sb_.append("#header li {\n");
    	sb_.append("	float: left;\n");
    	sb_.append("	border: 1px solid #bbb;\n");
    	sb_.append("	border-bottom-width: 0;\n");
    	sb_.append("	margin: 0;\n");
    	sb_.append("}\n");
    	sb_.append("\n");
    	sb_.append("#header a {\n");
    	sb_.append("	text-decoration: none;\n");
    	sb_.append("	display: block;\n");
    	sb_.append("	background: #eee;\n");
    	sb_.append("	padding: 0.24em 1em;\n");
    	sb_.append("	color: #00c;\n");
    	sb_.append("	width: 8em;\n");
    	sb_.append("	text-align: center;\n");
    	sb_.append("	}\n");
    	sb_.append("\n");
    	sb_.append("#header a:hover {\n");
    	sb_.append("	background: #ddf;\n");
    	sb_.append("}\n");
    	sb_.append("\n");
    	sb_.append("#header #selected {\n");
    	sb_.append("	border-color: black;\n");
    	sb_.append("}\n");
    	sb_.append("\n");
    	sb_.append("#header #selected a {\n");
    	sb_.append("	position: relative;\n");
    	sb_.append("	top: 1px;\n");
    	sb_.append("	background: white;\n");
    	sb_.append("	color: black;\n");
    	sb_.append("	font-weight: bold;\n");
    	sb_.append("}\n");
    	sb_.append("\n");
    	sb_.append("#content {\n");
    	sb_.append("	border: 1px solid black;\n");
    	sb_.append("	visibility:hidden;\n");
    	sb_.append("	position:absolute;\n");
    	sb_.append("	top:200;\n");
    	sb_.append("	clear: both;\n");
    	sb_.append("	padding: 0 1em;\n");
    	sb_.append("}\n");
    	sb_.append("\n");
    	sb_.append("h1 {\n");
    	sb_.append("	margin: 0;\n");
    	sb_.append("	padding: 0 0 1em 0;\n");
    	sb_.append("}\n");
    }

    public void addJSForTabs()
    {
    	sb_.append("var curSelectedDivId = \"one\";\n");
    	sb_.append("\n");
    	sb_.append("function selectTab(tabDivId)\n");
    	sb_.append("{\n");
    	sb_.append("	var x = document.getElementsByName(curSelectedDivId);\n");
    	sb_.append("	if(x[1])\n");
    	sb_.append("		x[1].style.visibility=\"hidden\";\n");
    	sb_.append("	if(x[0])\n");
    	sb_.append("		x[0].id=curSelectedDivId;\n");
    	sb_.append("\n");
    	sb_.append("\n");
    	sb_.append("	var y = document.getElementsByName(tabDivId);\n");
    	sb_.append("	if(y[1])\n");
    	sb_.append("		y[1].style.visibility=\"visible\";\n");
    	sb_.append("	if(y[0])\n");
    	sb_.append("		y[0].id = \"selected\";\n");
    	sb_.append("\n");
    	sb_.append("	curSelectedDivId = tabDivId;\n");
    	sb_.append("}\n");
    }

    public String toString()
    {
        return sb_.toString();
    }
}
