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

package org.apache.cassandra.contrib.circuit;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextPane;

public class AboutDialog extends JDialog
{
    private static final long serialVersionUID = 1L;

    public AboutDialog(JFrame parent)
    {
        super(parent, "About " + parent.getTitle(), true);

        Box vbox = Box.createVerticalBox();
        vbox.add(Box.createGlue());
        JTextPane textPane = new JTextPane();
        textPane.setContentType("text/html");
        textPane.setEditable(false);
        textPane.setText(getHtmlMarkup());
        vbox.add(textPane);
        vbox.add(Box.createGlue());
        getContentPane().add(vbox, "Center");

        JPanel bottomPanel = new JPanel();
        JButton closeButton = new JButton("Close");
        bottomPanel.add(closeButton);
        getContentPane().add(bottomPanel, "South");

        closeButton.addActionListener(new ActionListener()
        {
          public void actionPerformed(ActionEvent evt)
          {
            setVisible(false);
          }
        });

        setSize(350, 220);
        setResizable(false);
        setModalityType(ModalityType.APPLICATION_MODAL);
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        setLocationRelativeTo(parent);
    }
    
    public static String getHtmlMarkup()
    {
        return String.format("<html><body bgcolor=#aaaab9><center><br>" +
                "<font size=+2><b>Circuit</b></font><br><br>" +
                "Visualization and diagnostics for Cassandra clusters.<br><br>" +
                "<font size=-2 color=#333355>&copy; 2009 The Apache Software Foundation</font>" +
                "</center></body></html>");
    }

    public static void main(String[] args)
    {
        JDialog f = new AboutDialog(new JFrame("Phony"));
        f.setVisible(true);
    }
}
