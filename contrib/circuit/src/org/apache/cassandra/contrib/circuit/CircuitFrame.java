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

import java.awt.Component;
import java.awt.Dimension;
import java.awt.Event;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.KeyStroke;
import javax.swing.SwingUtilities;
import com.google.common.collect.Sets;

public class CircuitFrame extends JFrame implements ActionListener
{
    private static final long serialVersionUID = 1L;
    private static final String appTitle = "Circuit";
    private static final Dimension defaultSize = new Dimension(550, 600);
    private static final SimpleDateFormat dateFormatter;
    private static final Lock verifyLock = new ReentrantLock();
    
    private RingModel ringModel;
    private RingPanel ringPanel;
    private JTextArea statusOutput;
    
    private JMenuBar menuBar;
    private JMenuItem quitMI, verifyMI, aboutMI;
    
    static
    {
        dateFormatter = new SimpleDateFormat("HH:mm:ss");
    }
    
    public CircuitFrame(String hostname, int port)
    {
        super(appTitle);
        setSize(defaultSize);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        
        // The menu bar w/ items.
        menuBar = new JMenuBar();
        setJMenuBar(menuBar);
        
        JMenu fileMenu = new JMenu("File");
        fileMenu.setMnemonic(KeyEvent.VK_F);
        menuBar.add(fileMenu);
        
        quitMI = new JMenuItem("Quit");
        quitMI.setMnemonic(KeyEvent.VK_Q);
        quitMI.setAccelerator(
                KeyStroke.getKeyStroke(KeyEvent.VK_Q, Event.CTRL_MASK));
        quitMI.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) { System.exit(0); }
        });
        fileMenu.add(quitMI);
        
        JMenu toolsMenu = new JMenu("Tools");
        toolsMenu.setMnemonic(KeyEvent.VK_T);
        menuBar.add(toolsMenu);
        
        verifyMI = new JMenuItem("Verify Ring");
        verifyMI.addActionListener(this);
        toolsMenu.add(verifyMI);
        
        JMenu helpMenu = new JMenu("Help");
        helpMenu.setMnemonic(KeyEvent.VK_H);
        menuBar.add(helpMenu);
        
        aboutMI = new JMenuItem("About");
        aboutMI.setMnemonic(KeyEvent.VK_A);
        aboutMI.addActionListener(this);
        helpMenu.add(aboutMI);
        
        // FIXME: a progress dialog should be up while instantiating RingPanel
        ringModel = new RingModel(hostname, port);
        ringPanel = new RingPanel(ringModel);

        statusOutput = new JTextArea();
        statusOutput.setEditable(false);
        Component logPanel = new JScrollPane(statusOutput);
        
        JSplitPane contentPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, ringPanel, logPanel);
        setContentPane(contentPane);

        // Order matters here...
        ringPanel.setPreferredSize(getSize());
        setVisible(true);
        contentPane.setDividerLocation(0.8);
    }

    public void actionPerformed(ActionEvent e)
    {
        Object src = e.getSource();
        
        if (src == verifyMI)
        {
            verifyRing();
        }
        else if(src == aboutMI)
        {
            new AboutDialog(this).setVisible(true);
        }
    }

    /**
     * For each node, retrieve that nodes list and compare it to ours. If the
     * list of remote nodes doesn't match (it's long or short), then the node is
     * flagged accordingly and an error message is written to the status display.
     */
    private void verifyRing()
    {
        new Thread("VERIFY-RING")
        {
            public void run()
            {
                verifyLock.lock();
                ringPanel.setVerifying(true);
                try {
                    writeStatusOutput("Beginning ring verification...");
                    for (Node node : ringModel.getNodes())
                    {
                        // Skip the node we already queried at startup
                        if (node.isSeed())
                            continue;

                        writeStatusOutput("Verifying %s (ring) against reference node", node);
                        node.setSelected(true);
                        
                        SwingUtilities.invokeLater(new Runnable() {
                            public void run() {
                                ringPanel.repaint();
                            }
                        });

                        // uncomment this to simulate a slow running verification process
//                        try {Thread.currentThread().sleep(2000L); } catch (Exception ex) { }

                        Set<Node> othersSet, nodesSet;
                        try {
                            othersSet = new HashSet<Node>(ringModel.getRemoteNodes(node.getHost()));
                        } catch (IOException e) {
                            e.printStackTrace();
                            writeStatusOutput("Error retrieving node list from %s", node.getHost());
                            continue;
                        }

                        nodesSet = new HashSet<Node>(ringModel.getNodes());

                        for (Node upShort : Sets.difference(nodesSet, othersSet))
                        {
                            node.setStatus(NodeStatus.SHORT);
                            writeStatusOutput("%s: missing node %s", node, upShort);
                        }

                        for (Node upLong : Sets.difference(othersSet, nodesSet))
                        {
                            node.setStatus(NodeStatus.LONG);
                            writeStatusOutput("%s: contains node %s missing from reference list", node, upLong);
                        }

                        node.setSelected(false);
                    }

                    SwingUtilities.invokeLater(new Runnable() {
                        public void run() {
                            ringPanel.repaint();
                        }
                    });
                    writeStatusOutput("Ring verification complete.");
                } finally
                {
                    verifyLock.unlock();
                    ringPanel.setVerifying(false);
                }
            }
        }.start();
    }
    
    // TODO: use StatusLevel to distinguish message priorities.
    private void writeStatusOutput(String msg, StatusLevel level, Object...args)
    {
        String pref = String.format("[%s] ", dateFormatter.format(new Date()));
        statusOutput.append(String.format(pref + msg + "\n", args));
        statusOutput.setCaretPosition(statusOutput.getDocument().getLength());
    }
    
    private void writeStatusOutput(String msg, Object...args)
    {
        writeStatusOutput(msg, StatusLevel.INFO, args);
    }
    
    public static void main(final String[] args) throws IOException
    {
        if (args.length != 2)
        {
            System.err.println("Usage: java " + CircuitFrame.class.getName() + " <host> <port>");
            System.exit(1);
        }
        try
        {
            SwingUtilities.invokeAndWait(new Runnable() { public void run() {
                CircuitFrame app = new CircuitFrame(args[0], Integer.parseInt(args[1]));
                app.setVisible(true);
            }});
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}

enum StatusLevel
{
    INFO,
    WARN,
    ERROR,
    CRITICAL,
    DEBUG,
}
