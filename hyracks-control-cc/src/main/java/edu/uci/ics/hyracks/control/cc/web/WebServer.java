/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.cc.web;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;

public class WebServer {
    private final Server server;
    private final SelectChannelConnector connector;
    private final HandlerCollection handlerCollection;

    public WebServer() throws Exception {
        server = new Server();

        connector = new SelectChannelConnector();

        server.setConnectors(new Connector[] { connector });

        handlerCollection = new ContextHandlerCollection();
        server.setHandler(handlerCollection);
    }

    public void setPort(int port) {
        connector.setPort(port);
    }

    public int getListeningPort() {
        return connector.getLocalPort();
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
    }

    public void addHandler(Handler handler) {
        handlerCollection.addHandler(handler);
    }
}