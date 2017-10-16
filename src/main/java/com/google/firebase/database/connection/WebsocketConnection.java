/*
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.firebase.database.connection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.firebase.database.logging.LogWrapper;
import com.google.firebase.database.util.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.websocket.ClientEndpointConfig;
import javax.websocket.CloseReason;
import javax.websocket.DeploymentException;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import org.glassfish.tyrus.client.ClientManager;
import org.glassfish.tyrus.client.ClientProperties;
import org.glassfish.tyrus.client.ThreadPoolConfig;

class WebsocketConnection {

  private static final long KEEP_ALIVE_TIMEOUT_MS = 45 * 1000; // 45 seconds
  private static final long CONNECT_TIMEOUT_MS = 30 * 1000; // 30 seconds
  private static final AtomicLong connectionId = new AtomicLong(0);

  private final ConnectionContext connectionContext;
  private final ScheduledExecutorService executorService;
  private final LogWrapper logger;
  private final Delegate delegate;
  private final WSClient conn;

  private boolean everConnected = false;
  private boolean isClosed = false;
  private ScheduledFuture<?> keepAlive;
  private ScheduledFuture<?> connectTimeout;

  public WebsocketConnection(
      ConnectionContext connectionContext,
      HostInfo hostInfo,
      String optCachedHost,
      Delegate delegate,
      String optLastSessionId) {
    this.connectionContext = connectionContext;
    this.executorService = connectionContext.getExecutorService();
    this.delegate = delegate;
    long connId = connectionId.getAndIncrement();
    logger = new LogWrapper(connectionContext.getLogger(), WebsocketConnection.class,
        "ws_" + connId);
    conn = createConnection(hostInfo, optCachedHost, optLastSessionId);
  }

  private WSClient createConnection(
      HostInfo hostInfo, String optCachedHost, String optLastSessionId) {
    String host = (optCachedHost != null) ? optCachedHost : hostInfo.getHost();
    URI uri = HostInfo.getConnectionUrl(
        host, hostInfo.isSecure(), hostInfo.getNamespace(), optLastSessionId);
    return new WSEndpoint(uri, ImmutableMap.of("User-Agent", connectionContext.getUserAgent()));
  }

  void open() {
    connectTimeout =
        executorService.schedule(
            new Runnable() {
              @Override
              public void run() {
                closeIfNeverConnected();
              }
            },
            CONNECT_TIMEOUT_MS,
            TimeUnit.MILLISECONDS);
    conn.connect();
  }

  public void start() {
    // No-op in java
  }

  void close() {
    if (logger.logsDebug()) {
      logger.debug("websocket is being closed");
    }
    isClosed = true;
    // Although true is passed for both of these, they each run on the same event loop, so
    // they will
    // never be running.
    conn.close();
    if (connectTimeout != null) {
      connectTimeout.cancel(true);
    }
    if (keepAlive != null) {
      keepAlive.cancel(true);
    }
  }

  public void send(Map<String, Object> message) {
    resetKeepAlive();

    try {
      String toSend = JsonMapper.serializeJson(message);
      conn.send(toSend);
    } catch (IOException e) {
      logger.error("Failed to serialize message: " + message.toString(), e);
      shutdown();
    }
  }

  private void resetKeepAlive() {
    if (!isClosed) {
      if (keepAlive != null) {
        keepAlive.cancel(false);
        if (logger.logsDebug()) {
          logger.debug("Reset keepAlive. Remaining: " + keepAlive.getDelay(TimeUnit.MILLISECONDS));
        }
      } else {
        if (logger.logsDebug()) {
          logger.debug("Reset keepAlive");
        }
      }
      keepAlive = executorService.schedule(nop(), KEEP_ALIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
  }

  private Runnable nop() {
    return new Runnable() {
      @Override
      public void run() {
        conn.send("0");
        resetKeepAlive();
      }
    };
  }

  private void onClosed() {
    if (!isClosed) {
      if (logger.logsDebug()) {
        logger.debug("closing itself");
      }
      shutdown();
    }
    if (keepAlive != null) {
      keepAlive.cancel(false);
    }
  }

  private void shutdown() {
    isClosed = true;
    delegate.onDisconnect(everConnected);
  }

  // Close methods

  private void closeIfNeverConnected() {
    if (!everConnected && !isClosed) {
      if (logger.logsDebug()) {
        logger.debug("timed out on connect");
      }
      conn.close();
    }
  }

  public interface Delegate {

    void onMessage(Map<String, Object> message);

    void onDisconnect(boolean wasEverConnected);
  }

  private interface WSClient {

    void connect();

    void close();

    void send(String msg);
  }

  private class WSEndpoint extends Endpoint implements WSClient, MessageHandler.Whole<String> {

    private final URI uri;
    private final Map<String, String> extraHeaders;

    private final ClientManager client;
    private Session session;

    WSEndpoint(URI uri, Map<String, String> headers) {
      this.uri = uri;
      this.extraHeaders = headers;
      ClientManager client = ClientManager.createClient();
      ThreadPoolConfig config = ThreadPoolConfig.defaultConfig()
          .setCorePoolSize(0)
          .setMaxPoolSize(3)
          .setDaemon(true)
          .setThreadFactory(connectionContext.getThreadFactory())
          .setPoolName("hkj-websocket");
      client.getProperties().put(ClientProperties.WORKER_THREAD_POOL_CONFIG, config);
      this.client = client;
    }

    @Override
    public void onOpen(Session session, EndpointConfig endpointConfig) {
      this.session = session;
      connectTimeout.cancel(false);
      everConnected = true;
      if (logger.logsDebug()) {
        logger.debug("websocket opened");
      }
      resetKeepAlive();
      session.addMessageHandler(this);
    }

    @Override
    public void onMessage(String message) {
      try {
        resetKeepAlive();
        Map<String, Object> decoded = JsonMapper.parseJson(message);
        if (logger.logsDebug()) {
          logger.debug("handleIncomingFrame complete frame: " + decoded);
        }
        delegate.onMessage(decoded);
      } catch (IOException e) {
        logger.error("Error parsing frame: " + message, e);
        close();
        shutdown();
      } catch (ClassCastException e) {
        logger.error("Error parsing frame (cast error): " + message, e);
        close();
        shutdown();
      }
    }

    @Override
    public void onClose(Session session, CloseReason closeReason) {
      if (logger.logsDebug()) {
        logger.debug("closed");
      }
      onClosed();
    }

    @Override
    public void onError(Session session, Throwable e) {
      logger.debug("WebSocket error.", e);
      onClosed();
    }

    @Override
    public void connect() {
      ClientEndpointConfig endpointConfig = ClientEndpointConfig.Builder.create()
          .configurator(new ClientEndpointConfig.Configurator() {
            @Override
            public void beforeRequest(Map<String, List<String>> headers) {
              super.beforeRequest(headers);
              for (Map.Entry<String, String> entry : extraHeaders.entrySet()) {
                headers.put(entry.getKey(), ImmutableList.of(entry.getValue()));
              }
            }
          })
          .build();
      try {
        client.connectToServer(this, endpointConfig, uri);
      } catch (DeploymentException | IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      try {
        if (session != null) {
          session.close();
        }
        client.shutdown();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void send(String msg) {
      session.getAsyncRemote().sendText(msg);
    }
  }
}
