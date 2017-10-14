package com.google.firebase.database.tyrus;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import com.google.firebase.database.connection.HostInfo;
import com.google.firebase.database.connection.RequestResultCallback;
import com.google.firebase.database.core.AuthTokenProvider;
import com.google.firebase.database.util.GAuthToken;
import com.google.firebase.database.util.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
import org.json.JSONObject;


class TyrusWebsocket extends Endpoint implements MessageHandler.Whole<String> {

  private final ClientManager client;
  private final HostInfo hostInfo;
  private final Delegate delegate;
  private final AuthTokenProvider authProvider;

  private final AtomicLong requestCounter = new AtomicLong(0);
  private final Map<Long, OutgoingMessage> outgoingMessages = new ConcurrentHashMap<>();

  private State state = State.READY;
  private Session session;
  private String cachedHost;
  private String lastSessionId;

  TyrusWebsocket(HostInfo hostInfo, Delegate delegate, AuthTokenProvider authProvider) {
    ClientManager client = ClientManager.createClient();
    ThreadPoolConfig config = ThreadPoolConfig.defaultConfig()
        .setCorePoolSize(0)
        .setMaxPoolSize(3)
        .setDaemon(true)
        .setPoolName("hkj-websocket");
    client.getProperties().put(ClientProperties.WORKER_THREAD_POOL_CONFIG, config);
    this.client = client;
    this.hostInfo = checkNotNull(hostInfo);
    this.delegate = checkNotNull(delegate);
    this.authProvider = checkNotNull(authProvider);
  }

  synchronized void open() throws IOException, DeploymentException {
    checkState(state == State.READY);
    String host = cachedHost != null ? cachedHost : hostInfo.getHost();
    URI uri = HostInfo.getConnectionUrl(
        host, hostInfo.isSecure(), hostInfo.getNamespace(), lastSessionId);
    ClientEndpointConfig endpointConfig = ClientEndpointConfig.Builder.create().build();
    client.connectToServer(this, endpointConfig, uri);
  }

  public synchronized void close() throws IOException {
    checkState(state != State.READY);
    try {
      session.close();
    } finally {
      session = null;
      state = State.READY;
    }
  }

  @Override
  public void onOpen(Session session, EndpointConfig endpointConfig) {
    state = State.OPEN;
    this.session = session;
    this.lastSessionId = session.getId();
    session.addMessageHandler(this);
  }

  @Override
  public void onClose(Session session, CloseReason closeReason) {
    super.onClose(session, closeReason);
  }

  @Override
  public void onError(Session session, Throwable thr) {
    super.onError(session, thr);
  }

  @Override
  public void onMessage(String data) {
    System.out.println("State: " + state + "; Incoming: " + data);
    Message message;
    try {
      message = parse(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (message instanceof Message.AckMessage) {
      Message.AckMessage ack = (Message.AckMessage) message;
      OutgoingMessage outgoingMessage = outgoingMessages.remove(ack.getRequestNumber());
      checkState(outgoingMessage != null);
      if (state == State.AUTHENTICATING) {
        if (ack.getData("s", String.class).equals("ok")) {
          state = State.AUTHENTICATED;
          System.out.println("Auth accepted");
          delegate.onAuth(true);
        } else {
          System.out.println("Auth not accepted");
          delegate.onAuth(false);
        }
      } else {
        outgoingMessage.onResponse(ack, delegate);
      }

    } else if (message.getType().equals("control:h")) {
      checkState(state == State.OPEN);
      cachedHost = ((Message.ControlMessage) message).getData("h", String.class);
      delegate.onConnect();

      Map<String, Object> stats = ImmutableMap.<String, Object>of(
          "c", ImmutableMap.of("sdk.admin_java.5-4-1-SNAPSHOT", 1));
      sendAsync(new OutgoingMessage.StatsMessage(stats, requestCounter.getAndIncrement()));

      Map<String, Object> request = new HashMap<>();
      String token = getToken();
      GAuthToken googleAuthToken = GAuthToken.tryParseFromString(token);
      if (googleAuthToken != null) {
        request.put("cred", googleAuthToken.getToken());
      } else {
        request.put("cred", token);
      }
      state = State.AUTHENTICATING;
      sendAsync(new OutgoingMessage.AuthMessage(request, requestCounter.getAndIncrement()));
    }
  }

  private String getToken() {
    final Semaphore semaphore = new Semaphore(0);
    final AtomicReference<String> result = new AtomicReference<>();
    final AtomicBoolean errorFlag = new AtomicBoolean(false);

    authProvider.getToken(false, new AuthTokenProvider.GetTokenCompletionListener() {
      @Override
      public void onSuccess(String token) {
        result.set(token);
        semaphore.release();
      }

      @Override
      public void onError(String error) {
        errorFlag.set(true);
        result.set(error);
        semaphore.release();
      }
    });

    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    if (errorFlag.get()) {
      throw new RuntimeException(result.get());
    }
    return result.get();
  }

  void sendQuery(Map<String, Object> data, RequestResultCallback callback) {
    if (state == State.AUTHENTICATED) {
      sendAsync(new OutgoingMessage.QueryMessage(data, callback, requestCounter.getAndIncrement()));
    }
  }

  private void sendAsync(OutgoingMessage message) {
    String data;
    try {
      data = JsonMapper.serializeJson(message.toJson());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    System.out.println("State: " + state + "; Outgoing: " + data);
    outgoingMessages.put(message.getRequestNumber(), message);
    session.getAsyncRemote().sendText(data);
  }

  private static Message parse(String data) throws IOException {
    JSONObject parsed = new JSONObject(data);
    if ("c".equals(parsed.get("t"))) {
      return new Message.ControlMessage(parsed.getJSONObject("d").toMap());
    } else if ("d".equals(parsed.get("t"))) {
      JSONObject nestedData = parsed.getJSONObject("d");
      if (nestedData.has("r")) {
        long requestNumber = nestedData.getLong("r");
        return new Message.AckMessage(requestNumber, nestedData.getJSONObject("b").toMap());
      }
    }
    throw new RuntimeException("Unsupported message: " + data);
  }

  enum State {
    READY,
    OPEN,
    AUTHENTICATING,
    AUTHENTICATED
  }

  interface Delegate {
    void onConnect();

    void onAuth(boolean status);

    void onQuery(boolean status, RequestResultCallback callback);
  }
}
