package com.google.firebase.database.tyrus;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.firebase.database.connection.HostInfo;
import com.google.firebase.database.connection.ListenHashProvider;
import com.google.firebase.database.connection.PersistentConnection;
import com.google.firebase.database.connection.RequestResultCallback;
import com.google.firebase.database.core.AuthTokenProvider;
import com.google.firebase.database.core.Repo;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.websocket.ClientEndpointConfig;
import javax.websocket.DeploymentException;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import org.glassfish.tyrus.client.ClientManager;
import org.glassfish.tyrus.client.ClientProperties;
import org.glassfish.tyrus.client.ThreadPoolConfig;


public class TyrusPersistentConnection extends Endpoint
    implements PersistentConnection, MessageHandler.Whole<String> {

  private final HostInfo hostInfo;
  private final PersistentConnection.Delegate delegate;
  private final AuthTokenProvider authProvider;

  private final ClientManager client;
  private final AtomicLong requestCounter = new AtomicLong(0);
  private final Map<Long, OutgoingMessage> outbox = new ConcurrentHashMap<>();
  private final Map<ListenQuerySpec, OutgoingMessage.Query> listens = new ConcurrentHashMap<>();
  private final Map<Long, OutgoingMessage.Put> writes = new ConcurrentHashMap<>();

  private State state = State.READY;
  private String cachedHost;
  private String lastSessionId;
  private Session session;

  public TyrusPersistentConnection(Repo repo, HostInfo hostInfo, AuthTokenProvider authProvider) {
    this.delegate = checkNotNull(repo);
    this.hostInfo = checkNotNull(hostInfo);
    this.authProvider = checkNotNull(authProvider);

    ClientManager client = ClientManager.createClient();
    ThreadPoolConfig config = ThreadPoolConfig.defaultConfig()
        .setCorePoolSize(0)
        .setMaxPoolSize(3)
        .setDaemon(true)
        .setPoolName("hkj-websocket");
    client.getProperties().put(ClientProperties.WORKER_THREAD_POOL_CONFIG, config);
    this.client = client;
  }

  @Override
  public void onOpen(Session session, EndpointConfig endpointConfig) {
    checkState(state == State.CONNECTING);
    state = State.CONNECTED;
    this.session = session;
    this.lastSessionId = session.getId();
    session.addMessageHandler(this);
  }

  private void sendAsync(OutgoingMessage message) {
    String data = message.toJson();
    System.out.println("State: " + state + "; Outgoing: " + data);
    outbox.put(message.getRequestNumber(), message);
    session.getAsyncRemote().sendText(data);
  }

  private void handleConnectedState(Message message) {
    checkState(message instanceof Message.Control);
    Message.Control controlMessage = (Message.Control) message;
    checkState(controlMessage.getType().equals("h"));
    cachedHost = controlMessage.getDataField("h", String.class);
    delegate.onConnect();

    state = State.AUTHENTICATING;
    sendAsync(new OutgoingMessage.Stats(requestCounter.getAndIncrement()));
    String token = AuthUtils.getToken(authProvider);
    sendAsync(new OutgoingMessage.Auth(token, requestCounter.getAndIncrement()));
  }

  private void handleAuthenticatingState(Message message) {
    checkArgument(message instanceof Message.Ack);
    OutgoingMessage request = resolve((Message.Ack) message);
    if (request instanceof OutgoingMessage.Auth) {
      if (message.getDataField("s", String.class).equals("ok")) {
        state = State.AUTHENTICATED;
        // Restore state
        for (OutgoingMessage.Query listen : listens.values()) {
          sendAsync(listen);
        }

        // Restore puts
        List<Long> outstanding = new ArrayList<>(writes.keySet());
        // Make sure puts are restored in order
        Collections.sort(outstanding);
        for (Long put : outstanding) {
          sendAsync(writes.get(put));
        }
      }
    }
  }

  private void handleAuthenticatedState(Message message) {
    if (message instanceof Message.Ack) {
      Message.Ack ack = (Message.Ack) message;
      OutgoingMessage request = resolve(ack);
      if (request instanceof OutgoingMessage.Put) {
        System.out.println("Write complete");
        writes.remove(request.getRequestNumber());
      }
    } else if (message instanceof Message.DataMessage) {
      Message.DataMessage dataMessage = (Message.DataMessage) message;
      dataMessage.onComplete(delegate);
    }
  }

  private OutgoingMessage resolve(Message.Ack ack) {
    OutgoingMessage request = outbox.remove(ack.getRequestNumber());
    checkNotNull(request);
    request.onComplete(delegate, ack);
    return request;
  }

  @Override
  public void onMessage(String data) {
    System.out.println("wire >> " + data);
    Message message = Message.parse(data);
    switch (state) {
      case CONNECTED:
        handleConnectedState(message);
        break;

      case AUTHENTICATING:
        handleAuthenticatingState(message);
        break;

      case AUTHENTICATED:
        handleAuthenticatedState(message);
        break;

      default:
        System.out.println("Unsupported state: " + state);
        throw new RuntimeException();
    }
  }

  @Override
  public void initialize() {
    checkState(state == State.READY);
    String host = cachedHost != null ? cachedHost : hostInfo.getHost();
    URI uri = HostInfo.getConnectionUrl(
        host, hostInfo.isSecure(), hostInfo.getNamespace(), lastSessionId);
    ClientEndpointConfig endpointConfig = ClientEndpointConfig.Builder.create().build();

    try {
      state = State.CONNECTING;
      client.connectToServer(this, endpointConfig, uri);
    } catch (DeploymentException | IOException e) {
      state = State.READY;
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {

  }

  @Override
  public void refreshAuthToken() {

  }

  @Override
  public void refreshAuthToken(String token) {

  }

  @Override
  public void listen(
      List<String> path, Map<String, Object> queryParams,
      ListenHashProvider currentHashFn, Long tag, RequestResultCallback callback) {

    ListenQuerySpec query = new ListenQuerySpec(path, queryParams);
    checkState(!listens.containsKey(query), "listen() called twice for same QuerySpec.");
    OutgoingMessage.Query queryMessage = new OutgoingMessage.Query(callback, query,
        currentHashFn, tag, requestCounter.getAndIncrement());
    listens.put(query, queryMessage);
    if (state == State.AUTHENTICATED) {
      sendAsync(queryMessage);
    }
  }

  @Override
  public void unlisten(List<String> path, Map<String, Object> queryParams) {

  }

  @Override
  public void purgeOutstandingWrites() {

  }

  @Override
  public void put(List<String> path, Object data, RequestResultCallback onComplete) {
    OutgoingMessage.Put put = new OutgoingMessage.Put(
        path, data, onComplete, requestCounter.getAndIncrement());
    writes.put(put.getRequestNumber(), put);
    if (state == State.AUTHENTICATED) {
      sendAsync(put);
    }
  }

  @Override
  public void compareAndPut(List<String> path, Object data, String hash, RequestResultCallback
      onComplete) {

  }

  @Override
  public void merge(List<String> path, Map<String, Object> data, RequestResultCallback onComplete) {

  }

  @Override
  public void onDisconnectPut(List<String> path, Object data, RequestResultCallback onComplete) {

  }

  @Override
  public void onDisconnectMerge(List<String> path, Map<String, Object> updates,
                                RequestResultCallback onComplete) {

  }

  @Override
  public void onDisconnectCancel(List<String> path, RequestResultCallback onComplete) {

  }

  @Override
  public void interrupt(String reason) {

  }

  @Override
  public void resume(String reason) {

  }

  @Override
  public boolean isInterrupted(String reason) {
    return false;
  }

  enum State {
    READY,
    CONNECTING,
    CONNECTED,
    AUTHENTICATING,
    AUTHENTICATED
  }
}
