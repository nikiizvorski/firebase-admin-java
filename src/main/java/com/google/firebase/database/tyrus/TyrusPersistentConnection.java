package com.google.firebase.database.tyrus;

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
  private final Map<Long, Messages.OutgoingMessage> outbox = new ConcurrentHashMap<>();
  private final Map<ListenQuerySpec, Messages.QueryMessage> listens = new ConcurrentHashMap<>();

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

  private void sendAsync(Messages.OutgoingMessage message) {
    String data = message.toJson();
    System.out.println("State: " + state + "; Outgoing: " + data);
    outbox.put(message.getRequestNumber(), message);
    session.getAsyncRemote().sendText(data);
  }

  private void handleConnectedState(Message message) {
    checkState(message instanceof Messages.ControlMessage);
    Messages.ControlMessage controlMessage = (Messages.ControlMessage) message;
    checkState(controlMessage.getType().equals("h"));
    cachedHost = controlMessage.getData("h", String.class);
    delegate.onConnect();

    state = State.AUTHENTICATING;
    sendAsync(new Messages.StatsMessage(requestCounter.getAndIncrement()));

    String token = AuthUtils.getToken(authProvider);
    Messages.MessageCallback oncomplete = new Messages.MessageCallback() {
      @Override
      public void onComplete(Messages.AckMessage ack) {
        if (ack.getData("s", String.class).equals("ok")) {
          System.out.println("Auth successful");
          state = State.AUTHENTICATED;
          delegate.onAuthStatus(true);
          for (Messages.QueryMessage listen : listens.values()) {
            sendAsync(listen);
          }
        } else {
          delegate.onAuthStatus(false);
        }
      }
    };
    sendAsync(new Messages.AuthMessage(token, oncomplete, requestCounter.getAndIncrement()));
  }

  private void handleAuthenticatingState(Message message) {
    checkState(message instanceof Messages.AckMessage);
    Messages.AckMessage ack = (Messages.AckMessage) message;
    resolveAck(ack);
  }

  private void handleAuthenticatedState(Message message) {
    if (message instanceof Messages.AckMessage) {
      Messages.AckMessage ack = (Messages.AckMessage) message;
      resolveAck(ack);
    } else if (message instanceof Messages.IncomingActionMessage) {
      ((Messages.IncomingActionMessage) message).process(delegate);
    }
  }

  private void resolveAck(Messages.AckMessage ack) {
    Messages.OutgoingMessage request = outbox.remove(ack.getRequestNumber());
    checkState(request != null);
    request.process(ack);
  }

  @Override
  public void onMessage(String data) {
    System.out.println("wire >> " + data);
    Message message = Messages.parse(data);
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
    Messages.QueryMessage queryMessage = new Messages.QueryMessage(callback, query,
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
