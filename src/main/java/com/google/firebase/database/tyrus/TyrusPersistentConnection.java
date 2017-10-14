package com.google.firebase.database.tyrus;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.firebase.database.connection.CompoundHash;
import com.google.firebase.database.connection.ConnectionUtils;
import com.google.firebase.database.connection.HostInfo;
import com.google.firebase.database.connection.ListenHashProvider;
import com.google.firebase.database.connection.PersistentConnection;
import com.google.firebase.database.connection.RequestResultCallback;
import com.google.firebase.database.core.AuthTokenProvider;
import com.google.firebase.database.core.Repo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TyrusPersistentConnection implements PersistentConnection, TyrusWebsocket.Delegate {

  private final TyrusWebsocket websocket;
  private final PersistentConnection.Delegate delegate;

  private final Map<ListenQuerySpec, OutstandingListen> listens = new ConcurrentHashMap<>();

  public TyrusPersistentConnection(Repo repo, HostInfo hostInfo, AuthTokenProvider authProvider) {
    this.delegate = checkNotNull(repo);
    this.websocket = new TyrusWebsocket(hostInfo, this, authProvider);
  }

  @Override
  public void initialize() {
    try {
      websocket.open();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
    try {
      websocket.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
      ListenHashProvider currentHashFn, Long tag, RequestResultCallback onComplete) {

    ListenQuerySpec query = new ListenQuerySpec(path, queryParams);
    checkState(!listens.containsKey(query), "listen() called twice for same QuerySpec.");
    OutstandingListen listen =
        new OutstandingListen(onComplete, query, tag, currentHashFn);
    listens.put(query, listen);
    websocket.sendQuery(listen.toMap(), onComplete);
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

  @Override
  public void onConnect() {
    delegate.onConnect();
  }

  @Override
  public void onAuth(boolean status) {
    delegate.onAuthStatus(status);
    for (OutstandingListen listen : listens.values()) {
      websocket.sendQuery(listen.toMap(), listen.resultCallback);
    }
  }

  @Override
  public void onQuery(boolean status, RequestResultCallback callback) {
    if (!status) {
      callback.onRequestResult("some_error", "error");
    }
  }

  private static class ListenQuerySpec {

    private final List<String> path;
    private final Map<String, Object> queryParams;

    public ListenQuerySpec(List<String> path, Map<String, Object> queryParams) {
      this.path = path;
      this.queryParams = queryParams;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ListenQuerySpec)) {
        return false;
      }

      ListenQuerySpec that = (ListenQuerySpec) o;

      if (!path.equals(that.path)) {
        return false;
      }
      return queryParams.equals(that.queryParams);
    }

    @Override
    public int hashCode() {
      int result = path.hashCode();
      result = 31 * result + queryParams.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return ConnectionUtils.pathToString(this.path) + " (params: " + queryParams + ")";
    }
  }

  private static class OutstandingListen {

    private final RequestResultCallback resultCallback;
    private final ListenQuerySpec query;
    private final ListenHashProvider hashFunction;
    private final Long tag;

    private OutstandingListen(
        RequestResultCallback callback,
        ListenQuerySpec query,
        Long tag,
        ListenHashProvider hashFunction) {
      this.resultCallback = callback;
      this.query = query;
      this.hashFunction = hashFunction;
      this.tag = tag;
    }

    ListenQuerySpec getQuery() {
      return query;
    }

    Long getTag() {
      return this.tag;
    }

    @Override
    public String toString() {
      return query.toString() + " (Tag: " + this.tag + ")";
    }

    Map<String, Object> toMap() {
      Map<String, Object> request = new HashMap<>();
      request.put("p", ConnectionUtils.pathToString(query.path));
      // Only bother to send query if it's non-default
      if (tag != null) {
        request.put("q", query.queryParams);
        request.put("t", tag);
      }

      request.put("h", hashFunction.getSimpleHash());
      if (hashFunction.shouldIncludeCompoundHash()) {
        CompoundHash compoundHash = hashFunction.getCompoundHash();

        List<String> posts = new ArrayList<>();
        for (List<String> post : compoundHash.getPosts()) {
          posts.add(ConnectionUtils.pathToString(post));
        }
        Map<String, Object> hash = new HashMap<>();
        hash.put("hs", compoundHash.getHashes());
        hash.put("ps", posts);
        request.put("ch", hash);
      }
      return request;
    }
  }
}
