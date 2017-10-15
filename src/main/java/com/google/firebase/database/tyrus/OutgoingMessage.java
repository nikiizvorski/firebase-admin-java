package com.google.firebase.database.tyrus;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.firebase.database.connection.CompoundHash;
import com.google.firebase.database.connection.ConnectionUtils;
import com.google.firebase.database.connection.ListenHashProvider;
import com.google.firebase.database.connection.PersistentConnection;
import com.google.firebase.database.connection.RequestResultCallback;
import com.google.firebase.database.util.GAuthToken;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONObject;

abstract class OutgoingMessage {

  private final long requestNumber;

  OutgoingMessage(long requestNumber) {
    this.requestNumber = requestNumber;
  }

  final long getRequestNumber() {
    return requestNumber;
  }

  final String toJson() {
    Map<String, Object> map = ImmutableMap.<String, Object>of(
        "t", "d",
        "d", ImmutableMap.builder().put("r", requestNumber).putAll(getBody()).build());
    JSONObject json = new JSONObject(map);
    return json.toString();
  }

  abstract Map<String, Object> getBody();

  void onComplete(PersistentConnection.Delegate delegate, Message.Ack ack) {

  }


  abstract static class Action extends OutgoingMessage {

    private final String action;

    Action(String action, long requestNumber) {
      super(requestNumber);
      this.action = action;
    }

    @Override
    protected final Map<String, Object> getBody() {
      return ImmutableMap.of("a", action, "b", getActionBody());
    }

    protected abstract Map<String, Object> getActionBody();
  }

  static final class Stats extends Action {

    Stats(long requestNumber) {
      super("s", requestNumber);
    }

    @Override
    protected Map<String, Object> getActionBody() {
      return ImmutableMap.<String, Object>of(
          "c", ImmutableMap.of("sdk.admin_java.5-4-1-SNAPSHOT", 1));
    }
  }

  static final class Auth extends Action {

    private final String token;

    Auth(String token, long requestNumber) {
      super("gauth", requestNumber);
      checkArgument(!Strings.isNullOrEmpty(token));
      this.token = token;
    }

    @Override
    protected Map<String, Object> getActionBody() {
      ImmutableMap.Builder<String, Object> request = ImmutableMap.builder();
      GAuthToken googleAuthToken = GAuthToken.tryParseFromString(token);
      if (googleAuthToken != null) {
        request.put("cred", googleAuthToken.getToken());
      } else {
        request.put("cred", token);
      }
      return request.build();
    }

    @Override
    void onComplete(PersistentConnection.Delegate delegate, Message.Ack ack) {
      if (ack.getDataField("s", String.class).equals("ok")) {
        System.out.println("Auth successful");
        delegate.onAuthStatus(true);
      } else {
        delegate.onAuthStatus(false);
      }
    }
  }

  static final class Query extends Action {

    private final RequestResultCallback resultCallback;
    private final ListenQuerySpec query;
    private final ListenHashProvider hashFunction;
    private final Long tag;

    Query(
        RequestResultCallback resultCallback,
        ListenQuerySpec query,
        ListenHashProvider hashFunction,
        Long tag, long requestNumber) {
      super("q", requestNumber);
      this.resultCallback = resultCallback;
      this.query = query;
      this.hashFunction = hashFunction;
      this.tag = tag;
    }

    @Override
    protected Map<String, Object> getActionBody() {
      ImmutableMap.Builder<String, Object> request = ImmutableMap.builder();
      request.put("p", ConnectionUtils.pathToString(query.getPath()));
      // Only bother to send query if it's non-default
      if (tag != null) {
        request.put("q", query.getQueryParams());
        request.put("t", tag);
      }

      request.put("h", hashFunction.getSimpleHash());
      if (hashFunction.shouldIncludeCompoundHash()) {
        CompoundHash compoundHash = hashFunction.getCompoundHash();

        List<String> posts = new ArrayList<>();
        for (List<String> post : compoundHash.getPosts()) {
          posts.add(ConnectionUtils.pathToString(post));
        }
        Map<String, Object> hash = ImmutableMap.<String, Object>of(
            "hs", compoundHash.getHashes(),
            "ps", posts);
        request.put("ch", hash);
      }
      return request.build();
    }

    @Override
    void onComplete(PersistentConnection.Delegate delegate, Message.Ack ack) {
      String status = ack.getDataField("s", String.class);
      if (status.equals("ok")) {
        System.out.println("Query accepted");
      } else {
        // TODO: Pass the right args
        this.resultCallback.onRequestResult(status, "error");
      }
    }
  }

  static final class Put extends Action {

    private final List<String> path;
    private final Object data;
    private final RequestResultCallback onComplete;

    Put(
        List<String> path,
        Object data,
        RequestResultCallback onComplete,
        long requestNumber) {
      super("p", requestNumber);
      this.path = path;
      this.data = data;
      this.onComplete = onComplete;
    }

    @Override
    protected Map<String, Object> getActionBody() {
      return ImmutableMap.of(
          "p", ConnectionUtils.pathToString(path),
          "d", data);
    }

    @Override
    void onComplete(PersistentConnection.Delegate delegate, Message.Ack ack) {
      if (onComplete != null) {
        String status = ack.getDataField("s", String.class);
        if (status.equals("ok")) {
          onComplete.onRequestResult(null, null);
        } else {
          String errorMessage = ack.getDataField("d", String.class);
          onComplete.onRequestResult(status, errorMessage);
        }
      }
    }
  }

}
