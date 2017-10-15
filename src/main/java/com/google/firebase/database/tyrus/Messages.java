package com.google.firebase.database.tyrus;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.firebase.database.connection.CompoundHash;
import com.google.firebase.database.connection.ConnectionUtils;
import com.google.firebase.database.connection.ListenHashProvider;
import com.google.firebase.database.connection.PersistentConnectionImpl;
import com.google.firebase.database.connection.RequestResultCallback;
import com.google.firebase.database.util.GAuthToken;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

class Messages {

  abstract static class DataMessage implements Message {

    protected final JSONObject data;

    DataMessage(JSONObject data) {
      this.data = checkNotNull(data);
    }

    final <T> T getData(String key, Class<T> clazz) {
      return clazz.cast(data.get(key));
    }

    final JSONObject getData() {
      return data;
    }

  }

  static final class IncomingActionMessage extends DataMessage {

    private final String action;

    IncomingActionMessage(JSONObject data) {
      super(data.getJSONObject("b"));
      this.action = data.getString("a");
    }

    void process(PersistentConnectionImpl.Delegate delegate) {
      if (action.equals("d") || action.equals("m")) {
        boolean isMerge = action.equals("m");
        Long tagNumber = null;
        if (data.has("t")) {
          tagNumber = data.getLong("t");
        }
        // ignore empty merges
        Map<String, Object> parsed = data.toMap();
        if (!isMerge || !parsed.isEmpty()) {
          List<String> path = ConnectionUtils.stringToPath(data.getString("p"));
          delegate.onDataUpdate(path, parsed, isMerge, tagNumber);
        }
      }
    }
  }

  static class ControlMessage extends DataMessage {

    private final String type;

    private ControlMessage(JSONObject data) {
      super(data.getJSONObject("d"));
      this.type = data.getString("t");
    }

    String getType() {
      return type;
    }

  }

  interface MessageCallback {
    void onComplete(AckMessage ack);
  }

  static final class AckMessage extends DataMessage {

    private final long requestNumber;

    AckMessage(JSONObject data) {
      super(data.getJSONObject("b"));
      this.requestNumber = data.getLong("r");
    }

    public String getType() {
      return "ack";
    }

    long getRequestNumber() {
      return requestNumber;
    }

  }

  abstract static class OutgoingMessage implements Message {

    private final long requestNumber;
    private final MessageCallback callback;

    OutgoingMessage(long requestNumber) {
      this(requestNumber, null);
    }

    OutgoingMessage(long requestNumber, MessageCallback callback) {
      this.requestNumber = requestNumber;
      this.callback = callback;
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

    protected abstract Map<String, Object> getBody();

    void process(AckMessage ack) {
      if (callback != null) {
        callback.onComplete(ack);
      }
    }
  }

  abstract static class ActionMessage extends OutgoingMessage {

    private final String action;

    ActionMessage(String action, MessageCallback callback, long requestNumber) {
      super(requestNumber, callback);
      this.action = action;
    }

    ActionMessage(String action, long requestNumber) {
      super(requestNumber);
      this.action = action;
    }

    @Override
    protected final Map<String, Object> getBody() {
      return ImmutableMap.of("a", action, "b", getActionBody());
    }

    protected abstract Map<String, Object> getActionBody();
  }

  static class StatsMessage extends ActionMessage {

    StatsMessage(long requestNumber) {
      super("s", requestNumber);
    }

    @Override
    protected Map<String, Object> getActionBody() {
      return ImmutableMap.<String, Object>of(
          "c", ImmutableMap.of("sdk.admin_java.5-4-1-SNAPSHOT", 1));
    }
  }

  static class AuthMessage extends ActionMessage {

    private final String token;

    AuthMessage(String token, MessageCallback callback, long requestNumber) {
      super("gauth", callback, requestNumber);
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
  }

  static class QueryMessage extends ActionMessage {

    private final RequestResultCallback resultCallback;
    private final ListenQuerySpec query;
    private final ListenHashProvider hashFunction;
    private final Long tag;

    QueryMessage(
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
        Map<String, Object> hash = new HashMap<>();
        hash.put("hs", compoundHash.getHashes());
        hash.put("ps", posts);
        request.put("ch", hash);
      }
      return request.build();
    }
  }

  static Message parse(String data) {
    JSONObject json = new JSONObject(data);
    if (json.getString("t").equals("c")) {
      return new ControlMessage(json.getJSONObject("d"));
    } else if (json.getString("t").equals("d")) {
      JSONObject nestedData = json.getJSONObject("d");
      if (nestedData.has("r")) {
        return new AckMessage(nestedData);
      } else if (nestedData.has("a")) {
        return new IncomingActionMessage(nestedData);
      }
    }
    throw new RuntimeException("Unsupported message: " + data);
  }

}
