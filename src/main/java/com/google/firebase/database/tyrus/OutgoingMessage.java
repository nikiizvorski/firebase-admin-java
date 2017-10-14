package com.google.firebase.database.tyrus;

import com.google.common.collect.ImmutableMap;
import com.google.firebase.database.connection.RequestResultCallback;

import java.util.Map;

abstract class OutgoingMessage implements Message {

  private final long requestNumber;

  OutgoingMessage(long requestNumber) {
    this.requestNumber = requestNumber;
  }

  final long getRequestNumber() {
    return requestNumber;
  }

  final Map<String, Object> toJson() {
    return ImmutableMap.<String, Object>of(
        "t", "d",
        "d", ImmutableMap.builder().put("r", requestNumber).putAll(getBody()).build());
  }

  protected abstract Map<String, Object> getBody();

  abstract void onResponse(AckMessage message, TyrusWebsocket.Delegate socket);



  abstract static class ActionMessage extends OutgoingMessage {

    private final String action;
    protected final Map<String, Object> data;

    ActionMessage(String action, Map<String, Object> data, long requestNumber) {
      super(requestNumber);
      this.action = action;
      this.data = data;
    }

    @Override
    public String getType() {
      return "action";
    }

    @Override
    protected Map<String, Object> getBody() {
      return ImmutableMap.of("a", action, "b", data);
    }
  }

  static class StatsMessage extends ActionMessage {

    StatsMessage(Map<String, Object> data, long requestNumber) {
      super("s", data, requestNumber);
    }

    @Override
    public void onResponse(AckMessage message, TyrusWebsocket.Delegate websocket) {
      if (message.getData("s", String.class).equals("ok")) {
        System.out.println("Stats accepted");
      } else {
        System.out.println("Stats not accepted");
      }
    }
  }

  static class AuthMessage extends ActionMessage {

    AuthMessage(Map<String, Object> data, long requestNumber) {
      super("gauth", data, requestNumber);
    }

    @Override
    public void onResponse(AckMessage message, TyrusWebsocket.Delegate websocket) {
    }
  }

  static class QueryMessage extends ActionMessage {

    private final RequestResultCallback callback;

    public QueryMessage(
        Map<String, Object> data, RequestResultCallback callback, long requestNumber) {
      super("q", data, requestNumber);
      this.callback = callback;
    }

    @Override
    void onResponse(AckMessage message, TyrusWebsocket.Delegate socket) {
      if (message.getData("s", String.class).equals("ok")) {
        socket.onQuery(true, callback);
        System.out.println("Query accepted");
      } else {
        socket.onQuery(false, callback);
        System.out.println("Query not accepted");
      }
    }
  }

}
