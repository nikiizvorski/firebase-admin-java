package com.google.firebase.database.tyrus;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.firebase.database.connection.ConnectionUtils;
import com.google.firebase.database.connection.PersistentConnection;

import java.util.List;
import java.util.Map;

import org.json.JSONObject;

class Message {

  private final JSONObject data;

  Message(JSONObject data) {
    this.data = data;
  }

  protected final JSONObject getData() {
    return data;
  }

  final <T> T getDataField(String key, Class<T> clazz) {
    return clazz.cast(data.get(key));
  }

  final boolean hasDataField(String key) {
    return data.has(key);
  }

  static final class Control extends Message {

    private final String type;

    Control(JSONObject data) {
      super(data.getJSONObject("d"));
      this.type = data.getString("t");
    }

    String getType() {
      return type;
    }
  }

  static final class Ack extends Message {

    private final long requestNumber;

    Ack(JSONObject data) {
      super(data.getJSONObject("b"));
      this.requestNumber = data.getLong("r");
    }

    long getRequestNumber() {
      return requestNumber;
    }
  }

  abstract static class DataMessage extends Message {

    final String action;

    DataMessage(JSONObject data) {
      super(data.getJSONObject("b"));
      this.action = data.getString("a");
    }

    abstract void onComplete(PersistentConnection.Delegate delegate);
  }

  static final class DataUpdate extends DataMessage {

    DataUpdate(JSONObject data) {
      super(data);
      checkArgument(action.equals("d") || action.equals("m"));
    }

    @Override
    void onComplete(PersistentConnection.Delegate delegate) {
      boolean isMerge = action.equals("m");
      Long tagNumber = null;
      if (hasDataField("t")) {
        tagNumber = getDataField("t", Number.class).longValue();
      }

      // ignore empty merges
      Object parsed = getData().toMap().get("d");
      if (!isMerge || !(parsed instanceof Map) || !((Map) parsed).isEmpty()) {
        List<String> path = ConnectionUtils.stringToPath(getDataField("p", String.class));
        delegate.onDataUpdate(path, parsed, isMerge, tagNumber);
      }
    }
  }


  static Message parse(String data) {
    JSONObject json = new JSONObject(data);
    if (json.getString("t").equals("c")) {
      JSONObject nestedData = json.getJSONObject("d");
      if (nestedData.getString("t").equals("h")) {
        return new Control(nestedData);
      }
    } else if (json.getString("t").equals("d")) {
      JSONObject nestedData = json.getJSONObject("d");
      if (nestedData.has("r")) {
        return new Ack(nestedData);
      } else if (nestedData.has("a") && nestedData.getString("a").equals("d")) {
        return new DataUpdate(nestedData);
      }
    }
    throw new RuntimeException("Unknown message: " + data);
  }
}
