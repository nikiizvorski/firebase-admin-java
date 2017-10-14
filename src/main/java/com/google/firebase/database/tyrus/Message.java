package com.google.firebase.database.tyrus;

import java.util.Map;


interface Message {

  String getType();

  abstract class DataMessage implements Message {

    private final Map<String, Object> data;

    protected DataMessage(Object data) {
      this.data = (Map) data;
    }

    final <T> T getData(String key, Class<T> clazz) {
      return clazz.cast(data.get(key));
    }
  }

  final class ControlMessage extends DataMessage {

    private final String type;

    ControlMessage(Map<String, Object> data) {
      super(data.get("d"));
      this.type = (String) data.get("t");
    }

    @Override
    public String getType() {
      return "control:" + type;
    }
  }

  final class AckMessage extends DataMessage {

    private final long requestNumber;

    AckMessage(long requestNumber, Map<String, Object> data) {
      super(data);
      this.requestNumber = requestNumber;
    }

    @Override
    public String getType() {
      return "ack";
    }

    long getRequestNumber() {
      return requestNumber;
    }
  }

}
