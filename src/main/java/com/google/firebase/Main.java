package com.google.firebase;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.Logger;
import com.google.firebase.database.ValueEventListener;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import javax.websocket.ClientEndpointConfig;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import org.glassfish.tyrus.client.ClientManager;

public class Main {

  public static void main(String[] args) throws Exception {
    runFirebase();
    //runWebsocket();
  }

  private static void runWebsocket() throws Exception {
    final ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();

    ClientManager client = ClientManager.createClient();
    URI uri = new URI(
        "wss://admin-java-integration.firebaseio.com/.ws?ns=admin-java-integration&v=5");
    client.connectToServer(new Endpoint() {

      @Override
      public void onOpen(Session session, EndpointConfig config) {
        session.addMessageHandler(new FirebaseMessaheHandler(session));
      }
    }, cec, uri);
    Thread.sleep(60000);
  }

  private static class FirebaseMessaheHandler implements MessageHandler.Whole<String> {

    private final Session session;
    private final AtomicInteger counter = new AtomicInteger(0);

    public FirebaseMessaheHandler(Session session) {
      this.session = session;
    }

    @Override
    public void onMessage(String s) {
      System.out.println(s);
      int index = counter.incrementAndGet();
      String msg = null;
      if (index == 1) {
        msg = "{\"t\":\"d\",\"d\":{\"a\":\"s\",\"r\":0,\"b\":{\"c\":{\"sdk" +
            ".admin_java.5-4-1-SNAPSHOT\":1}}}}";
      } else if (index == 2) {
        msg = "{\"t\":\"d\",\"d\":{\"a\":\"gauth\",\"r\":1," +
            "\"b\":{\"cred\":\"ya29.El_kBHo7Pg6ZFy1TtCAfju5S9yayPotN857JFVMTvXRmDC7TPtjvyvM6qwQ" +
            "ScnnogGb90Bpe7vVpF4rMyMFFdDDfph-DVZpWG27PgnfI7PhMgAZeVl6IJtcUUiAVAor_kQ\"}}}";
      } else if (index == 3) {
        msg = "{\"t\":\"d\",\"d\":{\"a\":\"q\",\"r\":2,\"b\":{\"p\":\"foo\",\"h\":\"\"}}}";
      }

      if (msg != null) {
        try {
          session.getBasicRemote().sendText(msg);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void runFirebase() throws Exception {
    GoogleCredentials credentials = GoogleCredentials.fromStream(
        new FileInputStream("integration_cert.json"));
    FirebaseOptions options = new FirebaseOptions.Builder()
        .setCredentials(credentials)
        .setDatabaseUrl("https://admin-java-integration.firebaseio.com")
        .build();
    FirebaseApp app = FirebaseApp.initializeApp(options);
    FirebaseDatabase database = FirebaseDatabase.getInstance();
    database.setLogLevel(Logger.Level.DEBUG);

    database.getReference().child("foo").addValueEventListener(new ValueEventListener() {
      @Override
      public void onDataChange(DataSnapshot snapshot) {
        System.out.println(snapshot.getValue());
      }

      @Override
      public void onCancelled(DatabaseError error) {

      }
    });
    Thread.sleep(500000);
    app.delete();
  }

}
