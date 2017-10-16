package com.google.firebase;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.database.ChildEventListener;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.Logger;
import com.google.firebase.database.ValueEventListener;

import java.io.FileInputStream;
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
    }
  }

  private static void runFirebase() throws Exception {
    GoogleCredentials credentials = GoogleCredentials.fromStream(
        new FileInputStream("integration_cert.json"));
    FirebaseOptions options = new FirebaseOptions.Builder()
        .setCredentials(credentials)
        .setDatabaseUrl("https://admin-java-integration.firebaseio.com")
        .build();
    FirebaseApp.initializeApp(options);
    FirebaseDatabase database = FirebaseDatabase.getInstance();
    database.setLogLevel(Logger.Level.DEBUG);

    DatabaseReference foo = database.getReference().child("foo");
    foo.addValueEventListener(new ValueEventListener() {
      @Override
      public void onDataChange(DataSnapshot snapshot) {
        System.out.println(snapshot.getValue());
      }

      @Override
      public void onCancelled(DatabaseError error) {

      }
    });
    foo.child("bar").setValueAsync(System.currentTimeMillis()).get();

    database.getReference().child("parent").addChildEventListener(new ChildEventListener() {
      @Override
      public void onChildAdded(DataSnapshot snapshot, String previousChildName) {
        System.out.println("[EVENT] ADD " + snapshot.getValue());
      }

      @Override
      public void onChildChanged(DataSnapshot snapshot, String previousChildName) {
        System.out.println("[EVENT] CHANGE " + snapshot.getValue());
      }

      @Override
      public void onChildRemoved(DataSnapshot snapshot) {
        System.out.println("[EVENT] DELETE " + snapshot.getValue());
      }

      @Override
      public void onChildMoved(DataSnapshot snapshot, String previousChildName) {
        System.out.println("[EVENT] MOVE " + snapshot.getValue());
      }

      @Override
      public void onCancelled(DatabaseError error) {
        System.out.println("ERROR: " + error.getMessage());
      }
    });
    System.in.read();
    FirebaseApp.getInstance().delete();
  }

}
