package com.google.firebase.database.tyrus;

import com.google.firebase.database.core.AuthTokenProvider;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class AuthUtils {

  static String getToken(final AuthTokenProvider authProvider) {
    final Semaphore semaphore = new Semaphore(0);
    final AtomicReference<String> result = new AtomicReference<>();
    final AtomicBoolean errorFlag = new AtomicBoolean(false);

    authProvider.getToken(false, new AuthTokenProvider.GetTokenCompletionListener() {
      @Override
      public void onSuccess(String token) {
        result.set(token);
        semaphore.release();
      }

      @Override
      public void onError(String error) {
        errorFlag.set(true);
        result.set(error);
        semaphore.release();
      }
    });

    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    if (errorFlag.get()) {
      throw new RuntimeException(result.get());
    }
    return result.get();
  }
}
