package io.inline.clients;

import io.inline.clients.internal.IKVClientJNI;

public class Main {
  public static void main(String[] args) {
    String output = IKVClientJNI.provideHelloWorld();
    System.out.println(output);
  }
}
