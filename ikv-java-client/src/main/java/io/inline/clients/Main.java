package io.inline.clients;

public class Main {
    public static void main(String[] args) {
        String output = IKVClientJNI.provideHelloWorld();
        System.out.println(output);
    }
}