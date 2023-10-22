package io.inline;

public class Main {
    public static void main(String[] args) {
        String output = IKVClientJNI.provideHelloWorld();
        System.out.println(output);
    }
}