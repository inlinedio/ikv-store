package io.inline.clients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class ToyCli {
    public static void main(String[] args) throws IOException {
        LegacyIKVClient client = LegacyIKVClient.open("/tmp/benchmark");
        System.out.println("Welcome! Enter a CRUD command for inline-io.");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            System.out.print(">");
            String command = br.readLine();
            if ("EXIT".equalsIgnoreCase(command)) {
                break;
            }
            process(command, client);
        }

        System.exit(0);
    }

    private static void process(String command, LegacyIKVClient client) {
        // Syntax: GET primary_key field_name
        // Syntax: PUT primary_key field_name field_value
        // Syntax: DEL primary_key field_name
        List<String> commandArgs = Arrays.stream(command.split(" "))
                .map(String::strip)
                .toList();

        switch (commandArgs.get(0).toUpperCase()) {
            case "GET":
            {
                String primaryKey = commandArgs.get(1);
                String fieldName = commandArgs.get(2);
                byte[] rawValue = client.readBytesField(
                        primaryKey.getBytes(StandardCharsets.UTF_8), fieldName);
                if (rawValue == null) {
                    System.out.println("Not found");
                } else {
                    System.out.println(new String(rawValue, StandardCharsets.UTF_8));
                }
                break;
            }
            case "PUT":
            {
                String primaryKey = commandArgs.get(1);
                String fieldName = commandArgs.get(2);
                String fieldValue = commandArgs.get(3);
                client.upsertFieldValue(primaryKey.getBytes(StandardCharsets.UTF_8), fieldValue.getBytes(StandardCharsets.UTF_8),fieldName);
                System.out.println("Success");
                break;
            }
            case "DEL":
            {
                String primaryKey = commandArgs.get(1);
                String fieldName = commandArgs.get(2);
                break;
            }
            default:
                System.out.println("unknown command!");
                break;
        }
    }
}
