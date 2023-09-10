package org.learn;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello Kafka!");

        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the message content: ");
        String content = scanner.nextLine();

        new KConsumer().consume();
        //new KProducer().produce(content);
    }
}