//package com.roman.rmq.consumer;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.concurrent.TimeoutException;


import com.rabbitmq.client.*;


public class Recv {

    private final static String QUEUE_NAME = "task_queue";
    private final static String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws java.io.IOException, java.lang.InterruptedException {

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            //boolean durable = true;

            // Declare an exchange (probably fins the existing on with the same name)
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, EXCHANGE_NAME, "");

            //channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            int prefetchCount = 1;

            channel.basicQos(prefetchCount); // accept only one unack-ed message at a time

            final Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");

                    System.out.println(" [x] Received '" + message + "'");

                    try {
                        doWork(message);
                    } catch (InterruptedException ie) {
                        System.out.println(ie.toString());
                    } finally {
                        System.out.println(" [x] Done");
                        channel.basicAck(envelope.getDeliveryTag(), false);
                    }
                }
            };

            boolean autoAck = true;

            channel.basicConsume(queueName, autoAck, consumer);

        } catch (TimeoutException tOutEx) {
            tOutEx.printStackTrace(System.err);
        }

    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                Thread.sleep(1000);
            }
        }
    }
}