/**
 * Copyright © 2017 Sander Borny (sander.borny@ugent.be)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.LoggerFactory;


public class SetUpRabbitMQ {

  private static final org.slf4j.Logger log = LoggerFactory.getLogger(SetUpRabbitMQ.class);
  private RabbitMQSinkConnectorConfig config;
  private String topics;
  private Connection connection;
  private Channel channel;

  public SetUpRabbitMQ(RabbitMQConnectorConfig config, String topics) {
    this.config = (RabbitMQSinkConnectorConfig) config;
    this.topics = topics;
  }

  public void SetUpConnection() {
    ConnectionFactory connectionFactory = this.config.connectionFactory();
    try {
      this.connection = connectionFactory.newConnection();
      this.channel = this.connection.createChannel();
    } catch (IOException | TimeoutException e) {
      throw new ConnectException(e);
    }
  }

  public void CloseConnection() {
    try {
      this.connection.close();
    } catch (IOException e) {
      log.error("Exception thrown while closing connection.", e);
    }
  }

  public void SetUpExchange() {
    try {
      this.channel.exchangeDeclare(this.config.exchange, "direct", true);        
    } catch (IOException e) {
      log.error("Exception thrown while creating exchange.", e);
    }
  }

  public void CreateQueues() {
    for (String topic : this.topics.split(",")) {
      try {
        this.channel.queueDeclare(topic, true, false, false, null);
      } catch (IOException e) {
        log.error("Exception thrown while creating queues.", e);
      }
    }
  }
    
  public void CreateBindings() {
    for (String topic : this.topics.split(",")) {
      try {
        this.channel.queueBind(topic, this.config.exchange, topic);
      } catch (IOException e) {
        log.error("Exception thrown while creating bindings.", e);
      }
    }
  }

}
