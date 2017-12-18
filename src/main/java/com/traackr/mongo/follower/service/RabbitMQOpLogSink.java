package com.traackr.mongo.follower.service;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

public class RabbitMQOpLogSink implements OpLogSink {
	private static final Logger logger = Logger.getLogger(OpLogSink.class.getName());
	private SimpleMessageListenerContainer listener = null;

	public RabbitMQOpLogSink() {
		RabbitTemplate template = new RabbitTemplate();
		logger.info("Created RabbitMQ oplog sink");
	}
	
	@Override
	public boolean send(Serializable data) {
		return false;
	}

	@Override
	public boolean send(Serializable data, long timeout, TimeUnit unit) {
		return false;
	}

}
