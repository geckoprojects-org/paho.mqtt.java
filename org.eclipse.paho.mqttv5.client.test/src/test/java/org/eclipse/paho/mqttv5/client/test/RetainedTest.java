/*
 * Copyright (c) 2012 - 2017 Data In Motion and others.
 * All rights reserved. 
 * 
 * This program and the accompanying materials are made available under the terms of the 
 * Eclipse Public License v1.0 which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Data In Motion
 *******************************************************************************/

package org.eclipse.paho.mqttv5.client.test;

import static org.junit.Assert.assertTrue;
import org.junit.runners.Parameterized;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.IMqttClient;
import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.test.client.MqttClientFactoryPaho;
import org.eclipse.paho.mqttv5.client.test.logging.LoggingUtilities;
import org.eclipse.paho.mqttv5.client.test.properties.TestProperties;
import org.eclipse.paho.mqttv5.client.test.utilities.Utility;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttPersistenceException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests shows an issue with a wildcard subscription to a topic with lots of
 * retained messages.
 */
@RunWith(Parameterized.class)
public class RetainedTest {

	static final Class<?> cclass = RetainedTest.class;
	private static final String className = cclass.getName();
	private static final Logger log = Logger.getLogger(className);

	private static final int MESSAGE_COUNT = 20000;
	private static final int TOPIC_COUNT = 4;
	private static URI serverURI;
	private static MqttClientFactoryPaho clientFactory;
	private static String topicPrefix;

	/**
	 * @throws Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		try {
			String methodName = Utility.getMethodName();
			LoggingUtilities.banner(log, cclass, methodName);

			serverURI = TestProperties.getServerURI();
			clientFactory = new MqttClientFactoryPaho();
			clientFactory.open();
			topicPrefix = "RetainedTest-" + UUID.randomUUID().toString() + "-";

		} catch (Exception exception) {
			log.log(Level.SEVERE, "caught exception:", exception);
			throw exception;
		}
	}

	/**
	 * @throws Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		String methodName = Utility.getMethodName();
		LoggingUtilities.banner(log, cclass, methodName);

		try {
			if (clientFactory != null) {
				clientFactory.close();
				clientFactory.disconnect();
			}
		} catch (Exception exception) {
			log.log(Level.SEVERE, "caught exception:", exception);
		}
	}
	 @Parameterized.Parameters
	    public static Object[][] data() {
	        return new Object[10][0];
	    }
	/**
	 * @throws Exception
	 */
	@Test
	@Parameters
	public void testSubscribeRetained() throws Exception {
		String methodName = Utility.getMethodName();
		LoggingUtilities.banner(log, cclass, methodName);

		IMqttClient client = null;
		try {
			String clientId = methodName + "-pub";
			client = clientFactory.createMqttClient(serverURI, clientId);

			log.info("Connecting...(serverURI:" + serverURI + ", ClientId:" + clientId);
			client.connect();

			for (int i = 0; i < TOPIC_COUNT; i++) {
				String topic = topicPrefix + i + "/";
				publishMessages(client, topic);
			}

			log.info("Disconnecting... Closing ...");
			client.disconnect();
			client.close();

			log.info("Creating new client ... ");
			clientId = methodName + "-sub";
			final CountDownLatch messageLatch = new CountDownLatch(TOPIC_COUNT * MESSAGE_COUNT);
			client = clientFactory.createMqttClient(serverURI, clientId);

			log.info("Connecting...(serverURI:" + serverURI + ", ClientId:" + clientId);
			client.connect();

			for (int i = 0; i < TOPIC_COUNT; i++) {
				String t = topicPrefix + i + "/#";
				log.info("Subscribe to " + t);
				client.subscribe(t, 1, new IMqttMessageListener() {
					int messageCounter;
					@Override
					public void messageArrived(String topic, MqttMessage message) throws Exception {
						messageLatch.countDown();
						if (++messageCounter % 10000 == 0) {
							log.info(messageCounter + " messages arrived on " + t);
						}
					}
				});
				((MqttClient)client).getDebug().dumpClientState();
			}
			log.info("wait for the " + TOPIC_COUNT * MESSAGE_COUNT + " retained messages to arrive.");
			((MqttClient)client).getDebug().dumpClientState();
			boolean result = messageLatch.await(10, TimeUnit.SECONDS);
			assertTrue(messageLatch.getCount() + " messages didn't arrive.", result);

		} catch (MqttException exception) {
			log.log(Level.SEVERE, "caught exception:", exception);
			Assert.fail("Unexpected exception: " + exception);
		} finally {
			if (client != null) {
				log.info("Disconnecting...");
				client.disconnect();
				log.info("Close...");
				client.close();
			}
		}
	}

	private void publishMessages(IMqttClient client, String topic) throws MqttException, MqttPersistenceException {
		log.info("Publishing " + MESSAGE_COUNT + " retained messages to " + topic + ".");
		for (int i = 0; i < MESSAGE_COUNT; i++) {
			client.publish(topic + "1234567890-1234567890-1234567890-1234567890-" + i,
					("abcdefghijklmnopqrstuvw-abcdefghijklmnopqrstuvw-abcdefghijklmnopqrstuvw-abcdefghijklmnopqrstuvw-abcdefghijklmnopqrstuvw-abcdefghijklmnopqrstuvw-"
							+ i).getBytes(),
					0, true);
		}
	}

}
