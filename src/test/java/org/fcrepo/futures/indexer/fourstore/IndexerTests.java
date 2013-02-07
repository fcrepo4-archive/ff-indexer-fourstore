package org.fcrepo.futures.indexer.fourstore;

import static org.apache.abdera.model.Text.Type.TEXT;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Properties;
import java.util.concurrent.Executors;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.abdera.Abdera;
import org.apache.abdera.model.Entry;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexerTests {
	
	private String bindAddress;
	private BrokerService broker = new BrokerService();
	private static String FEDORA_URI = "info:fedora/";
	private static String TEST_PID = "changeme:1001";
	private static String TEST_DS = "contentds";
	private static String TEST_REL = "info:fedora/fedora-system:hasDatastream";
	private static String TEST_GRAPH = "test-graph";
	ActiveMQConnectionFactory connectionFactory;
	@Before
	public void setUp() {
    	InputStream propFile = getClass().getResourceAsStream("test.properties");
    	Properties props = new Properties();
    	try {
			props.load(propFile);
		} catch (IOException e) {
			fail(e.getMessage());
		}
    	bindAddress = props.getProperty("broadcast", ActiveMQConnection.DEFAULT_BROKER_URL);
        broker.setUseJmx(true);
        try {
			broker.addConnector(bindAddress);
			broker.start();
		} catch (Exception e) {
			fail(e.getMessage());
		}
    	String username = props.getProperty("username", ActiveMQConnection.DEFAULT_USER);
    	String password = props.getProperty("password", ActiveMQConnection.DEFAULT_PASSWORD);
        connectionFactory = new ActiveMQConnectionFactory(
                username,
                password,
                bindAddress);
	}
	
	@After
	public void shutdown() throws Exception {
		if (broker.isStarted()) {
		    broker.stop();
		}
	}
	
	private static Message getAddMessage(Session session) throws JMSException {
		return getTestMessage(session, "addDatastream");
	}

	private static Message getDeleteMessage(Session session) throws JMSException {
		return getTestMessage(session, "purgeDatastream");
	}
	
	private static Message getTestMessage(Session session, String operation) throws JMSException {
		Abdera abdera = new Abdera();
		
		Entry entry = abdera.newEntry();
		entry.setTitle(operation, TEXT).setBaseUri("http://localhost:8080/rest");
		entry.addCategory("xsd:string", TEST_PID, "fedora-types:pid");
		entry.setContent("contentds");
		StringWriter writer = new StringWriter();
		try {
			entry.writeTo(writer);
		} catch (IOException e) {
			// hush
		}
		
		String atomMessage = writer.toString();
		return session.createTextMessage(atomMessage);
	}
	
	private static Message getIrrelevantMessage(Session session) throws JMSException {
		Abdera abdera = new Abdera();
		
		Entry entry = abdera.newEntry();
		entry.setTitle("modifyObject", TEXT).setBaseUri("http://localhost:8080/rest");
		entry.addCategory("xsd:string", TEST_PID, "fedora-types:pid");
		StringWriter writer = new StringWriter();
		try {
			entry.writeTo(writer);
		} catch (IOException e) {
			// hush
		}
		
		String atomMessage = writer.toString();
		return session.createTextMessage(atomMessage);
	}
	
    @Test
    public void testStuff() throws InterruptedException {
    	FourStore mock4store = mock(FourStore.class);
		Indexer r = new Indexer(connectionFactory, mock4store, TEST_GRAPH);
		Session session = null;
		Destination destination = null;
		try {
			Connection connection = connectionFactory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			destination = session.createQueue("test index messages");
		} catch (JMSException e) {
			fail(e.getMessage());
		}
		Executors.newFixedThreadPool(1).submit(r);
		// send a message
		MessageProducer producer = null;
		try {
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			producer.send(getAddMessage(session));
			synchronized (r) {
				r.wait(1000);
			}
			producer.send(getIrrelevantMessage(session));
			synchronized (r) {
				r.wait(1000);
			}
			producer.send(getDeleteMessage(session));
			synchronized (r) {
				r.wait(1000);
			}
	        r.stop();
		} catch (JMSException e) {
		} finally {
			if (producer != null) {
				try {
					producer.close();
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		}
		try {
			verify(mock4store).add(TEST_GRAPH, FEDORA_URI + TEST_PID, TEST_REL, FEDORA_URI + TEST_PID + "/" + TEST_DS);
			verify(mock4store).delete(TEST_GRAPH, FEDORA_URI + TEST_PID, TEST_REL, FEDORA_URI + TEST_PID + "/" + TEST_DS);
			verify(mock4store, times(1)).add(anyString(),anyString(), anyString(), anyString());
			verify(mock4store, times(1)).delete(anyString(),anyString(), anyString(), anyString());
		} catch (Exception e) {
			fail(e.getMessage());
		}
    }
}
