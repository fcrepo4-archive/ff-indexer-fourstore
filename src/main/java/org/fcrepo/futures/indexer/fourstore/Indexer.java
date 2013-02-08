package org.fcrepo.futures.indexer.fourstore;

import java.io.StringReader;
import java.util.concurrent.Callable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.abdera.Abdera;
import org.apache.abdera.model.Document;
import org.apache.abdera.model.Entry;
import org.apache.abdera.parser.Parser;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;

public class Indexer implements Callable<Object> {
	private ConnectionFactory m_connectionFactory;
	private Connection m_connection;
	private MessageConsumer m_consumer;
	private FourStore m_store;
	private String m_graphName;
	private Parser abderaParser = new Abdera().getParser();
	public Indexer(ConnectionFactory connectionFactory, FourStore store, String graphName) {
		m_connectionFactory = connectionFactory;
		m_store = store;
		m_graphName = graphName;
	}

	public Object call() throws JMSException {
		m_connection = m_connectionFactory.createConnection();
		m_connection.start();
		Session session = m_connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue("test index messages");
		m_consumer = session.createConsumer(destination);
		while(true){
			try {
				TextMessage message = (TextMessage) m_consumer.receive();
				if (message != null){
					System.out.println("Got a message: " + message.toString());
					Document<Entry> doc = abderaParser.parse(new StringReader(message.getText()));
					Entry entry = doc.getRoot();
					if ("addDatastream".equals(entry.getTitle())) {
						String pid = "info:fedora/" + entry.getCategories("xsd:string").get(0).getTerm();
						String dsid = pid + "/" + entry.getContent();
						String rel = "info:fedora/fedora-system:hasDatastream";
						try {
							StatusLine response = m_store.add(m_graphName, pid, rel, dsid);
							if (response.getStatusCode() == HttpStatus.SC_CREATED){
							    System.out.println(String.format("added %s:<%s><%s><%s>",m_graphName, pid,  rel, dsid));
							} else {
								System.out.println("WARN: unexpected response code for add: " + response);
							}
						} catch (Exception e) {
							System.out.println("ERROR: " + e.toString());
						}
					}
					if ("purgeDatastream".equals(entry.getTitle())) {
						String pid = "info:fedora/" + entry.getCategories("xsd:string").get(0).getTerm();
						String dsid = pid + "/" + entry.getContent();
						String rel = "info:fedora/fedora-system:hasDatastream";
						try {
							StatusLine response = m_store.delete(m_graphName, pid, rel, dsid);
							if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT){
								System.out.println(String.format("deleted %s:<%s><%s><%s>",m_graphName, pid,  rel, dsid));
							} else {
								System.out.println("WARN: unexpected response code for delete: " + response);
							}
						} catch (Exception e) {
							System.out.println("ERROR: " + e.toString());
						}
					}
				} else {
					break;
				}
				synchronized(this) { notifyAll(); }
			} catch (javax.jms.IllegalStateException e) {
				// nothing: this consumer was shut down
			} catch (JMSException e) {
				e.printStackTrace();
			}

		}
		return null;
	}

	public void stop() throws JMSException {
		try{
			if (m_consumer != null) {
				m_consumer.close();
			}
			if (m_connection != null) {
				m_connection.stop();
			}
		} finally {
			synchronized(this) { notifyAll(); }
		}

	}

}
