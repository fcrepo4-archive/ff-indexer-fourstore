package org.fcrepo.futures.indexer.fourstore;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FourstoreTests {
	HttpClient client = new DefaultHttpClient();
	int port = 8000;
	Process backend;
	Process httpd;
	String test_graph = "fcr4test";
	FourStore testObject = new FourStore();
	String test_subject = "info:fedora/changeme:1001";
	String test_predicate = "info:fedora/fedora-system:hasDatastream";
	String test_object = "info:fedora/changeme:1001/contentds";
	@Before
	public void setup(){
		try {
			// start the 4store httpd
			Process p = Runtime.getRuntime().exec("4s-backend-destroy " + test_graph);
			p = Runtime.getRuntime().exec("4s-backend-setup " + test_graph);
			backend = Runtime.getRuntime().exec("4s-backend " + test_graph);
			httpd = Runtime.getRuntime().exec("4s-httpd -p " + port + " " + test_graph);
		} catch (IOException e) {
			fail(e.toString());
		}
	}
	
	@Test
	public void testAdd() {
		try {
			testObject.add(test_graph, test_subject, test_predicate, test_object);
			// get the data
			throw new UnsupportedOperationException("get the data from sparql");
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}
	}
	
	@Test
	public void testDelete() {
		try {
			testObject.add(test_graph, test_subject, test_predicate, test_object);
			testObject.delete(test_graph, test_subject, test_predicate, test_object);
			// get the data
			throw new UnsupportedOperationException("get the data from sparql");
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}
	}
	
	@After
	public void shutdown(){
	   // shutdown the 4store httpd
		httpd.destroy();
		backend.destroy();
	}

}
