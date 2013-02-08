package org.fcrepo.futures.indexer.fourstore;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FourstoreTests {
	HttpClient client = new DefaultHttpClient();
	int port = 8000;
	String hostname = "localhost";
	Process backend;
	Process httpd;
	String test_graph = "fcr4test";
	FourStore testObject = new FourStore(hostname, port);
	String test_subject = "http://fedora.info/changeme:1001";
	String test_predicate = "http://fedora.info/fedora-system:hasDatastream";
	String test_object = "http://fedora.info/changeme:1001/contentds";
	@Before
	public void setup(){
		try {
			// start the 4store httpd
			Process p = Runtime.getRuntime().exec("/usr/local/bin/4s-backend-destroy " + test_graph);
			synchronized (p) { p.wait(); };
			p.exitValue();
			p = Runtime.getRuntime().exec("/usr/local/bin/4s-backend-setup " + test_graph);
			synchronized (p) { p.wait(); };
			p.exitValue();
			backend = Runtime.getRuntime().exec("/usr/local/bin/4s-backend " + test_graph);
			synchronized (backend) { backend.wait(); };
			backend.exitValue();
			httpd = Runtime.getRuntime().exec("/usr/local/bin/4s-httpd -p " + port + " " + test_graph);
			synchronized (httpd) { httpd.wait(); };
			httpd.exitValue();
			HttpDelete request = new HttpDelete("http://" + hostname + ":" + port + "/data/" + test_graph);
			HttpResponse response = client.execute(request);
			response.getEntity().consumeContent();
			if (response.getStatusLine().getStatusCode() != 200) {
				fail("unexpected response code when deleting old graph: " + response.getStatusLine());
			}
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	private String getTheData() throws Exception {
		String sparql = "SELECT * WHERE { ?s ?p ?o } LIMIT 10";
		BasicNameValuePair query = new BasicNameValuePair("query", sparql);
		BasicNameValuePair graph = new BasicNameValuePair("graph", test_graph);
		BasicNameValuePair[] pairs = new BasicNameValuePair[]{query, graph};
		UrlEncodedFormEntity entity = new UrlEncodedFormEntity(Arrays.asList(pairs), "UTF-8");
		HttpPost request = new HttpPost("http://" + hostname + ":" + port + "/sparql/");
		request.setEntity(entity);
		HttpResponse response = client.execute(request);
		System.out.println("---Dumping query response content---");
		String body = dumpToString(response.getEntity().getContent());
		return body;
	}
	
	@Test
	public void testAdd() {
		try {
			StatusLine response = testObject.add(test_graph, test_subject, test_predicate, test_object);
			if (response.getStatusCode() != 200) {
				fail("unexpected http status for add: " + response + " " + response.getReasonPhrase());
			}
			// get the data
			String body = getTheData();
			assertTrue(body.indexOf(test_subject) > -1);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}
	}
	
	private String dumpToString(InputStream in) throws IOException {
		InputStreamReader r = new InputStreamReader(in, "UTF-8");
		char [] buf = new char[1024];
		int len = 0;
		StringBuffer buffer = new StringBuffer();
		while ((len = r.read(buf)) > -1) {
			buffer.append(buf,0,len);
		}
		in.close();
		String result = buffer.toString();
		return result;
	}
	
	@Test
	public void testDelete() {
		try {
			StatusLine response = testObject.add(test_graph, test_subject, test_predicate, test_object);
			if (response.getStatusCode() != 200) {
				fail("unexpected http status for add: " + response.getStatusCode());
			}
			response = testObject.delete(test_graph, test_subject, test_predicate, test_object);
			if (response.getStatusCode() != 200) {
				fail("unexpected http status for delete: " + response.getStatusCode());
			}
			// get the data
			String body = getTheData();
			assertTrue(body.indexOf(test_subject) == -1);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}
	}
	
	@After
	public void shutdown() throws Exception{
		// delete the graph!
		
	   // shutdown the 4store httpd
		httpd.getOutputStream().close();
		synchronized(httpd) { httpd.waitFor(); }
		httpd.destroy();
		synchronized(httpd) { httpd.waitFor(); }
		Process p = Runtime.getRuntime().exec("/usr/local/bin/4s-delete-model " + test_graph);
		synchronized (p) { p.wait(); };
		backend.getOutputStream().close();
		synchronized(backend) { backend.waitFor(); }
		backend.destroy();
		synchronized(backend) { backend.waitFor(); }
		Runtime rt = Runtime.getRuntime();
		// ridiculously, the only way I can get pipes to work is by using an array of String, and explicitly shelling out
		String[] cmd = new String[]{"/bin/sh", "-c", "ps -ef | /usr/bin/grep 4s- | /usr/bin/grep " + test_graph};
		p = rt.exec(cmd);
		InputStream in = p.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		synchronized(p) { p.waitFor(); }
		String line = null;
		while ((line = reader.readLine()) != null) {
			line = line.trim();
			String [] parts = line.split("\\s+");
			String pid = parts[1];
			String kill_cmd = " kill " + pid;
			Process kill = rt.exec(kill_cmd);
			synchronized(kill) { kill.waitFor(); }
		}
				
	}

}
