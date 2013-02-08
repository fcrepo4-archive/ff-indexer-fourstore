package org.fcrepo.futures.indexer.fourstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;

public class FourStore {
    private static final byte[] DISCARD = new byte[1024];
	private final String m_updateUri;

	private HttpClient client = new DefaultHttpClient(new PoolingClientConnectionManager());

	public FourStore() {
		this("localhost", 8000);
	}
	
	public FourStore(String host, int port) {
		m_updateUri = "http://" + host + ":" + port + "/update/";
	}
    public StatusLine add(String graph, String subject, String predicate, String object) 
    		throws ClientProtocolException, IOException {
    	return update("INSERT", graph, subject, predicate, object);
    }
    
    public StatusLine delete(String graph, String subject, String predicate, String object)
    		throws ClientProtocolException, IOException {
    	return update("DELETE", graph, subject, predicate, object);
    }
    
    private StatusLine update(String op, String graph, String subject, String predicate, String object)
    		throws ClientProtocolException, IOException {
    	HttpPost request = new HttpPost(m_updateUri);
    	System.out.println(request.getURI());
    	UrlEncodedFormEntity entity = getUpdateEntity(op, graph, subject, predicate, object);
    	request.setEntity(entity);
    	request.setHeader("Content-Type", "application/x-www-form-urlencoded");
    	HttpResponse response = client.execute(request);
    	InputStream content = response.getEntity().getContent(); // to make sure the connection is released
    	while (content.read(DISCARD) > -1){};
    	return response.getStatusLine();
    }
    
    private UrlEncodedFormEntity getUpdateEntity(String op, String graph, String subject, String predicate, String object) {
    	try {
	    	// but what if the object is a literal?
    		String format = "%s DATA { GRAPH <%s> { <%s> <%s> <%s> } }";
    		String query = String.format(format, op, graph, subject, predicate, object);
	    	System.out.println(query);
	    	NameValuePair[] pairs = new NameValuePair[]{new BasicNameValuePair("update",query)};
	    	UrlEncodedFormEntity entity = new UrlEncodedFormEntity(Arrays.asList(pairs), "UTF-8");
	    	return entity;
		} catch (UnsupportedEncodingException e) {
			// hush you
	    	return null;
		}
    }
    
}
