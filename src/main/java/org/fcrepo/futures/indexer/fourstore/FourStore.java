package org.fcrepo.futures.indexer.fourstore;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;

public class FourStore {
	private final String m_baseUri;
	public FourStore() {
		this("localhost", 8000);
	}
	
	public FourStore(String host, int port) {
		m_baseUri = "http://" + host + ":" + port + "/data/";
	}
	private HttpClient client = new DefaultHttpClient();
    public HttpResponse add(String graph, String subject, String predicate, String object) throws ClientProtocolException, IOException {
    	HttpPost request = new HttpPost(m_baseUri + graph);
    	UrlEncodedFormEntity entity = getEntity(graph, subject, predicate, object);
    	request.setEntity(entity);
    	return client.execute(request);
    }
    
    public HttpResponse delete(String g, String s, String p, String o) throws ClientProtocolException, IOException {
    	HttpDelete request = new HttpDelete(m_baseUri + getQuery(g, s, p, o));
    	return client.execute(request);
    }
    
    private UrlEncodedFormEntity getEntity(String graph, String subject, String predicate, String object) {
    	try {
	    	// but what if the object is a literal?
	    	String message = String.format("<%s><%s><%s>", graph, subject, predicate, object);
	    	NameValuePair[] pairs = new NameValuePair[]{new BasicNameValuePair("graph",graph),
	    			                                   new BasicNameValuePair("data",message)};
	    	UrlEncodedFormEntity entity = new UrlEncodedFormEntity(Arrays.asList(pairs), "UTF-8");
	    	return entity;
		} catch (UnsupportedEncodingException e) {
			// hush you
	    	return null;
		}
    }
    
    private String getQuery(String g, String s, String p, String o) {
    	try {
    		g = URLEncoder.encode(g, "UTF-8");
    		s = URLEncoder.encode(s, "UTF-8");
    		p = URLEncoder.encode(p, "UTF-8");
    		o = URLEncoder.encode(o, "UTF-8");
    		return String.format("graph=%s&data=%s%s%s", g,s,p,o); 
		} catch (UnsupportedEncodingException e) {
			// hush you
	    	return null;
		}
    }
    
}
