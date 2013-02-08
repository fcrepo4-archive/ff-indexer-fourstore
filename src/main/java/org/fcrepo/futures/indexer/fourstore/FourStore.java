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
		m_baseUri = "http://" + host + ":" + port + "/";
	}
	private HttpClient client = new DefaultHttpClient();
    public HttpResponse add(String graph, String subject, String predicate, String object) 
    		throws ClientProtocolException, IOException {
    	HttpPost request = new HttpPost(m_baseUri + "update/");
    	UrlEncodedFormEntity entity = getAddEntity(graph, subject, predicate, object);
    	System.out.println(request.getURI());
    	request.setEntity(entity);
    	request.setHeader("Content-Type", "application/x-www-form-urlencoded");
    	System.out.println(request.getFirstHeader("Content-Type"));
    	HttpResponse response = client.execute(request);
    	return response;
    }
    
    public HttpResponse delete(String graph, String subject, String predicate, String object)
    		throws ClientProtocolException, IOException {
    	HttpPost request = new HttpPost(m_baseUri + "update/");
    	System.out.println(request.getURI());
    	UrlEncodedFormEntity entity = getDeleteEntity(graph, subject, predicate, object);
    	request.setEntity(entity);
    	request.setHeader("Content-Type", "application/x-www-form-urlencoded");
    	HttpResponse response = client.execute(request);
    	return response;
    }
    
    private UrlEncodedFormEntity getPostAddEntity(String graph, String subject, String predicate, String object) {
    	try {
	    	// but what if the object is a literal?
	    	String message = String.format("<%s> <%s> <%s> .", subject, predicate, object);
	    	System.out.println(message);
	    	NameValuePair[] pairs = new NameValuePair[]{new BasicNameValuePair("graph",graph),
	    			                                   new BasicNameValuePair("data",message),
	    			                                   new BasicNameValuePair("mime-type", "application/x-turtle")};
	    	UrlEncodedFormEntity entity = new UrlEncodedFormEntity(Arrays.asList(pairs), "UTF-8");
	    	return entity;
		} catch (UnsupportedEncodingException e) {
			// hush you
	    	return null;
		}
    }
    
    private UrlEncodedFormEntity getAddEntity(String graph, String subject, String predicate, String object) {
    	try {
	    	// but what if the object is a literal?
    		String format = "INSERT DATA { GRAPH <%s> { <%s> <%s> <%s> } }";
    		String query = String.format(format, graph, subject, predicate, object);
	    	System.out.println(query);
	    	NameValuePair[] pairs = new NameValuePair[]{new BasicNameValuePair("update",query)};
	    	UrlEncodedFormEntity entity = new UrlEncodedFormEntity(Arrays.asList(pairs), "UTF-8");
	    	return entity;
		} catch (UnsupportedEncodingException e) {
			// hush you
	    	return null;
		}
    }

    private UrlEncodedFormEntity getDeleteEntity(String graph, String subject, String predicate, String object) {
    	try {
	    	// but what if the object is a literal?
    		String format = "DELETE DATA { GRAPH <%s> { <%s> <%s> <%s> } }";
    		String query = String.format(format, graph, subject, predicate, object);
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
