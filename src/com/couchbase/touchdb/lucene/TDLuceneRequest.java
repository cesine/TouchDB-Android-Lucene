package com.couchbase.touchdb.lucene;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.node.ObjectNode;

public class TDLuceneRequest {

	private String url;
	private ObjectNode data;
	private Map<String, String> header = new HashMap<String, String>();
	private Map<String, String> params = new HashMap<String, String>();

	public TDLuceneRequest() {

	}

	/**
	 * Add a parameter to the request. Forces string input for value
	 * 
	 * @param param
	 * @param value
	 */
	public TDLuceneRequest addParam(String param, String value) {
		this.params.put(param, value);
		return this;
	}

	public String getParameter(String param) {
		return getParamAsString(param);
	}

	public boolean getParamAsBoolean(String param) {
		return Boolean.parseBoolean(params.get(param));
	}

	public String getParamAsString(String param) {
		return params.get(param);
	}

	public TDLuceneRequest addHeader(String param, String value) {
		this.header.put(param, value);
		return this;
	}

	public String getHeader(String param) {
		return this.header.get(param);
	}

	public TDLuceneRequest addData(ObjectNode json) {
		this.data = json;
		return this;
	}

	public ObjectNode getData() {
		return this.data;
	}

	public TDLuceneRequest setUrl(String url) {
		this.url = url;
		return this;
	}

	public String getUrl() {
		return url;
	}

	/**
	 * Provides the url ignoring the params
	 * 
	 * @return
	 */
	public String getRequestURI() {
		return getUrl();
	}
}
