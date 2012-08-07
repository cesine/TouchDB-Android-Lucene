package com.github.rnewson.couchdb.lucene.util;

/**
 * Copyright 2009 Robert Newson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.codehaus.jackson.node.ObjectNode;
import org.json.JSONException;
import org.json.JSONObject;

import com.couchbase.touchdb.lucene.TDLuceneRequest;

public final class ServletUtils {

	public static boolean getBooleanParameter(String req,
			final String parameterName) {
		return Boolean.parseBoolean((String) Utils.getParamsFromUrl(req).get(
				parameterName));
	}

	public static int getIntParameter(String req, final String parameterName,
			final int defaultValue) {
		final String result = (String) Utils.getParamsFromUrl(req).get(
				parameterName);
		return result != null ? Integer.parseInt(result) : defaultValue;
	}

	public static long getLongParameter(String req, final String parameterName,
			final long defaultValue) {
		final String result = (String) Utils.getParamsFromUrl(req).get(
				parameterName);
		return result != null ? Long.parseLong(result) : defaultValue;
	}

	public static String getParameter(String req, final String parameterName,
			final String defaultValue) {
		final String result = (String) Utils.getParamsFromUrl(req).get(
				parameterName);
		return result != null ? result : defaultValue;
	}

	public static void setResponseContentTypeAndEncoding(TDLuceneRequest req,
			ObjectNode resp) {
		// final String accept = req.getHeader("Accept");
		// if (getBooleanParameter(req, "force_json")
		// || (accept != null && accept.contains("application/json"))) {
		// resp.setContentType("application/json");
		// } else {
		// resp.setContentType("text/plain");
		// }
		// if (!resp.containsHeader("Vary")) {
		// resp.addHeader("Vary", "Accept");
		// }
		// resp.setCharacterEncoding("utf-8");
	}

	public static void sendJsonError(TDLuceneRequest request,
			ObjectNode response, final int code, final String reason)
			throws IOException, JSONException {
		final JSONObject obj = new JSONObject();
		obj.put("reason", reason);
		sendJsonError(request, response, code, obj);
	}

	public static void sendJsonError(TDLuceneRequest request,
			ObjectNode response, final int code, final JSONObject error)
			throws IOException, JSONException {
		// setResponseContentTypeAndEncoding(request, response);
		// response.setHeader(HttpHeaders.CACHE_CONTROL,
		// "must-revalidate,no-cache,no-store");
		// response.setStatus(code);
		// error.put("code", code);
		//
		// final Writer writer = response.getWriter();
		// try {
		// writer.write(error.toString());
		// writer.write("\r\n");
		// } finally {
		// writer.close();
		// }

		setStatus(response, code);
		response.put("error", error.toString());
	}

	public static void sendJson(TDLuceneRequest req, ObjectNode resp,
			final JSONObject json) throws IOException {
		// setResponseContentTypeAndEncoding(req, resp);
		// final Writer writer = resp.getWriter();
		// try {
		// writer.write(json.toString() + "\r\n");
		// } finally {
		// writer.close();
		// }
		resp.put("data", json.toString());
	}

	public static void sendJsonSuccess(TDLuceneRequest req, ObjectNode resp)
			throws IOException {
		// setResponseContentTypeAndEncoding(req, resp);
		// final Writer writer = resp.getWriter();
		// try {
		// writer.write("{\"ok\": true}\r\n");
		// } finally {
		// writer.close();
		// }
		resp.put("ok", true);
	}

	public static void setStatus(ObjectNode resp, int code) {
		resp.put("status", code);
	}

}
