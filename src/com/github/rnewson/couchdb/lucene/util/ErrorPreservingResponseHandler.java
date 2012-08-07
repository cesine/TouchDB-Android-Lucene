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

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;

public final class ErrorPreservingResponseHandler implements
		ResponseHandler<String> {

	public String handleResponse(final org.ektorp.http.HttpResponse response)
			throws HttpResponseException, IOException {
		String str = IOUtils.toString(response.getContent());
		if (response.getCode() >= 300) {
			throw new HttpResponseException(response.getCode(), str);
		}

		return str;
	}

	@Override
	public String handleResponse(HttpResponse response)
			throws ClientProtocolException, IOException {
		return null;
	}

}
