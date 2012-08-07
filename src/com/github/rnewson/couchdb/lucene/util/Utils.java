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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.store.Directory;

public class Utils {

	public static String getLogger(final Class<?> clazz, final String suffix) {
		return clazz.getCanonicalName() + "." + suffix;
	}

	public static boolean getStaleOk(String req) {
		return "ok".equals(getParamsFromUrl(req).get("stale"));
	}

	public static Field text(final String name, final String value,
			final boolean store) {
		return new Field(name, value, store ? Store.YES : Store.NO,
				Field.Index.ANALYZED);
	}

	public static Field token(final String name, final String value,
			final boolean store) {
		return new Field(name, value, store ? Store.YES : Store.NO,
				Field.Index.NOT_ANALYZED_NO_NORMS);
	}

	public static String urlEncode(final String path) {
		try {
			return URLEncoder.encode(path, "UTF-8");
		} catch (final UnsupportedEncodingException e) {
			throw new Error("UTF-8 support missing!");
		}
	}

	public static long directorySize(final Directory dir) throws IOException {
		long result = 0;
		for (final String name : dir.listAll()) {
			result += dir.fileLength(name);
		}
		return result;
	}

	/**
	 * Split a string on commas but respect commas inside quotes.
	 * 
	 * @param str
	 * @return
	 */
	public static String[] splitOnCommas(final String str) {
		return str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	}

	/**
	 * Extracts the params from the url
	 * 
	 * @param req
	 * @return
	 */
	public static HashMap<String, Object> getParamsFromUrl(String req) {
		HashMap<String, Object> params = new HashMap<String, Object>();
		if (req.indexOf("?") > 0) {
			// Extracting string after ?
			String paramList = req.substring(req.indexOf("?") + 1);
			//TODO needs to be completed
		}

		return params;
	}
}
