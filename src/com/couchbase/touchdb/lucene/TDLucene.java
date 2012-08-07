package com.couchbase.touchdb.lucene;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.json.JSONException;
import org.json.JSONObject;

import android.os.AsyncTask;

import com.couchbase.touchdb.TDServer;
import com.couchbase.touchdb.ektorp.TouchDBHttpClient;
import com.couchbase.touchdb.router.TDURLStreamHandlerFactory;
import com.github.rnewson.couchdb.lucene.Config;
import com.github.rnewson.couchdb.lucene.DatabaseIndexer;
import com.github.rnewson.couchdb.lucene.PathParts;
import com.github.rnewson.couchdb.lucene.couchdb.Couch;
import com.github.rnewson.couchdb.lucene.couchdb.Database;
import com.github.rnewson.couchdb.lucene.util.ServletUtils;

public class TDLucene {

	static {
		TDURLStreamHandlerFactory.registerSelfIgnoreError();
	}

	private final Map<Database, DatabaseIndexer> indexers = new HashMap<Database, DatabaseIndexer>();
	private final Map<Database, Thread> threads = new HashMap<Database, Thread>();
	private TouchDBHttpClient client;

	public TDLucene(TDServer server) throws MalformedURLException {
		this.client = Config.getClient(server);
	}

	private synchronized DatabaseIndexer getIndexer(final Database database)
			throws IOException, JSONException {
		DatabaseIndexer result = indexers.get(database);
		Thread thread = threads.get(database);
		if (result == null || thread == null || !thread.isAlive()) {
			result = new DatabaseIndexer(client, Config.getDir(), database);
			thread = new Thread(result);
			thread.start();
			result.awaitInitialization();
			if (result.isClosed()) {
				return null;
			} else {
				indexers.put(database, result);
				threads.put(database, thread);
			}
		}

		return result;
	}

	private Couch getCouch(TDLuceneRequest req) throws IOException {
		final String sectionName = new PathParts(req).getKey();
		// final Configuration section = ini.getSection(sectionName);
		// if (!section.containsKey("url")) {
		// throw new FileNotFoundException(sectionName
		// + " is missing or has no url parameter.");
		// }
		return new Couch(client, Config.url);
	}

	private DatabaseIndexer getIndexer(TDLuceneRequest req) throws IOException,
			JSONException {
		final Couch couch = getCouch(req);
		final Database database = couch.getDatabase("/"
				+ new PathParts(req).getDatabaseName());
		return getIndexer(database);
	}

	public void fetch(TDLuceneRequest req, Callback callback)
			throws IOException, JSONException {
		TDLuceneAsync async = new TDLuceneAsync(callback);
		async.execute(req);
	}

	private class TDLuceneAsync extends
			AsyncTask<TDLuceneRequest, Integer, Object> {

		private Callback callback;

		public TDLuceneAsync(Callback callback) {
			this.callback = callback;
		}

		@Override
		protected Object doInBackground(TDLuceneRequest... params) {

			TDLuceneRequest req = params[0];
			try {

				DatabaseIndexer indexer = getIndexer(req);
				ObjectNode resp = JsonNodeFactory.instance.objectNode();
				if (indexer == null) {
					ServletUtils.sendJsonError(req, resp, 500,
							"error_creating_index");
					return new JSONObject(resp.toString());
				} else {
					String json = indexer.search(req);
					return new JSONObject(json);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}

			return null;
		}

		@Override
		protected void onPostExecute(Object result) {

			if (result != null && callback != null) {
				callback.onSucess(result);
			}
		}
	}

	public static abstract class Callback {

		public abstract void onSucess(Object resp);

		public abstract void onError(Object resp);
	}

}
