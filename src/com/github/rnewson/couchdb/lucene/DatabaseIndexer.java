package com.github.rnewson.couchdb.lucene;

import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.ReaderUtil;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.android.util.ChangesFeedAsyncTask;
import org.ektorp.changes.ChangesCommand;
import org.ektorp.changes.ChangesFeed;
import org.ektorp.changes.DocumentChange;
import org.ektorp.impl.StdCouchDbInstance;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mozilla.javascript.ClassShutter;
import org.mozilla.javascript.Context;

import android.util.Log;

import com.couchbase.touchdb.ektorp.TouchDBHttpClient;
import com.couchbase.touchdb.lucene.TDLuceneRequest;
import com.github.rnewson.couchdb.lucene.couchdb.CouchDocument;
import com.github.rnewson.couchdb.lucene.couchdb.Database;
import com.github.rnewson.couchdb.lucene.couchdb.DesignDocument;
import com.github.rnewson.couchdb.lucene.couchdb.UpdateSequence;
import com.github.rnewson.couchdb.lucene.couchdb.View;
import com.github.rnewson.couchdb.lucene.util.Analyzers;
import com.github.rnewson.couchdb.lucene.util.Constants;
import com.github.rnewson.couchdb.lucene.util.ServletUtils;
import com.github.rnewson.couchdb.lucene.util.StopWatch;
import com.github.rnewson.couchdb.lucene.util.Utils;

public final class DatabaseIndexer implements Runnable {

	private class IndexState {

		private final DocumentConverter converter;
		private boolean readerDirty;
		private String etag;

		private final Analyzer analyzer;
		private UpdateSequence pending_seq;
		private IndexReader reader;
		private final IndexWriter writer;
		private final Database database;
		private final View view;

		public IndexState(final DocumentConverter converter,
				final IndexWriter writer, final Analyzer analyzer,
				final Database database, final View view) {
			this.converter = converter;
			this.writer = writer;
			this.analyzer = analyzer;
			this.database = database;
			this.view = view;
		}

		public synchronized IndexReader borrowReader(final boolean staleOk)
				throws IOException, JSONException {
			blockForLatest(staleOk);
			if (reader == null) {
				etag = newEtag();
			}

			if (reader != null) {
				reader.decRef();
			}
			reader = IndexReader.open(writer, !staleOk);
			if (readerDirty) {
				etag = newEtag();
				readerDirty = false;
			}

			reader.incRef();
			return reader;
		}

		public IndexSearcher borrowSearcher(final boolean staleOk)
				throws IOException, JSONException {
			return new IndexSearcher(borrowReader(staleOk));
		}

		public void returnReader(final IndexReader reader) throws IOException {
			reader.decRef();
		}

		public void returnSearcher(final IndexSearcher searcher)
				throws IOException {
			returnReader(searcher.getIndexReader());
		}

		public Query parse(final String query, final Operator operator,
				final Analyzer analyzer) throws ParseException, JSONException {
			final QueryParser parser = new CustomQueryParser(Constants.VERSION,
					Constants.DEFAULT_FIELD, analyzer);
			parser.setDefaultOperator(operator);
			parser.setAllowLeadingWildcard(Config.allowLeadingWildcard);
			parser.setLowercaseExpandedTerms(Config.lowercaseExpandedTerms);
			return parser.parse(query);
		}

		public Analyzer analyzer(final String analyzerName)
				throws JSONException {
			return analyzerName == null ? this.analyzer : Analyzers
					.getAnalyzer(analyzerName);
		}

		private synchronized void close() throws IOException {
			if (reader != null)
				reader.close();
			if (writer != null)
				writer.rollback();
		}

		private synchronized String getEtag() {
			return etag;
		}

		public UUID getUuid() throws JSONException, IOException {
			return database.getUuid();
		}

		public String getDigest() {
			return view.getDigest();
		}

		private String newEtag() {
			return Long.toHexString(now());
		}

		private synchronized boolean notModified(TDLuceneRequest req) {
			return etag != null && etag.equals(req.getHeader("If-None-Match"));
		}

		private void blockForLatest(final boolean staleOk) throws IOException,
				JSONException {
			if (staleOk) {
				return;
			}
			final UpdateSequence latest = database.getLastSequence();
			synchronized (this) {
				long timeout = getSearchTimeout();
				while (pending_seq.isEarlierThan(latest)) {
					try {
						final long start = System.currentTimeMillis();
						wait(timeout);
						timeout -= (System.currentTimeMillis() - start);
						if (timeout <= 0) {
							throw new IOException("Search timed out.");
						}
					} catch (final InterruptedException e) {
						throw new IOException("Search timed out.");
					}
				}
			}
		}

		private synchronized void setPendingSequence(final UpdateSequence seq) {
			pending_seq = seq;
			notifyAll();
		}

		@Override
		public String toString() {
			return writer.getDirectory().toString();
		}
	}

	private final class RestrictiveClassShutter implements ClassShutter {

		public boolean visibleToScripts(final String fullClassName) {
			return false;
		}
	}

	public static File uuidDir(final File root, final UUID uuid) {
		return new File(root, uuid.toString());
	}

	public static File viewDir(final File root, final UUID uuid,
			final String digest, final boolean mkdirs) throws IOException {
		final File uuidDir = uuidDir(root, uuid);
		final File viewDir = new File(uuidDir, digest);
		if (mkdirs) {
			viewDir.mkdirs();
		}
		return viewDir;
	}

	private static long now() {
		return System.nanoTime();
	}

	private final TouchDBHttpClient client;

	private boolean closed;

	private Context context;

	private final Database database;

	private UpdateSequence ddoc_seq;

	private long lastCommit;

	private final CountDownLatch latch = new CountDownLatch(1);

	private final Map<String, View> paths = new HashMap<String, View>();

	// private HttpUriRequest req;
	private ChangesFeedAsyncTask changesFeed;

	private final File root;

	private UpdateSequence since;

	private final Map<View, IndexState> states = Collections
			.synchronizedMap(new HashMap<View, IndexState>());

	private UUID uuid;

	private String LOG_TAG;

	private String database_name;

	private CouchDbConnector stdCouchDBConnector;

	// private final HierarchicalINIConfiguration ini;

	public DatabaseIndexer(final TouchDBHttpClient client, final File root,
			final Database database) throws IOException, JSONException {
		this.client = client;
		this.root = root;
		this.database = database;
		// this.ini = ini;
		this.database_name = database.getInfo().getName();
		this.LOG_TAG = DatabaseIndexer.class.getName() + "."
				+ this.database_name;
		this.stdCouchDBConnector = new StdCouchDbInstance(client)
				.createConnector(database_name, false);
	}

	public void admin(TDLuceneRequest req, ObjectNode resp) throws IOException,
			JSONException {
		final IndexState state = getState(req, resp);
		if (state == null)
			return;
		final String command = new PathParts(req).getCommand();

		if ("_expunge".equals(command)) {
			Log.i(LOG_TAG, "Expunging deletes from " + state);
			state.writer.forceMergeDeletes(false);
			ServletUtils.setStatus(resp, 202);
			ServletUtils.sendJsonSuccess(req, resp);
			return;
		}

		if ("_optimize".equals(command)) {
			Log.i(LOG_TAG, "Optimizing " + state);
			state.writer.forceMerge(1, false);
			ServletUtils.setStatus(resp, 202);
			ServletUtils.sendJsonSuccess(req, resp);
			return;
		}

		ServletUtils.sendJsonError(req, resp, 400, "bad_request");
	}

	public void awaitInitialization() {
		try {
			latch.await();
		} catch (final InterruptedException e) {
			// Ignore.
		}
	}

	public void info(TDLuceneRequest req, ObjectNode resp) throws IOException,
			JSONException {
		final IndexState state = getState(req, resp);
		if (state == null)
			return;
		final IndexReader reader = state.borrowReader(isStaleOk(req));
		try {
			final JSONObject result = new JSONObject();
			result.put("current", reader.isCurrent());
			result.put("disk_size", Utils.directorySize(reader.directory()));
			result.put("doc_count", reader.numDocs());
			result.put("doc_del_count", reader.numDeletedDocs());
			result.put("uuid", state.getUuid());
			result.put("digest", state.getDigest());
			final JSONArray fields = new JSONArray();
			final Iterator<FieldInfo> it = ReaderUtil.getMergedFieldInfos(
					reader).iterator();
			while (it.hasNext()) {
				final FieldInfo fieldInfo = it.next();
				if (fieldInfo.name.startsWith("_")) {
					continue;
				}
				if (fieldInfo.isIndexed) {
					fields.put(fieldInfo.name);
				}
			}
			result.put("fields", fields);
			result.put("last_modified",
					Long.toString(IndexReader.lastModified(reader.directory())));
			result.put("optimized", reader.isOptimized());
			result.put("ref_count", reader.getRefCount());

			final JSONObject info = new JSONObject();
			info.put("code", 200);
			info.put("json", result);

			ServletUtils.setResponseContentTypeAndEncoding(req, resp);
			// final Writer writer = resp.getWriter();
			// try {
			// writer.write(result.toString());
			// } finally {
			// writer.close();
			// }
		} finally {
			state.returnReader(reader);
		}
	}

	public void run() {
		if (closed) {
			throw new IllegalStateException("closed!");
		}

		try {
			init();
		} catch (final Exception e) {
			Log.w(LOG_TAG, "Exiting after init() raised exception.", e);
			close();
			return;
		}

		Thread changesFeed = new Thread(new Runnable() {

			@Override
			public void run() {
				ChangesFeed feed = null;
				try {

					String s = "start".equals(since.toString()) ? "0" : since
							.toString();
					ChangesCommand changesCommand = new ChangesCommand.Builder()
							.includeDocs(true).since(s).build();

					List<DocumentChange> changes = stdCouchDBConnector
							.changes(changesCommand);
					for (DocumentChange docChange : changes) {

						JSONObject json;

						json = new JSONObject(docChange.getDoc());

						if (json.has("error")) {
							Log.w(LOG_TAG, "Indexing stopping due to error: "
									+ json);
							return;
						}

						if (json.has("last_seq")) {
							Log.w(LOG_TAG, "End of changes detected.");
							return;
						}

						final UpdateSequence seq = UpdateSequence
								.parseUpdateSequence(""
										+ docChange.getSequence());
						// UpdateSequence.parseUpdateSequence(json.getString("seq"));
						final String id = json.getString("_id");
						CouchDocument doc = null;
						// if (!json.isNull("doc")) {
						doc = new CouchDocument(json); // .getJSONObject("doc")
						// }

						if (id.startsWith("_design")) {
							if (seq.isLaterThan(ddoc_seq)) {
								Log.i(LOG_TAG,
										"Exiting due to design document change.");
								return;
							}
						}

						if (doc.isDeleted()) {
							for (final IndexState state : states.values()) {
								state.writer
										.deleteDocuments(new Term("_id", id));
								state.setPendingSequence(seq);
								state.readerDirty = true;
							}
						} else {
							for (final Entry<View, IndexState> entry : states
									.entrySet()) {
								final View view = entry.getKey();
								final IndexState state = entry.getValue();

								if (seq.isLaterThan(state.pending_seq)) {
									final Collection<Document> docs;
									try {
										docs = state.converter.convert(doc,
												view.getDefaultSettings(),
												database);
									} catch (final Exception e) {
										Log.w(LOG_TAG,
												id + " caused "
														+ e.getMessage());
										return;
									}

									state.writer.updateDocuments(new Term(
											"_id", id), docs, view
											.getAnalyzer());
									state.setPendingSequence(seq);
									state.readerDirty = true;
								}
							}
						}
					}

					commitAll();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (JSONException e) {
					e.printStackTrace();
				} finally {
					if (feed != null) {
						feed.cancel();
					}
				}
			}
		});
		changesFeed.start();
	}

	public String search(TDLuceneRequest req) throws IOException, JSONException {

		final ObjectNode resp = JsonNodeFactory.instance.objectNode();

		final IndexState state = getState(req, resp);
		if (state == null)
			return resp.toString();

		final IndexSearcher searcher = state.borrowSearcher(isStaleOk(req));
		final String etag = state.getEtag();
		final FastVectorHighlighter fvh = new FastVectorHighlighter(true, true);
		final JSONArray result = new JSONArray();
		try {
			if (state.notModified(req)) {
				ServletUtils.setStatus(resp, 304);
				return resp.toString();
			}
			for (final String queryString : getQueryStrings(req)) {
				final Analyzer analyzer = state.analyzer(req
						.getParameter("analyzer"));
				final Operator operator = "and".equalsIgnoreCase(req
						.getParameter("default_operator")) ? Operator.AND
						: Operator.OR;
				final Query q = state.parse(queryString, operator, analyzer);

				final JSONObject queryRow = new JSONObject();
				queryRow.put("q", q.toString());
				if (getBooleanParameter(req, "debug")) {
					queryRow.put("plan", QueryPlan.toPlan(q));
					queryRow.put("analyzer", analyzer.getClass());
				}
				queryRow.put("etag", etag);
				if (getBooleanParameter(req, "rewrite")) {
					final Query rewritten_q = q.rewrite(searcher
							.getIndexReader());
					queryRow.put("rewritten_q", rewritten_q.toString());

					final JSONObject freqs = new JSONObject();

					final Set<Term> terms = new HashSet<Term>();
					rewritten_q.extractTerms(terms);
					for (final Object term : terms) {
						final int freq = searcher.docFreq((Term) term);
						freqs.put(term.toString(), freq);
					}
					queryRow.put("freqs", freqs);
				} else {
					// Perform the search.
					final TopDocs td;
					final StopWatch stopWatch = new StopWatch();

					final boolean include_docs = getBooleanParameter(req,
							"include_docs");
					final int highlights = getIntParameter(req, "highlights", 0);
					final int highlight_length = max(
							getIntParameter(req, "highlight_length", 18), 18); // min
																				// for
																				// fast
																				// term
																				// vector
																				// highlighter
																				// is
																				// 18
					final boolean include_termvectors = getBooleanParameter(
							req, "include_termvectors");
					final int limit = getIntParameter(req, "limit",
							Config.limit);
					final Sort sort = CustomQueryParser.toSort(req
							.getParameter("sort"));
					final int skip = getIntParameter(req, "skip", 0);

					final FieldSelector fieldSelector;
					if (req.getParameter("include_fields") == null) {
						fieldSelector = null;
					} else {
						final String[] fields = Utils.splitOnCommas(req
								.getParameter("include_fields"));
						fieldSelector = new MapFieldSelector(
								Arrays.asList(fields));
					}

					if (sort == null) {
						td = searcher.search(q, null, skip + limit);
					} else {
						td = searcher.search(q, null, skip + limit, sort);
					}
					stopWatch.lap("search");

					// Fetch matches (if any).
					final int max = Math.max(0,
							Math.min(td.totalHits - skip, limit));
					final JSONArray rows = new JSONArray();
					final String[] fetch_ids = new String[max];
					for (int i = skip; i < skip + max; i++) {
						final Document doc = searcher.doc(td.scoreDocs[i].doc,
								fieldSelector);

						final JSONObject row = new JSONObject();
						final JSONObject fields = new JSONObject();
						final JSONObject highlight_rows = new JSONObject();

						// Include stored fields.
						for (final Fieldable f : doc.getFields()) {
							if (!f.isStored()) {
								continue;
							}
							final String name = f.name();
							final Object value;
							if (f instanceof NumericField) {
								value = ((NumericField) f).getNumericValue();
							} else {
								value = f.stringValue();
							}
							if (value != null) {
								if ("_id".equals(name)) {
									row.put("id", value);
								} else {
									if (!fields.has(name)) {
										fields.put(name, value);
									} else {
										final Object obj = fields.get(name);
										if (obj instanceof String) {
											final JSONArray arr = new JSONArray();
											arr.put(obj);
											arr.put(value);
											fields.put(name, arr);
										} else {
											assert obj instanceof JSONArray;
											((JSONArray) obj).put(value);
										}
									}

									if (highlights > 0) {
										String[] frags = fvh.getBestFragments(
												fvh.getFieldQuery(q),
												searcher.getIndexReader(),
												td.scoreDocs[i].doc, name,
												highlight_length, highlights);
										highlight_rows.put(name, frags);
									}
								}
							}
						}

						if (!Float.isNaN(td.scoreDocs[i].score)) {
							row.put("score", td.scoreDocs[i].score);
						}// Include sort order (if any).
						if (td instanceof TopFieldDocs) {
							final FieldDoc fd = (FieldDoc) ((TopFieldDocs) td).scoreDocs[i];
							row.put("sort_order", fd.fields);
						}
						// Fetch document (if requested).
						if (include_docs) {
							fetch_ids[i - skip] = doc.get("_id");
						}
						if (fields.length() > 0) {
							row.put("fields", fields);
						}
						if (highlight_rows.length() > 0) {
							row.put("highlights", highlight_rows);
						}
						if (include_termvectors) {
							final JsonTermVectorMapper mapper = new JsonTermVectorMapper();
							searcher.getIndexReader().getTermFreqVector(
									td.scoreDocs[i].doc, mapper);
							row.put("termvectors", mapper.getObject());
						}

						rows.put(row);
					}
					// Fetch documents (if requested).
					if (include_docs && fetch_ids.length > 0) {
						final List<CouchDocument> fetched_docs = database
								.getDocuments(fetch_ids);
						for (int j = 0; j < max; j++) {
							final CouchDocument doc = fetched_docs.get(j);
							final JSONObject row = doc == null ? new JSONObject(
									"{\"error\":\"not_found\"}") : doc.asJson();
							rows.getJSONObject(j).put("doc", row);
						}
					}
					stopWatch.lap("fetch");

					queryRow.put("skip", skip);
					queryRow.put("limit", limit);
					queryRow.put("total_rows", td.totalHits);
					queryRow.put("search_duration",
							stopWatch.getElapsed("search"));
					queryRow.put("fetch_duration",
							stopWatch.getElapsed("fetch"));

					// Include sort info (if requested).
					if (td instanceof TopFieldDocs) {
						queryRow.put("sort_order", CustomQueryParser
								.toJSON(((TopFieldDocs) td).fields));
					}
					queryRow.put("rows", rows);
				}
				result.put(queryRow);
			}
		} catch (final ParseException e) {
			ServletUtils.sendJsonError(req, resp, 400,
					"Bad query syntax: " + e.getMessage());
			return resp.toString();
		} finally {
			state.returnSearcher(searcher);
		}

		// resp.setHeader("ETag", etag);
		// resp.setHeader("Cache-Control", "must-revalidate");
		ServletUtils.setResponseContentTypeAndEncoding(req, resp);

		final Object json = result.length() > 1 ? result : result
				.getJSONObject(0);
		final String callback = req.getParameter("callback");
		final String body;
		if (callback != null) {
			body = String.format("%s(%s)", callback, json);
		} else {
			if (json instanceof JSONObject) {
				final JSONObject obj = (JSONObject) json;
				body = getBooleanParameter(req, "debug") ? obj.toString(2)
						: obj.toString();
			} else {
				final JSONArray arr = (JSONArray) json;
				body = getBooleanParameter(req, "debug") ? arr.toString(2)
						: arr.toString();
			}
		}

		return json.toString();
	}

	private String[] getQueryStrings(TDLuceneRequest req) {
		return Utils.splitOnCommas(req.getParameter("q"));
	}

	private void close() {
		this.closed = true;

		for (final IndexState state : states.values()) {
			try {
				state.close();
			} catch (final IOException e) {
				Log.w(LOG_TAG, "Error while closing.", e);
			}
		}
		states.clear();
		if (context != null) {
			Context.exit();
			context = null;
		}
		latch.countDown();
	}

	public boolean isClosed() {
		return closed;
	}

	private void commitAll() throws IOException {
		for (final Entry<View, IndexState> entry : states.entrySet()) {
			final View view = entry.getKey();
			final IndexState state = entry.getValue();

			if (state.pending_seq.isLaterThan(getUpdateSequence(state.writer))) {
				final Map<String, String> userData = new HashMap<String, String>();
				userData.put("last_seq", state.pending_seq.toString());
				state.writer.commit(userData);
				Log.i(LOG_TAG, view + " now at update_seq " + state.pending_seq);
			}
		}
		lastCommit = now();
	}

	private boolean getBooleanParameter(TDLuceneRequest req,
			final String parameterName) {
		return Boolean.parseBoolean(req.getParameter(parameterName));
	}

	private int getIntParameter(TDLuceneRequest req,
			final String parameterName, final int defaultValue) {
		final String result = req.getParameter(parameterName);
		return result != null ? Integer.parseInt(result) : defaultValue;
	}

	private IndexState getState(TDLuceneRequest req, ObjectNode resp)
			throws IOException, JSONException {
		final View view = paths.get(toPath(req));
		if (view == null) {
			ServletUtils.sendJsonError(req, resp, 400, "no_such_view");
			return null;
		}

		final IndexState result = states.get(view);
		if (result == null) {
			ServletUtils.sendJsonError(req, resp, 400, "no_such_state");
		}
		return result;
	}

	private UpdateSequence getUpdateSequence(final Directory dir)
			throws IOException {
		if (!IndexReader.indexExists(dir)) {
			return UpdateSequence.START;
		}
		return getUpdateSequence(IndexReader.getCommitUserData(dir));
	}

	private UpdateSequence getUpdateSequence(final IndexWriter writer)
			throws IOException {
		return getUpdateSequence(writer.getDirectory());
	}

	private UpdateSequence getUpdateSequence(final Map<String, String> userData) {
		if (userData != null && userData.containsKey("last_seq")) {
			return UpdateSequence.parseUpdateSequence(userData.get("last_seq"));
		}
		return UpdateSequence.START;
	}

	private void init() throws IOException, JSONException {
		this.uuid = database.getOrCreateUuid();

		this.context = Context.enter();
		context.setClassShutter(new RestrictiveClassShutter());
		// Turned off compilation as this gives an error
		context.setOptimizationLevel(-1);
		// context.setOptimizationLevel(9);

		this.ddoc_seq = database.getInfo().getUpdateSequence();
		this.since = null;

		for (final DesignDocument ddoc : database.getAllDesignDocuments()) {
			for (final Entry<String, View> entry : ddoc.getAllViews()
					.entrySet()) {
				final String name = entry.getKey();
				final View view = entry.getValue();
				paths.put(toPath(ddoc.getId(), name), view);

				if (!states.containsKey(view)) {
					final Directory dir = FSDirectory.open(viewDir(view, true),
							new SingleInstanceLockFactory());
					final UpdateSequence seq = getUpdateSequence(dir);
					if (since == null) {
						since = seq;
					}
					since = seq.isEarlierThan(since) ? seq : since;
					Log.d(LOG_TAG, dir + " bumped since to " + since);

					final DocumentConverter converter = new DocumentConverter(
							context, view);
					final IndexWriter writer = newWriter(dir);

					final IndexState state = new IndexState(converter, writer,
							view.getAnalyzer(), database, view);
					state.setPendingSequence(seq);
					states.put(view, state);
				}
			}
		}
		if (since == null) {
			since = UpdateSequence.START;
		}
		Log.d(LOG_TAG, "paths: " + paths);

		this.lastCommit = now();
		latch.countDown();
	}

	private boolean isStaleOk(TDLuceneRequest req) {
		return "ok".equals(req.getParameter("stale"));
	}

	private void maybeCommit() throws IOException {
		if (now() - lastCommit >= getCommitInterval()) {
			commitAll();
		}
	}

	private IndexWriter newWriter(final Directory dir) throws IOException {
		final IndexWriterConfig config = new IndexWriterConfig(
				Constants.VERSION, Constants.ANALYZER);

		final LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
		mergePolicy.setMergeFactor(Config.mergeFactor);
		mergePolicy.setUseCompoundFile(Config.useCompoundFile);
		config.setMergePolicy(mergePolicy);

		config.setRAMBufferSizeMB(Config.ramBufferSizeMB);

		return new IndexWriter(dir, config);
	}

	private File viewDir(final View view, final boolean mkdirs)
			throws IOException {
		return viewDir(root, uuid, view.getDigest(), mkdirs);
	}

	private long getSearchTimeout() {
		return Config.timeout;
	}

	private long getCommitInterval() {
		final long commitSeconds = max(1L, Config.commitEvery);
		return SECONDS.toNanos(commitSeconds);
	}

	private String toPath(TDLuceneRequest req) {
		final PathParts parts = new PathParts(req);
		return toPath(parts.getDesignDocumentName(), parts.getViewName());
	}

	private String toPath(final String ddoc, final String view) {
		return ddoc + "/" + view;
	}

}
