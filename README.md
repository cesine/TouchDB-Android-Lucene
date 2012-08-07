TouchDB-Android-Lucene
======================

by Sameer Segal (@sameersegal), Artoo (@artootrills)

**TouchDB-Android-Lucene** is the Androdi port of <a href="https://github.com/rnewson/couchdb-lucene/">CouchDB Lucene</a> that is meant to work along with <a href="https://github.com/couchbaselabs/TouchDB-Android">TouchDB-Android</a>.

## Usage
You will need a design document to index documents. Refer to the <a href="https://github.com/rnewson/couchdb-lucene/#indexing-strategy">Indexing Strategy</a> on CouchDB-Lucene wiki.

Include TouchDB-Android-Lucene as an Android Library project in your activity code use the following snippet to make a request to TDLucene:
``` java
		try {

				if (lucene == null) {
						lucene = new TDLucene(server);
				}

				TDLuceneRequest req = new TDLuceneRequest();
				req.setUrl("/local/grocery-sync/_design/common/byContent")
						.addParam("q", str)
						.addParam("include_docs", "true")
						.addParam("highlights", "5");

				lucene.fetch(req, new Callback() {

								@Override
								public void onSucess(Object resp) {
									Toast.makeText(AndroidGrocerySyncActivity.this,
										"Success", Toast.LENGTH_LONG).show();
									if (resp instanceof JSONObject) {
										try {
											JSONArray rows = ((JSONObject) resp)
												.getJSONArray("rows");
											itemListView
												.setAdapter(new GrocerySyncSearchListAdapter(
													rows));
										} catch (JSONException e) {
											e.printStackTrace();
										}
									}
								}

								@Override
								public void onError(Object resp) {
										Toast.makeText(AndroidGrocerySyncActivity.this,
														"Error -- " + resp.toString(),
														Toast.LENGTH_LONG).show();
								}
				});

		} catch (MalformedURLException e) {
				e.printStackTrace();
		} catch (IOException e) {
				e.printStackTrace();
		} catch (JSONException e) {
				e.printStackTrace();
		}
```

You need to make a simple fix to TDRouter class to make it work by defining `do_POST_Document_all_docs`:

``` java
public TDStatus do_POST_Document_all_docs(TDDatabase _db, String _docID,
				String _attachmentName) {
		TDQueryOptions options = new TDQueryOptions();
		if (!getQueryOptions(options)) {
				return new TDStatus(TDStatus.BAD_REQUEST);
		}

		Map<String, Object> body = getBodyAsDictionary();
		if (body == null) {
				return new TDStatus(TDStatus.BAD_REQUEST);
		}

		Map<String, Object> result = null;
		if (body.containsKey("keys") && body.get("keys") instanceof ArrayList) {
				ArrayList<String> keys = (ArrayList<String>) body.get("keys");
				result = db.getDocsWithIDs(keys, options);
		} else {
				result = db.getAllDocs(options);
		}

		if (result == null) {
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
		}
		connection.setResponseBody(new TDBody(result));
		return new TDStatus(TDStatus.OK);
		// throw new UnsupportedOperationException();
}
```

## To Do
- Testing code
- Make config more generic

## Special Thanks
- Robert Newson (@rnewson)
- Marty Schoch (@mschoch)
- Jens Alfke (@snej) 
