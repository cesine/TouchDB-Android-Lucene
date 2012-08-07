package com.github.rnewson.couchdb.lucene;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.lucene.index.IndexWriterConfig;

import android.os.Environment;
import android.util.Log;

import com.couchbase.touchdb.TDServer;
import com.couchbase.touchdb.ektorp.TouchDBHttpClient;

public final class Config {

	private static final String LOG_TAG = Config.class.getName();

	// private static final String CONFIG_FILE = "couchdb-lucene.ini";
	// private static final String LUCENE_DIR = "lucene.dir";
	private static final String DEFAULT_DIR = "indexes";

	public static final boolean allowLeadingWildcard = false;
	public static final boolean lowercaseExpandedTerms = true;

	public static final long commitEvery = 15;

	public static final int mergeFactor = 10;

	public static final boolean useCompoundFile = false;

	public static final double ramBufferSizeMB = IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB;

	public static final long timeout = 50000;

	public static final int limit = 25;

	public static String url = "touchdb://";

	// private final HierarchicalINIConfiguration configuration;

	// public Config() throws ConfigurationException {
	// this.configuration = new HierarchicalINIConfiguration(Config.class
	// .getClassLoader().getResource(CONFIG_FILE));
	// this.configuration
	// .setReloadingStrategy(new FileChangedReloadingStrategy());
	// }
	//
	// public final HierarchicalINIConfiguration getConfiguration() {
	// return this.configuration;
	// }

	public final static File getDir() throws IOException {
		final File dir = new File(Environment.getExternalStorageDirectory(),
				DEFAULT_DIR);
		if (!dir.exists() && !dir.mkdir()) {
			throw new IOException("Could not create " + dir.getCanonicalPath());
		}
		if (!dir.canRead()) {
			throw new IOException(dir + " is not readable.");
		}
		if (!dir.canWrite()) {
			throw new IOException(dir + " is not writable.");
		}
		Log.i(LOG_TAG, "Index output goes to: " + dir.getCanonicalPath());
		return dir;
	}

	public final static TouchDBHttpClient getClient(TDServer server) throws MalformedURLException {
		// HttpClientFactory.setIni(this.configuration);
		return HttpClientFactory.getInstance(server);
	}
}
