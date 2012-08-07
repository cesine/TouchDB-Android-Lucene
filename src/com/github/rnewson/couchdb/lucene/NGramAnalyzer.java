package com.github.rnewson.couchdb.lucene;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.shingle.ShingleMatrixFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

public class NGramAnalyzer extends Analyzer {

	private Version version;

	public NGramAnalyzer(Version version) {
		this.version = version;
	}

	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		return new StopFilter(this.version, new LowerCaseFilter(
				new NGramTokenFilter(
						new StandardTokenizer(this.version, reader), 1, 1)),
				StopAnalyzer.ENGLISH_STOP_WORDS_SET);
	}
}
