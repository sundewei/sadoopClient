package com.sap.mapred;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;

import java.io.Reader;
import java.io.StringReader;

public class TextAnalyzer extends Analyzer {
    public final TokenStream tokenStream(final String fieldname, final Reader reader) {
        TokenStream result = new StandardTokenizer(Version.LUCENE_30, reader);
        result = new StandardFilter(result);
        result = new LowerCaseFilter(result);
        result = new StopFilter(true, result, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        //result = new PorterStemFilter(result);
        return result;
    }

    public static void main(String[] arg) throws Exception {
        String text = "Here@#!$%^^&. is! some, punctuation?, My mom said playing too much is not a play, it's faking";
        StringReader reader = new StringReader(text);
        Analyzer analyzer = new TextAnalyzer();
        TokenStream tokenStream = analyzer.tokenStream("ThisIsMyFieldName", reader);
        TermAttribute termAttribute = tokenStream.getAttribute(TermAttribute.class);
        while (tokenStream.incrementToken()) {
            String term = termAttribute.term();
            System.out.println(term);
        }

    }
}