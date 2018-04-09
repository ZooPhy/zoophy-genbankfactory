package edu.asu.zoophy.gbmetadataupdater.geonamelucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.language.DoubleMetaphone;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import org.apache.lucene.search.spell.*;





public class LuceneSpellChecker {

	List<String> filters= Arrays.asList("state of", "city of", "province", "state", "governorate", 
			"district", "county", "city", "area", "region", "village", "republic", "metropolitan",
			"south", "north", "east", "west", "eastern", "western", "southern", "northern", "south-east", "north-east", "south-west", "north-west", "interior", "exterior");

	LuceneSearcher searcher;
	IndexReader[] readers;
	public enum ReaderTypes {
		Country, ADM1, Name;
	}
	DirectSpellChecker dsc = new DirectSpellChecker();
	DoubleMetaphone dm = new DoubleMetaphone() ;

	public class WordBreaks {
		List<String> locations  = new ArrayList<String>();
		String unusedText="";

		public WordBreaks() {

		}
	}

	public LuceneSpellChecker(LuceneSearcher searcher) {
		IndexReader[] readers = new IndexReader[3];
		readers[ReaderTypes.Country.ordinal()] = searcher.countrySearcher.getIndexReader();
		readers[ReaderTypes.ADM1.ordinal()] = searcher.adm1Searcher.getIndexReader();
		readers[ReaderTypes.Name.ordinal()] = searcher.locationSearcher.getIndexReader();
		this.readers=readers;
		this.searcher = searcher;
	}

	public String customSuggest(String curPlace, LuceneSearcher searcher, int maxEditVariant, boolean suggestAlways) throws IOException {
		String suggestion = "";
		curPlace=curPlace.trim();
		try {
			SuggestMode suggestMode;
			IndexReader[] reader = new IndexReader[3];
			reader[ReaderTypes.Country.ordinal()] = searcher.countrySearcher.getIndexReader();
			reader[ReaderTypes.ADM1.ordinal()] = searcher.adm1Searcher.getIndexReader();
			reader[ReaderTypes.Name.ordinal()] = searcher.locationSearcher.getIndexReader();
			int readerType = ReaderTypes.Country.ordinal(); 
			if(suggestAlways==true) {
				suggestMode=SuggestMode.SUGGEST_ALWAYS;
			} else {
				suggestMode=SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
			}
			while(readerType<=ReaderTypes.Name.ordinal()&&suggestion.length()==0) {
				Term term = new Term(ReaderTypes.values()[readerType].name().toLowerCase(), curPlace.toLowerCase());
				boolean hasDirectMatch= hasDirectMatch(term, reader[readerType]);
				if(hasDirectMatch){
					suggestion=curPlace;
					return suggestion;
				}
				readerType++;
			} 

			readerType = ReaderTypes.Country.ordinal(); 
			while(readerType<=ReaderTypes.Name.ordinal()&&(suggestion.length()==0)) {
				Term term = new Term(ReaderTypes.values()[readerType].name().toLowerCase(), curPlace.toLowerCase());
				suggestion= tryWordBreakCombinations(suggestion, term, reader[readerType], searcher);
				readerType++;
			}
			readerType = ReaderTypes.Country.ordinal(); 
			while(readerType<=ReaderTypes.Name.ordinal()&&(suggestion.length()==0)) {
				Term term = new Term(ReaderTypes.values()[readerType].name().toLowerCase(), curPlace.toLowerCase());
				int editVariantsTried = 1;
				while(editVariantsTried<=maxEditVariant && (suggestion.length()==0)) {
					suggestion= tryEditVariants(term, reader[readerType], searcher, editVariantsTried, suggestMode);
					editVariantsTried++;
				}
				readerType++;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return suggestion;

	}



	public boolean hasDirectMatch(Term term, IndexReader reader) {
		try {
			int curFreq = reader.docFreq(term);
			if(curFreq>0) {
				return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private String tryEditVariants(Term term, IndexReader reader, LuceneSearcher searcher, int numEdits, SuggestMode suggestmode)
			throws IOException {

		String suggestion = "";
		dsc.setMaxEdits(numEdits);
		dsc.setComparator(new SpellCheckComparator(searcher, term.text()));
		SuggestWord[] suggestWords = dsc.suggestSimilar(term, 10, reader, suggestmode);
		if(suggestWords.length>0) {
			String reduced = reduceWord(term.text());
			for(SuggestWord cur:suggestWords) {
				String curReduced  = reduceWord(cur.string);
				if(curReduced.equals(reduced)) {
					suggestion = cur.string;
					return suggestion;
				}
			}			
	
			suggestion = checkDoubleMetaphone(term.text(), suggestion, suggestWords, searcher);
			if(suggestion.length()==0 && term.text().length()>5 && numEdits<=1) {
				suggestion = suggestWords[0].string;
			}
		}
		return suggestion;
	}


	private String checkDoubleMetaphone(String curPlace, String suggestion, SuggestWord[] a, LuceneSearcher searcher) {
		if(suggestion.length()==0) {
			for(SuggestWord cur:a){
				if(dm.isDoubleMetaphoneEqual(curPlace, cur.string)) {
					suggestion = cur.string;
					break;
				}
			}
		}
		return suggestion;
	}

	private String tryWordBreakCombinations(String suggestion, Term t,  IndexReader reader, LuceneSearcher searcher) throws IOException {
		String termText = t.text();
		int useMinBreakWordLength = 1;
		int termLength = termText.codePointCount(0, termText.length());
		for (int i = useMinBreakWordLength; i <= (termLength - useMinBreakWordLength); i++) {
			int end = termText.offsetByCodePoints(0, i);
			String leftText = termText.substring(0, end);
			String rightText = termText.substring(end);
			String place = leftText+" "+rightText;
			Term t1 = new Term(t.field(), place.trim().toLowerCase());
			int freq = reader.docFreq(t1);
			if(freq>0) {
				suggestion = place;
			}
		}
		return suggestion;
	}


	
	public String reduceWord(String cur) {
		String reduced = "";
		cur = cur.replaceAll(" ","");
		char[] cA = cur.toCharArray();
		for(int i=0; i<cA.length-1; i++) {
			char c= cA[i];
			if(cA[i+1]!=c) {
				reduced=reduced+cA[i];
			}
		}
		reduced = reduced+cA[cA.length-1];
		return reduced;
	}
}
