package edu.asu.zoophy.gblocationupdater.spellchecker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import edu.asu.zoophy.gbmetadataupdater.geonamelucene.LuceneSearcher;
import edu.asu.zoophy.gbmetadataupdater.geonamelucene.LuceneSpellChecker;


public class SpellChecker {

	static final int minLen2 = 8;
	static final int minLen1 = 5;

	
	LuceneSpellChecker luceneSpellChecker;
	LinkedHashMap<String, String> phoneticMap;	
	String[] safePrefixEdits = {"al ", "el ", "state of ", "changwat ", "city of "};
	String[] safeEndEdits =  {"republic", " province", " state", " governorate", " district", " county", " city", "metropolitan", " area", " region", " village", " shi", "-shi",  " qu"};
	String[] safeReplacers = { "south", "north", "east", "west", "eastern", "western", "southern", "northern", "south-east", "north-east", "south-west", "north-west", "interior", "exterior"};

	LuceneSearcher searcher;
	
	
	public SpellChecker(LuceneSearcher searcher) {
		luceneSpellChecker = new LuceneSpellChecker(searcher);
		this.searcher=searcher;
	//	initializePhoneticsMap();
	}
	
	
	public String checkSpell(String place) {
		String suggestion = "";	
		try {
			if(place.startsWith("-")&&place.length()>1) {
				place=place.substring(1,place.length());
			}
			if(place.endsWith("-")&&place.length()>1) {
				place = place.substring(0, place.length()-1);
			}
			place = place.replaceAll("_", " ");		
			if(place.length()>=minLen2) {
				suggestion = luceneSpellChecker.customSuggest(place, searcher, 2, true);
			} else if (place.length()>=minLen1) {
				suggestion = luceneSpellChecker.customSuggest(place, searcher, 1, true);
			} else {
				suggestion = luceneSpellChecker.customSuggest(place, searcher, 0, true);
			}
			return suggestion;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return suggestion;
	}

	
	public List<String> getSafeAlterations(String place) {
		List<String> alteredStrings = new ArrayList<String>();
		char[] placeArray = place.toLowerCase().toCharArray();
		for(String key: phoneticMap.keySet()) { 
			int start=-1;
			int end=-1;
			char[] keyArray = key.toCharArray();
			boolean found = false;
			int j=0;
			for(int i=0; i<placeArray.length; i++) {
				if(placeArray[i]==keyArray[j]) { 
					if(found==false) {
						start = i;
					}
					j++;
					if(j<keyArray.length) {
						found=true;
					} else {
						found=false;
						String alteredString;
						end=i+1;
						if(end<place.length()) {
							alteredString = place.substring(0, start)+phoneticMap.get(key)+place.substring(end, place.length());
						} else {
							alteredString = place.substring(0, start)+phoneticMap.get(key);
						}
						alteredStrings.add(alteredString);
						start=-1;
						end =-1;
						j=0;
					}
				} else if(placeArray[i]!=keyArray[j]) {
					if(found==true) {
						found=false;
						start = -1;
						end=-1;
						j=0;
					}
				}
			}
		}
		return alteredStrings;
	}
}
