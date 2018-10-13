package edu.asu.zoophy.gbmetadataupdater.metadataextractor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.asu.zoophy.gbmetadataupdater.geonamelucene.LuceneSearcher;

public class RecordLocationExtractorNonInfluenza extends RecordLocationExtractor {

	public RecordLocationExtractorNonInfluenza() throws Exception {
		super();
	}
	public RecordLocationExtractorNonInfluenza(LuceneSearcher searcher) throws Exception {
		super(searcher);
	}
	
	@Override
	protected boolean checkOtherField(String field, String type) throws Exception {
		List<String> combos = generateCombos(field);
		boolean isSufficient=false;
		for(String  curCombo: combos) {
			Set<String> missedPlaces = new HashSet<String>();
			//doing this separately for the first and last token of the split, 
			//e.g. if I have a string Variant of X Y/a/b/c, splitting on "/" first and then
			//splitting on whitespace and generating combos may result in "X Y" being
			//detected as a location, in preference to Y, when the latter is more likely. 
			String[] strainFields = curCombo.split("[/\\-_\\d]+");
			for(int i=0; i<strainFields.length; i++) {	
				String strainField = strainFields[i];
				strainField = strainField.toLowerCase();
				strainField = strainField.replaceAll("\\s+$", "");
				strainField = strainField.replaceAll("^\\s+", "");
				strainField = strainField.replaceAll("\\d+", " ").trim();
				if(!strainField.equals("null") && !isNumeric(strainField)&&!strainField.equals("")&&!isAnimal(strainField,i)) {
					int hasCountry = mlp.parseAsCountry(strainField);
					String placeAdded = "";
					if(strainField.length()>3) {
						placeAdded=mlp.checkMention(strainField);
					}
					if(hasCountry==1) {
						mlp.updateSourceMap(strainField, type);
					}
					if(placeAdded.length()>0) {
						mlp.updateSourceMap(placeAdded, type);
					}
					if(placeAdded.length()==0 && hasCountry==0) {
						missedPlaces.add(strainField);
					}
				}
			}
			for(String place: missedPlaces) {
				String correction = mlp.spellCheck(place);
				if(correction.length()>5) {
					String placeAdded = mlp.checkMention(place);
					if(placeAdded.length()>0) {
						mlp.updateSourceMap(placeAdded, field);
					}
				}
			}
			GBMetadata gm = mlp.integrateLocations();
			mlp.completeMetadataIfPossible(gm);
			if(gm.isSufficient()){
				finalRecLoc = gm;
				return true;
			} 
		}
		return isSufficient;
	}
	

	@Override
	public GBMetadata extractGBLocation(String country, String strain, String isolate, String organism) throws Exception {
		//String host = gb.getFields().get("Host");
		if(checkCountry(country)==true) {
			finalRecLoc.isSufficientInCountry=true;
			finalRecLoc.recSource="country";
		} else if(checkOtherField(strain, "strain")==true) {
			finalRecLoc.recSource="strain";
		} else if (checkOtherField(isolate, "isolate")==true) {
			finalRecLoc.recSource="isolate";
		} else {
			finalRecLoc = mlp.integrateLocations();
			mlp.completeMetadataIfPossible(finalRecLoc);
		}
		String[] placeSources = mlp.getAllLocationAndSources();
		finalRecLoc.allSources=placeSources[1];
		finalRecLoc.allLocations=placeSources[0];
		mlp.clear();
		return finalRecLoc;
	}
	private List<String> generateCombos(String field) {
		
		//punctuations will separate distinct locations, e.g. no one will put "," between New and York in "New York", 
		//but they will use it to separate different locations e.g. "New York, USA". 
		String[] sections=field.split("[,;:\\.]");
		List<String> combos = new ArrayList<String>();
		for(String sect:sections) {
			//split on white space and generate combinations of the tokens
			String[] parts = sect.split(" ");
			for(int i=0; i<parts.length; i++){
				String cur = parts[i];
				combos.add(cur);
				for(int j=i+1; j<parts.length; j++) {
					cur=cur+" "+parts[j];
					combos.add(cur);
				}
			}
		}
		//sort based on the number of tokens in each combo
		Collections.sort(combos, new CandidateComparator());
		return combos;
	}
	
}
