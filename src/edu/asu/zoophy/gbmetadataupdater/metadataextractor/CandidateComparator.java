package edu.asu.zoophy.gbmetadataupdater.metadataextractor;

import java.util.Comparator;

public class CandidateComparator implements Comparator<String> {
		@Override
	    public int compare(String o1, String o2)
	    {
			String[] parts1 = o1.split(" ");
			String[] parts2 = o2.split(" ");
			if(parts2.length==parts1.length) {
				return o2.length()-o1.length();
			}
	        return parts2.length-parts1.length;
	    }
}
