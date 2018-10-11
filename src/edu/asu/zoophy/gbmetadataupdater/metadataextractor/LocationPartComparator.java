package edu.asu.zoophy.gbmetadataupdater.metadataextractor;

import java.util.Comparator;

public class LocationPartComparator  implements Comparator<LocationPart>{


	@Override
	public int compare(LocationPart o1, LocationPart o2) {
		if(o2.getCodedFcode().compareTo(o1.getCodedFcode())!=0) {
			return o2.getCodedFcode().compareTo(o1.getCodedFcode());
		}
		 return o2.getPopulation().compareTo(o1.getPopulation());
	}
}
