package edu.asu.zoophy.gbmetadataupdater.metadataextractor;

import java.util.Comparator;


/**
 * A  class for comparing GenBank geospatial metadata to allow sorting based on how specific and comprehensive the metadata is
 * @author tasnia
 *
 */
public class MetadataComparator implements Comparator<GBMetadata> {
	@Override
    public int compare(GBMetadata o1, GBMetadata o2)
    {
		if(o1.getType()==o2.getType() && o1.getSpecific()!=null && o2.getSpecific()!=null) {
			return o1.getSpecific().getCodedFcode().compareTo(o2.getSpecific().getCodedFcode());
		}
        return o1.getType().compareTo(o2.getType());
    }
}