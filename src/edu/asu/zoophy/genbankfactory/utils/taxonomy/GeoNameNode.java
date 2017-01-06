package edu.asu.zoophy.genbankfactory.utils.taxonomy;

import edu.asu.zoophy.genbankfactory.database.GeoNameLocation;

public class GeoNameNode extends Node {

	protected GeoNameLocation location;
	
	public GeoNameNode(boolean isRoot, GeoNameLocation g) {
		super(isRoot, g.getName(), g.getId());
		setLocation(g);
	}

	public GeoNameLocation getLocation() {
		return location;
	}

	public void setLocation(GeoNameLocation location) {
		this.location = location;
	}
}