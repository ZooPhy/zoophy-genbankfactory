package edu.asu.zoophy.genbankfactory.utils.geonames;


/**
 * @author devdemetri
 * Custom exception for invalid Lucene querystrings
 */

public class InvalidLuceneQueryException extends Exception {

	private static final long serialVersionUID = -3278380923689647254L;
	
	public InvalidLuceneQueryException(String msg) {
		super(msg);
	}

}
