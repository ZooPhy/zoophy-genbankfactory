package edu.asu.zoophy.genbankfactory.utils.geonames;


/**
 * @author devdemetri
 * Custom exception for general Lucene errors
 */

public class LuceneSearcherException extends Exception {

	private static final long serialVersionUID = 4941959372750095512L;

	public LuceneSearcherException(String msg) {
		super(msg);
	}
	
}
