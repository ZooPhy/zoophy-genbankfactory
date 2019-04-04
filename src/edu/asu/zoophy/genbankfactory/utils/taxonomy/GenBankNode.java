package edu.asu.zoophy.genbankfactory.utils.taxonomy;

/**
 * Node which extends the basic node for the special use of representing GenBank taxonomy
 * @author Davy
 */
public class GenBankNode extends Node {
	public GenBankNode(boolean isRoot, String concept, Integer conceptID) {
		super(isRoot, concept, conceptID);
	}
	/**
	 * Allowed to set the concept in a second pass
	 * @param pConcept
	 */
	public void setConcept(String pConcept) {
			concept = pConcept;	
	}
}