package edu.asu.zoophy.genbankfactory.utils.taxonomy;

import java.util.ArrayList;
import java.util.List;

/**
 * The node is a concept in the taxonomy
 * @author Davy
 */
public class Node {
	protected boolean root = false;
	protected String concept = null;
	protected Integer ID = null;
	protected Node father = null;
	protected List<Node> children = null;
	
	public Node(boolean isRoot, String concept, Integer conceptID) {
		root = isRoot;
		this.concept = concept;
		ID = conceptID;
		children = new ArrayList<Node>();
	}
	boolean isRoot() {
		return root;
	}	
	public String getConcept() {
		return concept;
	}
	public Integer getID() {
		return ID;
	}
	public Node getFather() {
		return father;
	}
	/**
	 * @return the list of ancestors for this node, empty list if it's the root
	 * 			the list is root first, father last
	 */
	public List<Node> getAncestors() {
		List<Node> ancestors = new ArrayList<Node>();
		if(this.isRoot()) {
			return ancestors;
		}
		Node currentNode = this.getFather();
		while(!currentNode.isRoot()) {
			ancestors.add(currentNode);
			currentNode = currentNode.getFather();
		}
		ancestors.add(currentNode);//the last if the root normally
		return ancestors;
	}
	public void setFather(Node father) {
		this.father = father;
	}
	public List<Node> getChildren() {
		return children;
	}
	public void setChildren(List<Node> children) {
		this.children = children;
	}
	public void addChild(Node child){
		if (!getChildren().contains(child)) {
			getChildren().add(child);
		}
	}
	public String toString() {
		StringBuilder out = new StringBuilder();
		if(isRoot()) {
			out.append("ROOT->[");
			for(Node child: getChildren()){
				out.append(child.concept);
				out.append(" (");
				out.append(child.getID());
				out.append("),");
			}
			if(out.charAt(out.length()-1)==',')
				out.deleteCharAt(out.length()-1);
			out.append("]");
		}
		else {
			if(getFather()==null) {
				out.append("_null_->");
			}
			else {
				out.append(getFather().getConcept());
				out.append(" (");
				out.append(getFather().getID());
				out.append(")->");
			}
			out.append("{");
			out.append(getConcept());
			out.append(" (");
			out.append(getID());
			out.append(")}->[");
			for(Node child: getChildren()) {
				out.append(child.concept);
				out.append(" (");
				out.append(child.getID());
				out.append("),");
			}
			if(out.charAt(out.length()-1)==',')
				out.deleteCharAt(out.length()-1);
			out.append("]");
		}
		return out.toString();
	}
}