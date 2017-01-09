package edu.asu.zoophy.genbankfactory.utils.taxonomy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.asu.zoophy.genbankfactory.index.IndexerException;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

public class GenBankTree extends Tree {
	private final Logger log = Logger.getLogger("GenBankTree");
	private final String SELECT_NODES 		= "SELECT * FROM \"Taxonomy_Tree\" ORDER BY \"parent_node_id\" ASC";
	private final String SELECT_CONCEPTS	= "SELECT * FROM \"Taxonomy_Concept\"";
	private Map<Integer, GenBankNode> mapIDNodes = null;
	private static GenBankTree tree = null;
	private Connection conn;
	
	/**
	 * We build directly the tree from the DB, overload for other uses
	 * @param conn a connection open on the DB
	 * @throws Exception 
	 */
	public static GenBankTree getInstance() throws Exception {
		if (tree == null) {
			tree = new GenBankTree();
		}
		return tree;
	}
	
	private GenBankTree() throws Exception {
		super();
		try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch(Exception e) {
	    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new IndexerException("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
		instantiate(conn);
	}
	
	protected void instantiate(Connection c) throws Exception {
		log.info("Start creating taxonomy tree structure...");
		createStructure(c);
		log.info("Updating the taxonomy concepts...");
		insertInformation(c);
	}

	/**
	 * Now that we have the structure we update the nodes information
	 * @param conn
	 * @throws Exception
	 */
	protected void insertInformation(Connection c) throws Exception {
		DBQuery query = null;
		ResultSet rs = null;
		try {
			List<Object> queryParams = new LinkedList<Object>();
			query = new DBQuery(c, DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_CONCEPTS, queryParams);
			rs = query.executeSelect_MultiRows();
			while(rs.next()) {
				Integer id = rs.getInt("node_id");
				String name = rs.getString("name");
				GenBankNode node = mapIDNodes.get(id);
				if(node==null) {
					log.log(Level.SEVERE, "Apparently we have a node ID ["+id+"] for the concept ["+name+"] which hasn't been inserted in the tree");
					throw new Exception("Apparently we have a node ID ["+id+"] for the concept ["+name+"] which hasn't been inserted in the tree");
				}
				node.setConcept(name);
			}
		}
		catch(SQLException se) {
			log.log(Level.SEVERE, "Impossible to retrieve the concpets of the taxonomy: "+se.getMessage());
			throw new Exception("Impossible to retrieve the concpets of the taxonomy: "+se.getMessage());
		}
		catch(Exception e) {
			log.log(Level.SEVERE, "Other problem when retrieving the concepts of the taxonomy: "+e.getMessage());
			throw new Exception("Other problem when retrieving the concepts of the taxonomy: "+e.getMessage());
		}
		finally {
			if(rs!=null) {
				try {
					rs.close();
				} 
				catch (SQLException e) {
					log.warning("Impossible to close the ResultSet: "+e.getMessage());
				}
			}
			if(query!=null) {
				query.close();
			}
		}		
	}
	/**
	 * Read the DB to build the tree, here only IDs are linked given the map of the DB
	 * Information about the nodes are given in a second pass
	 * @param conn
	 * @throws Exception
	 */
	protected void createStructure(Connection c) throws Exception {
		mapIDNodes = new HashMap<Integer, GenBankNode>(2000000);
		DBQuery query = null;
		ResultSet rs = null;
		try {
			List<Object> queryParams = new LinkedList<Object>();
			query = new DBQuery(c, DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_NODES, queryParams);
			rs = query.executeSelect_MultiRows();
			root = new GenBankNode(true, "ROOT", 1);
			mapIDNodes.put(1, (GenBankNode) root);
			int lastFatherID = 1;
			GenBankNode lastFather = (GenBankNode)root;
			while (rs.next()) {			
				Integer node_id = rs.getInt("node_id");
				Integer father_id = rs.getInt("parent_node_id");
				if(lastFatherID!=father_id){//we enter in a new node, if it doesn't exists, it needs to be created
					lastFather = mapIDNodes.get(father_id);
					if(lastFather==null) {
						lastFather = new GenBankNode(false, null, father_id);
						mapIDNodes.put(father_id, lastFather);
					}
					lastFatherID = father_id;
				}
				if (node_id!=father_id) {//for some entry we have 1->1 for example for the root
					GenBankNode newNode = mapIDNodes.get(node_id);
					if (newNode==null) {//the node has already been inserted in the tree since it was a father for some other nodes
						newNode = new GenBankNode(false, null, node_id);
					}
					else {
						//update the information of the existing node
					}
					if(newNode.getFather()!=null && newNode.getFather().getID()!=lastFather.getID()) {
						log.log(Level.SEVERE, "We have a node with multiple fathers 1:["+newNode.getFather().getID()+"], 2:["+lastFather.getID()+"] for node ["+newNode.getID()+"]");
						throw new Exception("We have a node with multiple fathers 1:["+newNode.getFather().getID()+"], 2:["+lastFather.getID()+"] for node ["+newNode.getID()+"]");
					}
					newNode.setFather(lastFather);
					lastFather.addChild(newNode);
					mapIDNodes.put(node_id, newNode);
				}
			}
		}
		catch (SQLException se) {
			log.log(Level.SEVERE, "Impossible to retrieve the tree of the taxonomy: "+se.getMessage());
			throw new Exception("Impossible to retrieve the tree of the taxonomy: "+se.getMessage());
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Other problem when retrieving the tree of the taxonomy: "+e.getMessage());
			throw new Exception("Other problem when retrieving the tree of the taxonomy: "+e.getMessage());
		}
		finally {
			if(rs!=null) {
				try {
					rs.close();
				} 
				catch (SQLException e) {
					log.warning("Impossible to close the ResultSet: "+e.getMessage());
				}
			}
			if(query!=null) {
				query.close();
			}
		}
	}
	/**
	 * @param nodeID the node ID
	 * @return the GenBank Node corresponding to the node ID, null if the nodeID is incorrect
	 */
	public Node getNode(Integer nodeID) {
		return mapIDNodes.get(nodeID);
	}
	public Node getRoot() {
		return root;
	}
}