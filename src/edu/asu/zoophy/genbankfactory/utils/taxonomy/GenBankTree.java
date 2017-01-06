package edu.asu.zoophy.genbankfactory.utils.taxonomy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
		log.info("Adding missing taxonomy concepts...");
		addMissingInformation();
	}

	/**
	 * Adds missing taxon IDs (usually Flu A and B) to our tree
	 */
	private void addMissingInformation() {
		Map<String, Integer> generalTypes = new HashMap<String, Integer>();
		generalTypes.put("H1N1", 114727);
		generalTypes.put("H1N2", 114728);
		generalTypes.put("H1N3", 286279);
		generalTypes.put("H1N4", 352775);
		generalTypes.put("H1N5", 352776);
		generalTypes.put("H1N6", 222770);
		generalTypes.put("H1N7", 571502);
		generalTypes.put("H1N8", 385680);
		generalTypes.put("H1N9", 170500);
		generalTypes.put("H2N1", 114730);
		generalTypes.put("H2N2", 114729);
		generalTypes.put("H2N3", 114731);
		generalTypes.put("H2N4", 352777);
		generalTypes.put("H2N5", 282134);
		generalTypes.put("H2N6", 370290);
		generalTypes.put("H2N7", 286284);
		generalTypes.put("H2N8", 114732);
		generalTypes.put("H2N9", 114733);
		generalTypes.put("H3N1", 157802);
		generalTypes.put("H3N2", 119210);
		generalTypes.put("H3N3", 215851);
		generalTypes.put("H3N4", 136477);
		generalTypes.put("H3N5", 136481);
		generalTypes.put("H3N6", 215855);
		generalTypes.put("H3N7", 547380);
		generalTypes.put("H3N8", 119211);
		generalTypes.put("H3N9", 333276);
		generalTypes.put("H4N1", 282148);
		generalTypes.put("H4N2", 299327);
		generalTypes.put("H4N3", 286286);
		generalTypes.put("H4N4", 222768);
		generalTypes.put("H4N5", 309406);
		generalTypes.put("H4N6", 102800);
		generalTypes.put("H4N7", 418387);
		generalTypes.put("H4N8", 129779);
		generalTypes.put("H4N9", 284164);
		generalTypes.put("H5N1", 102793);
		generalTypes.put("H5N2", 119220);
		generalTypes.put("H5N3", 119221);
		generalTypes.put("H5N4", 342224);
		generalTypes.put("H5N5", 465975);
		generalTypes.put("H5N6", 329376);
		generalTypes.put("H5N7", 273303);
		generalTypes.put("H5N8", 232441);
		generalTypes.put("H5N9", 140020);
		generalTypes.put("H6N1", 119212);
		generalTypes.put("H6N2", 119213);
		generalTypes.put("H6N3", 333277);
		generalTypes.put("H6N4", 184002);
		generalTypes.put("H6N5", 184006);
		generalTypes.put("H6N6", 222769);
		generalTypes.put("H6N7", 184012);
		generalTypes.put("H6N8", 184009);
		generalTypes.put("H6N9", 129778);
		generalTypes.put("H7N1", 119216);
		generalTypes.put("H7N2", 119214);
		generalTypes.put("H7N3", 119215);
		generalTypes.put("H7N4", 325678);
		generalTypes.put("H7N5", 286295);
		generalTypes.put("H7N6", 476651);
		generalTypes.put("H7N7", 119218);
		generalTypes.put("H7N8", 119217);
		generalTypes.put("H7N9", 333278);
		generalTypes.put("H8N1", 571503);
		generalTypes.put("H8N2", 402586);
		generalTypes.put("H8N3", 475602);
		generalTypes.put("H8N4", 142943);
		generalTypes.put("H8N5", 311174);
		generalTypes.put("H8N6", 1316904);
		generalTypes.put("H8N7", 551228);
		generalTypes.put("H8N8", 1082910);
		generalTypes.put("H9N1", 147762);
		generalTypes.put("H9N2", 102796);
		generalTypes.put("H9N3", 147765);
		generalTypes.put("H9N4", 352778);
		generalTypes.put("H9N5", 187402);
		generalTypes.put("H9N6", 221119);
		generalTypes.put("H9N7", 147760);
		generalTypes.put("H9N8", 286287);
		generalTypes.put("H9N9", 136484);
		generalTypes.put("H10N1", 352769);
		generalTypes.put("H10N2", 402585);
		generalTypes.put("H10N3", 352770);
		generalTypes.put("H10N4", 222772);
		generalTypes.put("H10N5", 183666);
		generalTypes.put("H10N6", 352771);
		generalTypes.put("H10N7", 102801);
		generalTypes.put("H10N8", 286285);
		generalTypes.put("H10N9", 136506);
		generalTypes.put("H11N1", 127960);
		generalTypes.put("H11N2", 286294);
		generalTypes.put("H11N3", 352772);
		generalTypes.put("H11N4", 286292);
		generalTypes.put("H11N5", 475601);
		generalTypes.put("H11N6", 195471);
		generalTypes.put("H11N7", 437442);
		generalTypes.put("H11N8", 129771);
		generalTypes.put("H11N9", 129772);
		generalTypes.put("H12N1", 142949);
		generalTypes.put("H12N2", 397549);
		generalTypes.put("H12N3", 546801);
		generalTypes.put("H12N4", 142947);
		generalTypes.put("H12N5", 142951);
		generalTypes.put("H12N6", 416802);
		generalTypes.put("H12N7", 575460);
		generalTypes.put("H12N8", 286293);
		generalTypes.put("H12N9", 352773);
		generalTypes.put("H13N1", 656062);
		generalTypes.put("H13N2", 286281);
		generalTypes.put("H13N3", 222773);
		generalTypes.put("H13N4", 1396558);
		generalTypes.put("H13N6", 150171);
		generalTypes.put("H13N7", 286280);
		generalTypes.put("H13N8", 546800);
		generalTypes.put("H13N9", 352774);
		generalTypes.put("H14N2", 1346489);
		generalTypes.put("H14N3", 488106);
		generalTypes.put("H14N5", 309433);
		generalTypes.put("H14N6", 309405);
		generalTypes.put("H14N7", 1826661);
		generalTypes.put("H14N8", 1261419);
		generalTypes.put("H15N2", 359920);
		generalTypes.put("H15N4", 1084500);
		generalTypes.put("H15N6", 447195);
		generalTypes.put("H15N7", 1569252);
		generalTypes.put("H15N8", 173712);
		generalTypes.put("H15N9", 173714);
		generalTypes.put("H16N3", 304360);
		generalTypes.put("H16N9", 1313083);
		generalTypes.put("H1", 11320);
		generalTypes.put("H2", 11320);
		generalTypes.put("H3", 11320);
		generalTypes.put("H4", 11320);
		generalTypes.put("H5", 11320);
		generalTypes.put("H6", 11320);
		generalTypes.put("H7", 11320);
		generalTypes.put("H8", 11320);
		generalTypes.put("H9", 11320);
		generalTypes.put("H10", 11320);
		generalTypes.put("H11", 11320);
		generalTypes.put("H12", 11320);
		generalTypes.put("H13", 11320);
		generalTypes.put("H14", 11320);
		generalTypes.put("H15", 11320);
		generalTypes.put("H16", 11320);
		generalTypes.put("FluB", 197912);
		generalTypes.put("FluC", 197913);
		String missingTaxonFile = "missingFluTaxons.txt";
		Map<Integer,String> missingTaxons = new HashMap<Integer,String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(missingTaxonFile));
		    String line = br.readLine();
		    while (line != null) {
		    	String[] parts = line.split(" \\| ");
		        int taxon = Integer.parseInt(parts[0]);
		        String strain = parts[1];
		        missingTaxons.put(taxon,strain);
		        line = br.readLine();
		    }
		    br.close();
		    for (Entry<Integer, String> pair : missingTaxons.entrySet()) {
		    	Integer tax = generalTypes.get(pair.getValue());
		    	if (tax == null) {
		    		tax = 11320; //generic Flu A
		    	}
		    	if (tax != null && mapIDNodes.get(pair.getKey()) == null) {
	    			Node parent = mapIDNodes.get(tax);
	    			GenBankNode child = new GenBankNode(false, pair.getValue(), pair.getKey());
	    			child.setFather(parent);
	    			mapIDNodes.put(pair.getKey(), child);
		    	}
		    }
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "error reading missing taxon info"+e.getMessage());
		}
		missingTaxons.clear();
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