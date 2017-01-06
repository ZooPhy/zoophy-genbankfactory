package edu.asu.zoophy.genbankfactory.utils.taxonomy.inserter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

/**
 * The information related to the host in the GenBank dump are in semi structured form.
 * We need to link the name/description to the ID of the host in the taxonomy.
 * There is no automatic way to do it, I use RE to match as much as I can
 * 
 * Think to use Wikipedia for that it seems to help a lot with big animals like chukkar => chukar partridge
 *
 * @author Davy
 */
public class HostAligner implements HostNormalizer {
	private final Logger log = Logger.getLogger("HostAligner");
	/**
	 * Some host field contain virus strains which contain the host name, they are filtered
	 */
	private Boolean areVirusHostFiltered = true;
	private String UPDATE_HOST_ID 		= "UPDATE \"Host\" SET \"Host_taxon\"=? WHERE \"Accession\"=?";
	private String SELECT_CONCEPTS		= "SELECT * FROM \"Taxonomy_Concept\"";
	private String SELECT_HOST_NAMES 	= "SELECT \"Host_Name\", \"Accession\" FROM \"Host\" WHERE \"Host_taxon\"=0";
	private String HUMAN = "Homo sapiens|human";
	private Pattern pHUMAN = Pattern.compile(HUMAN, Pattern.CASE_INSENSITIVE);
	private Connection conn = null;
	private DBQuery updatHostQuery;
	private int batch_count;
	private final int BATCH_SIZE = 25000;
	private Map<String, Set<Integer>> nameToID = new HashMap<String, Set<Integer>>(3700000);
	
	public HostAligner() throws Exception {
	    try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch(Exception e) {
	    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
	}
	
	protected void createHashMapTaxon() throws Exception {
		DBQuery query = null;
		ResultSet rs = null;
		try {
			List<Object> queryParams = new LinkedList<Object>();
			query = new DBQuery(conn, DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_CONCEPTS, queryParams);
			rs = query.executeSelect_MultiRows();
			while(rs.next()) {
				Integer id = rs.getInt("node_id");
				String name = rs.getString("name");
				Set<Integer> ids = nameToID.get(name.toLowerCase());
				if (ids == null) {
					ids = new HashSet<Integer>();
				}
				ids.add(id);
				nameToID.put(name.toLowerCase(), ids);
			}
			//it has been inserted but it's not useful
			nameToID.remove("no culture available");
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
			if (rs!=null) {
				try {
					rs.close();
				} 
				catch (SQLException e) {
					log.warning("Impossible to close the ResultSet: "+e.getMessage());
				}
			}
			query.close();
		}
	}
	/**
	 * Now that we have all concepts in the Map, we need to read each hosts and map its name with the concept's name for finding the id
	 * @throws Exception
	 */
	@Override
	public void updateHosts() throws Exception {
		DBQuery pull_hosts_query = null;
		ResultSet rs = null;
		batch_count = 0;
		try {
			createHashMapTaxon();
			updatHostQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_HOST_ID);
			List<Object> queryParams = new LinkedList<Object>();
			pull_hosts_query = new DBQuery(conn, DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_HOST_NAMES, queryParams);
			rs = pull_hosts_query.executeSelect_MultiRows();
			while(rs.next()) {
				String accession = rs.getString("Accession");
				String name = rs.getString("Host_Name");
				Integer ID = null;//the ID selected
				Set<Integer> IDs = null;//one name can be ambiguous and multiple IDs can be retrieved
				if (name == null) {
					ID = 1;
				}
				else {
					// TODO: marker
					if (areVirusHostFiltered) {
						name = filterVirusHost(name, accession);
					}
					//first we apply direct mapping rules which link certain entry for our DB
					ID = applyDirectMapping(name, accession);
				}
				if (ID == null) {
					//not find in the direct mapping, try to match exactly to an entry in the taxonomy which is not ambiguous
					IDs = nameToID.get(name.toLowerCase());
					if(IDs == null) {//first we try with rules
						ID = applyMappingRules(name, accession);
						if(ID == null) { //still not, last try with the head to target mother concept
							ID = getHeadID(name, accession);
							if(ID == null) {
								log.warning("Impossible to find host ID for the accession ["+accession+"] name ["+name+"] (not in the map)");
								ID = 1;
							}
						}
					}
					else {
						ID = checkMultipleValues(name, IDs, accession);
					}					
				}
				updateDB(accession, ID);
			}
			if (batch_count != 0) {
				log.info("Final update_Host batch starting");
				updatHostQuery.executeBatch();
				updatHostQuery.close();
				log.info("Update_Host batch completed");
				batch_count = 0;
			}
			log.info("Host Updates Completed");
		}
		catch(SQLException se) {
			log.log(Level.SEVERE, "Impossible to retrieve the host name in the host table: "+se.getMessage());
			throw new Exception("Impossible to retrieve the host name in the host table: "+se.getMessage());
		}
		catch(Exception e) {
			log.log(Level.SEVERE, "Other problem when retrieving the host name in the host table: "+e.getMessage());
			throw new Exception("Other problem when retrieving the host name in the host table: "+e.getMessage());
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
			if(pull_hosts_query!=null) {
				pull_hosts_query.close();
			}
		}
	}
	
	protected void updateDB(String accession, Integer ID) throws Exception {
		try {
			List<Object> queryParams = new LinkedList<Object>();
			queryParams.add(ID);
			queryParams.add(accession);
			updatHostQuery.addBatch(queryParams);
			queryParams.clear();
			batch_count++;
			if (batch_count == BATCH_SIZE) {
				log.info("Update_Host batch starting");
				updatHostQuery.executeBatch();
				log.info("Update_Host batch completed");
				updatHostQuery.close();
				updatHostQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_HOST_ID);
				batch_count = 0;
			}
			//log.info("Processed: " + accession);
		}
		catch(Exception e) {
			log.log(Level.SEVERE, "ERROR running update host batch: " + e.getMessage());
		}
	}
	/**
	 * Some host field contain virus strains which contain the host name,
	 * we filter such cases to  
	 * @param name
	 * @return the name of the host if it's a virus and the host name is in the strain
	 * @throws Exception
	 */
	protected String filterVirusHost(String name, String Accession) throws Exception {
		if(name.toLowerCase().contains("virus") && name.contains("/")) {
			StringBuilder msg = new StringBuilder();
			msg.append("=> Found a virus strain in the host field [");
			msg.append(name);
			msg.append("] replace by [");
			int firstS = name.indexOf("/");
			int secondS = name.substring(firstS+1).indexOf("/");
			//we accept the new name only if it doesn't start with an upper case, to disambiguate with the country/city names
			if(!(name.substring(firstS+1).matches("[A-Z].+"))) {
				name = name.substring(firstS+1, (secondS+firstS+1));
			}
			else {//it's probably a human by default
				name = "homo sapiens";
			}
			msg.append(name);
			msg.append("] for accession [");
			msg.append(Accession);
			msg.append("]");
			log.info(msg.toString());
		}
		return name;
	}
	/**
	 * I apply direct mapping rules for the entries of this small DB on virus
	 * @param hostName
	 * @param Accession
	 * @return the corresponding ID
	 * @throws Exception
	 */
	protected Integer applyDirectMapping(String hostName, String Accession) throws Exception {
		if(hostName.equalsIgnoreCase("udorn")) {
			//log.info("=> found \"udorn\" replaced by [homo sapiens (ID: 9606)] concept for accession ["+Accession+"]");
				return new Integer(8910);
		}
		if(hostName.equalsIgnoreCase("nt")) {
			//log.info("=> found \"nt\" replaced by [homo sapiens (ID: 9606)] concept for accession ["+Accession+"]");
				return new Integer(8910);
		}
		if(hostName.equalsIgnoreCase("gull")||hostName.equalsIgnoreCase("gulls")) {
			//log.info("=> found \"gull\" replaced by [gulls (ID: 8910)] concept for accession ["+Accession+"]");
				return new Integer(8910);
		}
		if(hostName.equalsIgnoreCase("environment")) {
			return new Integer(12908);//unclassified sequences
		}
		if(hostName.equalsIgnoreCase("silky chicken")) {
			//log.info("=> found \"silky chicken\" replaced by [Chicken (ID:9031)] concept for accession ["+Accession+"]");
			return new Integer(9031);
		}
		if(hostName.equalsIgnoreCase("mink")) {
			//log.info("=> found \"mink\" replaced by [Mustelidae (ID:9655)] concept for accession ["+Accession+"]");
			return new Integer(9655);
		}
		if(hostName.equalsIgnoreCase("teal")) {
			//log.info("=> found \"teal\" replaced by [green-winged teal] concept for accession ["+Accession+"]");
			Set<Integer> ids = nameToID.get("green-winged teal");
			if(ids!=null){
				return checkMultipleValues("green-winged teal", ids, Accession);
			}
		}
		if(hostName.equalsIgnoreCase("pheasant")) {
			Set<Integer> ids = nameToID.get("phasianinae");
			if(ids!=null){
				return checkMultipleValues("phasianinae", ids, Accession);
			}
		}
		if(hostName.equalsIgnoreCase("partridge")) {
			Set<Integer> ids = nameToID.get("phasianidae");
			if(ids!=null) {
				return checkMultipleValues("phasianidae", ids, Accession);
			}
		}
		if(hostName.equalsIgnoreCase("condor")) {
			//log.info("=> found \"condor\" replaced by [vultur] concept for accession ["+Accession+"]");
			Set<Integer> ids = nameToID.get("vultur");
			if(ids!=null){
				return checkMultipleValues("vultur", ids, Accession);
			}
		}
		if(hostName.equalsIgnoreCase("guinea fowl")) {
			Set<Integer> ids = nameToID.get("guineafowls");
			if(ids!=null){
				return checkMultipleValues("guineafowls", ids, Accession);
			}
		}
		if(hostName.equalsIgnoreCase("chukkar")) {
			Set<Integer> ids = nameToID.get("chukar partridge");
			if(ids!=null) {
				return checkMultipleValues("chukar partridge", ids, Accession);
			}
		}
		if(hostName.equalsIgnoreCase("banana")) {
				return new Integer(4641);
		}
		if(hostName.equalsIgnoreCase("camel")) {
				return new Integer(9836);
		}
		if(hostName.equalsIgnoreCase("sugarcane")) {
				return new Integer(286192);
		}
		if(hostName.equalsIgnoreCase("puma")) {
				return new Integer(146712);
		}
		if(hostName.equalsIgnoreCase("mouse")) {
				return new Integer(10088);
		}
		if(hostName.equalsIgnoreCase("barley")) {
				return new Integer(4513);
		}
		return null;
	}
	/**
	 * Just tokenize the name and match the last token in the taxonomy
	 * @param name
	 * @return the ID if found, null otherwise 
	 * @throws Exception
	 */
	protected Integer getHeadID(String name, String Accession) throws Exception {
		StringTokenizer tkr = new StringTokenizer(name);
		String head = null;
		while(tkr.hasMoreTokens()) {
			head = tkr.nextToken();
		}
		Set<Integer> headIDs = nameToID.get(head);
		if (headIDs != null && headIDs.size() > 0) {
			if(headIDs.size()==1) {
				return headIDs.iterator().next();
			}
			else {
				return checkMultipleValues(name, headIDs, Accession);
			}
		}
		else {
			return 1;
		}
	}

	/**
	 * The host name has not been found in the map to the id, we need to match it to an entry
	 * @param hostName
	 * @return the ID of the host name, null otherwise
	 */
	protected Integer applyMappingRules(String hostName, String Accession) throws Exception {
		if(hostName.startsWith("\"") && hostName.endsWith("\""))
			hostName = hostName.substring(1,hostName.length()-1);
		//a lot of accessions are about human with various information:
		//human; gender M; age 25
		//Homo sapiens; female; 54 years
		//one quick win is to search the key word
		Matcher m = pHUMAN.matcher(hostName);
		if(m.find()) {
			Set<Integer> ids = nameToID.get("homo sapiens");
			if(ids.size() > 1) {
				return checkMultipleValues("homo sapiens", ids, Accession);
			}
			return ids.iterator().next(); 
		}
		//some start with domestic something, we remove domestic and search again in the map
		if(hostName.startsWith("domestic ")||hostName.startsWith("Domestic ")) {
			Set<Integer> ids = nameToID.get(hostName.substring(9).toLowerCase());
			if(ids!=null) {
				//log.info("=> \"domestic\" was found in front of the host name ["+hostName+"], we removed it for accession ["+Accession+"].");
				return checkMultipleValues(hostName.substring(9).toLowerCase(), ids, Accession);
			}
		}
		if(hostName.contains("(") && hostName.contains(")")) {//case like : spur-winged goose (Plectropterus gambensis) or white-faced whistling duck (Dendrocygna viduata) 
			int posFirstParenthesis = hostName.indexOf('(');
			int posLastParenthesis = hostName.lastIndexOf(")");
			if (posFirstParenthesis > 2 && (posFirstParenthesis < posLastParenthesis)) {
				Set<Integer> ids = nameToID.get(hostName.substring(0, posFirstParenthesis-1).toLowerCase());
				if(ids!=null) {
					return checkMultipleValues(hostName.substring(0, posFirstParenthesis-1).toLowerCase(), ids, Accession);
				}
				//we try with the name in the parenthesis
				ids = nameToID.get(hostName.substring(posFirstParenthesis+1, posLastParenthesis).toLowerCase());
				if(ids!=null) {
					return checkMultipleValues(hostName.substring(posFirstParenthesis+1, posLastParenthesis).toLowerCase(), ids, Accession);
				}
			}
		}
		if(hostName.contains(",")) {//cases like Gallus gallus, chicken
			int posComma = hostName.indexOf(",");
			if (posComma > 2) {
				Set<Integer> ids = nameToID.get(hostName.substring(0,posComma).toLowerCase());
				if(ids != null) {
					return checkMultipleValues(hostName.substring(0,posComma).toLowerCase(), ids, Accession);
				}
				ids = nameToID.get(hostName.substring(posComma+2).toLowerCase());
				if(ids != null) {
					return checkMultipleValues(hostName.substring(posComma+2).toLowerCase(), ids, Accession);
				}
			}
		}
		if(hostName.contains(";")){//cases like swine; gender M; age 6 W
			int posComma = hostName.indexOf(";");
			if(posComma>2){
				Set<Integer> ids = nameToID.get(hostName.substring(0,posComma).toLowerCase());
				if(ids!=null) {
					return checkMultipleValues(hostName.substring(0,posComma).toLowerCase(), ids, Accession);
				}
			}
		}
		//some are plural forms and no singular like gulls
		if(!hostName.endsWith("s")) {
			Set<Integer> ids = nameToID.get(hostName+"s");
			if(ids!=null) {
				return checkMultipleValues(hostName+"s", ids, Accession);
			}
		}
		return null;
	}
	
	protected Integer checkMultipleValues(String name, Set<Integer> ids, String Accession) {
		if (ids.size() > 1) {
			StringBuilder msg = new StringBuilder("~> Entry ["+name+"] found in the taxonomy but it has multiple IDs associated [");
			for(Integer id: ids) {
				msg.append(id);
				msg.append("-");
			}
			msg.append("], no disambiguation is performed, ID = 1");
			log.info(msg.toString());
			return 1;
		}
		else {
			return ids.iterator().next();
		}
	}
}