package edu.asu.zoophy.genbankfactory.utils.taxonomy;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.asu.zoophy.genbankfactory.database.GeoNameLocation;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider.RP_PROVIDED_RESOURCES;
/**
 * Tree structure for storing GeoName relationships
 * Similar to the GenBankTree
 * @author demetri
 */
public class GeoNameTree extends Tree {
	private final Logger log = Logger.getLogger("GeoNameTree");
	private Map<Integer, GeoNameNode> mapIDNodes = null;
	private Map<String, Integer> countryLookup = null;
	private Map<String, Integer> admLookup = null;
	private final String GeoInfoFile;
	private final String GeoMappingFile;
	private final String GeoCountryFile;
	private final String GeoADMFile;
	private final String geoDirectory;
	private static GeoNameTree tree = null;
	
	private GeoNameTree() {
		log.info("Constructing GeoName Tree...");
		mapIDNodes = new HashMap<Integer, GeoNameNode>(5000000, (float) 0.975);
		countryLookup = new HashMap<String, Integer>(300, (float) 0.9);
		admLookup = new HashMap<String, Integer>(5000, (float) 0.8);
		GeoNameLocation earth = new GeoNameLocation();
		earth.setId(6295630);
		earth.setName("Earth");
		root = new GeoNameNode(true, earth);
		mapIDNodes.put(root.getID(), (GeoNameNode) root);
		addContinents();
		geoDirectory = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("geo.dir");
		GeoInfoFile = geoDirectory + "allCountries.txt";
		GeoMappingFile = geoDirectory + "hierarchy.txt";
		GeoCountryFile = geoDirectory + "countryInfo.txt";
		GeoADMFile = geoDirectory + "admin1CodesASCII.txt";
		fillTree();
		log.info("Finished GeoName Tree");
	}
	/**
	 * The all countries file does not include continents, so here we manually add them to  the tree
	 */
	private void addContinents() {
		GeoNameNode temp;
		//Africa//
		GeoNameLocation africa = new GeoNameLocation();
		africa.setId(6255146);
		africa.setName("Africa");
		africa.setLatitude(7.1881);
		africa.setLongitude(21.09375);
		africa.setContinent("AF");
		temp = new GeoNameNode(false, africa);
		mapIDNodes.put(temp.getID(), temp);
		temp.setFather(root);
		root.addChild(temp);
		//Asia//
		GeoNameLocation asia = new GeoNameLocation();
		asia.setId(6255147);
		asia.setName("Asia");
		asia.setLatitude(29.84064);
		asia.setLongitude(89.29688);
		asia.setContinent("AS");
		temp = new GeoNameNode(false, asia);
		mapIDNodes.put(temp.getID(), temp);
		temp.setFather(root);
		root.addChild(temp);
		//Europe//
		GeoNameLocation europe = new GeoNameLocation();
		europe.setId(6255148);
		europe.setName("Europe");
		europe.setLatitude(48.69096);
		europe.setLongitude(9.14062);
		europe.setContinent("EU");
		temp = new GeoNameNode(false, europe);
		mapIDNodes.put(temp.getID(), temp);
		temp.setFather(root);
		root.addChild(temp);
		//North America//
		GeoNameLocation nAmerica = new GeoNameLocation();
		nAmerica.setId(6255149);
		nAmerica.setName("North America");
		nAmerica.setLatitude(46.07323);
		nAmerica.setLongitude(-100.54688);
		nAmerica.setContinent("NA");
		temp = new GeoNameNode(false, nAmerica);
		mapIDNodes.put(temp.getID(), temp);
		temp.setFather(root);
		root.addChild(temp);
		//Oceania//
		GeoNameLocation oceania = new GeoNameLocation();
		oceania.setId(6255151);
		oceania.setName("Oceania");
		oceania.setLatitude(-18.31281);
		oceania.setLongitude(138.51562);
		oceania.setContinent("OC");
		temp = new GeoNameNode(false, oceania);
		mapIDNodes.put(temp.getID(), temp);
		temp.setFather(root);
		root.addChild(temp);
		//South America//
		GeoNameLocation sAmerica = new GeoNameLocation();
		sAmerica.setId(6255150);
		sAmerica.setName("South America");
		sAmerica.setLatitude(-14.60485);
		sAmerica.setLongitude(-57.65625);
		sAmerica.setContinent("SA");
		temp = new GeoNameNode(false, sAmerica);
		mapIDNodes.put(temp.getID(), temp);
		temp.setFather(root);
		root.addChild(temp);
		//Antarctica//
		GeoNameLocation antarctica = new GeoNameLocation();
		antarctica.setId(6255152);
		antarctica.setName("Antarctica");
		antarctica.setLatitude(-78.15856);
		antarctica.setLongitude(16.40626);
		antarctica.setContinent("AN");
		temp = new GeoNameNode(false, antarctica);
		mapIDNodes.put(temp.getID(), temp);
		temp.setFather(root);
		root.addChild(temp);
	}
	/**
	 * Reads through the give geo_map_file and fills out the GeoNameTree
	 */
	protected void fillTree() {
		try {
			File geoFile = new File(GeoInfoFile);
			Scanner scan = new Scanner(geoFile);
			while (scan.hasNext()) {
				String[] geoname = scan.nextLine().trim().split("\t"); 
				String f_class = geoname[6];
				if (f_class.equalsIgnoreCase("A") || f_class.equalsIgnoreCase("P")) { //we only want country, state, city, etc.
					GeoNameLocation g = new GeoNameLocation();
					g.setId(Integer.parseInt(geoname[0]));
					g.setName(geoname[1]);
					g.setLatitude(Double.parseDouble(geoname[4]));
					g.setLongitude(Double.parseDouble(geoname[5]));
					g.setType(geoname[7]);
					g.setCountry(geoname[8]);
					g.setAdm1(geoname[10]);
//					g.setAdm2(geoname[11]); 
//					g.setAdm3(geoname[12]);
//					g.setAdm4(geoname[13]);
					GeoNameNode gNode = new GeoNameNode(false, g);
					mapIDNodes.put(gNode.getID(), gNode);
				}	
			}
			scan.close();
			geoFile = new File(GeoMappingFile);
			scan = new Scanner(geoFile);
			while (scan.hasNext()) {
				String[] geoname = scan.nextLine().trim().split("\t"); 
				int parent_id = Integer.parseInt(geoname[0]);
				int child_id = Integer.parseInt(geoname[1]);
				try {
					GeoNameNode c = mapIDNodes.get(child_id);
					if (c == null) {
						GeoNameLocation temp = new GeoNameLocation();
						temp.setId(child_id);
						c = new GeoNameNode(false, temp);
						mapIDNodes.put(c.getID(), c);
					}
					GeoNameNode p = mapIDNodes.get(parent_id);
					if (p == null) {
						GeoNameLocation temp = new GeoNameLocation();
						temp.setId(parent_id);
						p = new GeoNameNode(false, temp);
						mapIDNodes.put(p.getID(), p);
					}
					p.addChild(c);
					c.setFather(p);
				}
				catch (Exception e) {
					log.log(Level.SEVERE, "ERROR parsing GeoID: " + child_id + " : " + e.getMessage());
				}
			}
			scan.close();
			String line;
			geoFile = new File(GeoCountryFile);
			scan = new Scanner(geoFile);
			while (scan.hasNext()) {
				line = scan.nextLine();
				if (!line.startsWith("#")) {
					String[] geoname = line.trim().split("\t");
					String name = geoname[0];
					int id = Integer.parseInt(geoname[16]);
					countryLookup.put(name,id);
				}
			}
			scan.close();
			geoFile = new File(GeoADMFile);
			scan = new Scanner(geoFile);
			while (scan.hasNext()) {
				line = scan.nextLine();
				if (!line.startsWith("#")) {
					String[] geoname = line.trim().split("\t");
					String name = geoname[0];
					int id = Integer.parseInt(geoname[3]);
					admLookup.put(name,id);
				}
			}
			scan.close();
			geoFile = null;
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error Filling GeoNameTree: " + e.getMessage());
		}
	}

	public void free() {
		mapIDNodes.clear();
	}
	
	public Node getNode(int ID) {
		return mapIDNodes.get(ID);
	}
	/**
	 * for testing and manual data extraction only
	 */
	public void getNodeChildrenLists(int ID) {
		GeoNameNode node = mapIDNodes.get(ID);
		StringBuilder children = new StringBuilder();
		StringBuilder vals = new StringBuilder();
		for (Node n : node.getChildren()) {
			children.append("\"");
			children.append(n.concept);
			children.append("\",");
			vals.append(n.ID);
			vals.append(",");
		}
		String c = children.toString();
		String v = vals.toString();
		log.info(c);
		log.info(v);
	}
	
	public static GeoNameTree getInstance() {
		if (tree == null) {
			tree = new GeoNameTree();
		}
		return tree;
	}
	
	public Node getRoot() {
		return root;
	}
	
	public Map<String, Integer> getCountryLookup() {
		return countryLookup;
	}
	public Map<Integer, GeoNameNode> getMapIDNodes() {
		return mapIDNodes;
	}
	public Map<String, Integer> getAdmLookup() {
		return admLookup;
	}
}