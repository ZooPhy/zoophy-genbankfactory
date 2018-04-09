package edu.asu.zoophy.gbmetadataupdater.utils;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import java.util.HashMap;

import java.util.List;
import java.util.Map;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;



public class FileProcessor {

	class Gene {
		String id;
		String norm;
		String name;
	}
	
	public enum GeneHeaders {
		Gene_ID, Accession, Gene_Name, Normalized_Gene_Name, Itv;
	}
	
	Map<String, List<Gene>>geneMap = new HashMap<String, List<Gene>>();
	
	public static void main(String[] args) {
		FileProcessor fp = new FileProcessor();
		fp.parseFile("C:/Users/ttasn/Documents/ASU/Research/Zoophy/Publications/Databases/gene_normalized_4.csv", "C:/Users/ttasn/Documents/ASU/Research/Zoophy/Publications/Databases/gene_normalized_annotformat.txt");
	}
	public void parseFile(String path, String filePathToWrite) {
		final Path docsPath = Paths.get(path);
		if (!Files.isReadable(docsPath)) {
			System.exit(-1);
		}
		try {
			Reader reader = new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8);
			CSVParser parser = null;
			if(docsPath.toString().endsWith("csv")) {
				parser = new CSVParser(reader, CSVFormat.RFC4180.withHeader(GeneHeaders.class).withSkipHeaderRecord().withIgnoreEmptyLines().withCommentMarker('#'));
			} else {
				parser = new CSVParser(reader, CSVFormat.TDF.withHeader(GeneHeaders.class).withSkipHeaderRecord().withIgnoreEmptyLines().withQuote(null).withCommentMarker('#'));
			}
			Map<String, Integer> headerMap = parser.getHeaderMap();
			BufferedWriter writer = new BufferedWriter(new FileWriter(filePathToWrite));
			int row=0;
			StringBuilder sb = new StringBuilder();
			int i=0;
			for(GeneHeaders curField: GeneHeaders.values()) {
				if(curField.name().equals("Itv")||curField.name().equals("Gene_ID")) {
					continue;
				}
				if(i==0) {
					sb.append(curField.name());
					i=1;
				} else {
					sb.append("\t"+curField.name());
				}
			}
			sb.append("\n");
			writer.write(sb.toString());
			for(CSVRecord record:parser) {
				row++;
				Gene gene = new Gene();				
				gene.id= record.get(GeneHeaders.Gene_ID);
				gene.name= record.get(GeneHeaders.Gene_Name);
				gene.norm = record.get(GeneHeaders.Normalized_Gene_Name);
				String acc = record.get(GeneHeaders.Accession);
				List<Gene> genes = geneMap.get(acc);
				if(genes==null) {
					genes = new ArrayList<Gene>();
				}
				genes.add(gene);
				geneMap.put(acc, genes);
			}
			for(Map.Entry<String, List<Gene>> entry:geneMap.entrySet()) {
				List<Gene> genes = entry.getValue();
				String names = "";
				String ids ="";
				for(Gene gene:genes) {
					names = names+gene.name+";";
					ids = ids+gene.norm+";";
				}
				if(names.endsWith(";")) {
					names = names.substring(0, names.length()-1);
				}
				if(ids.endsWith(";")) {
					ids = ids.substring(0, ids.length()-1);
				}
				writer.write(entry.getKey()+"\t"+names+"\t"+ids+"\n");
			}
			reader.close();
			parser.close();
			writer.close();
			System.out.println("done");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
