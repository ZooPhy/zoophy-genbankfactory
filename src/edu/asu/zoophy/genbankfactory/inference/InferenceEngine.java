package edu.asu.zoophy.genbankfactory.inference;

public class InferenceEngine {
	
	public static void main(String[] args) {
		try {
			System.out.println("hello!");
		}catch(Exception e) {
//			log.fatal( "ERROR running GenBankFactory: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

}
