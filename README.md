# GenBankFactory #

Data extraction and normalization pipeline used for bi-monthly GenBank data dumps. 

Javadocs should be kept up to date [here](https://zodo.asu.edu/javadocs/genbankfactory/). 

## Dependencies:
* [JDK 1.8.x](http://www.oracle.com/technetwork/java/javase/overview/index.html)
* [Maven 3.x](https://maven.apache.org/index.html)
* [PostgreSQL 9.x](https://www.postgresql.org/) for SQL Database
* [Lucene 5.5.x](https://lucene.apache.org/core/5_5_0/) for Lucene Index
* Java IDE, [Spring Tool Suite](https://spring.io/tools) is recommended 

## Setup:

1) Import the project into an IDE as "Existing Maven Project"

2) Create an GenBankFactory.local.properties file in the src folder with your SQL and Lucene details. Refer to [GenBankFactory.local.properties.template](src/GenBankFactory.local.properties.template)

3) Run the [build.sh](build.sh) script

4) The build should run successfully and generate a runnable jar in the target folder.

## Usage
* Always ~~double~~ <i>triple</i> check parameters before building and running the .jar, as it may delete databases. 
* Any changes to GenBankFactory.local.properties will only be reflected <b>after</b> running a new build.
* Allocate <i>at minimum</i> 6GB RAM, preferably 8GB. 
* Typical usage scenario commands:
  * Fresh data dump: `nohup java -Xms4G -Xmx8G -jar target/zoophy-genbank-factory-1.x.x-jar-with-dependencies.jar dump create -f gbvrl &`
  * Re-Run data dump: `nohup java -Xms4G -Xmx8G -jar target/zoophy-genbank-factory-1.x.x-jar-with-dependencies.jar dump clean -f gbvrl &`
  * Rebuild Index: `nohup java -Xms4G -Xmx8G -jar target/zoophy-genbank-factory-1.x.x-jar-with-dependencies.jar index &`
