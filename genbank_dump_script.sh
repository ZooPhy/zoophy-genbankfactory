# Run this script with sudo 
# Genbank dump runs bimonthly
#

# Create a new database in pgadmin GenBankViruses_<month><year>

DB_USER=zoophyadmin
DB_HOST='zodo.asu.edu'
DB_NAME='GenBankViruses'_`date +%b%Y`
#DB_NAME='GenBankViruses_Dec2017'
CREATE_DB="create database $DB_NAME;"
SMALL_DB=''
JDBC_DB_URL='jdbc:postgresql://zodo.asu.edu/'$DB_NAME
UI_DB='GenBankViruses_UI'
# give postgres password here
export PGPASSWORD=

echo "psql -h $DB_HOST -U $DB_USER -c create database $DB_NAME;"
psql -h $DB_HOST -U $DB_USER template1 -c "create database \"$DB_NAME\";"

echo "created database ..!!"

# Taking backup of old UI db
pg_dump -h $DB_HOST -U $DB_USER -F c GenBankViruses_UI > /home/zoophy/old_versions/GenBankViruses_UI_DB_before_`date +%b%Y`.backup

echo "taking backup of old ui db ..!"

# Fetch latest code of zoophy-genbankfactory
echo "Fecting latest code "

cd /home/zoophy/genbank/zoophy-genbankfactory ; git pull

sleep 5

echo "Done fetching latest code ..!"

# Update zoophy-genbankfactory/src/GenBankFactory.local.properties file 
# update  DB.Big.Name
# update  DB.Small.Name

echo "Updating config file .!"   


sed -i "s/^\(DB\.Big\.Name\s*=\s*\).*\$/\1$DB_NAME/" src/GenBankFactory.local.properties 

#sed -i "s/^\(DB\.Small\.Name\s*=\s*\).*\$/\1$DB_NAME/" src/GenBankFactory.local.properties 

echo "Done updating the config file"

# Update GbMetadataUpdater/GBMetadataUpdater.local.properties
# set Set annotation.DB.Name  to DB.Big.Name
sed -i "s/^\(annotation\.DB\.Name\s*=\s*\).*\$/\1$DB_NAME/" /home/zoophy/genbank/GbMetadataUpdater/GBMetadataUpdater.local.properties

# Build jar  zoophy-genbankfactory/ run ./build.sh

echo "Building jar for genebankfactory"
./build.sh

echo "Jar built ..!"

# kick off the data dump  ‘nohup java -Xmx12G -Xms6G -jar target/zoophy-genbank-factory-<version>-jar-with-dependencies.jar dump create -f gbvrl &’.

echo "kick off the data dump"
echo "command"
java -Xms4G -Xmx8G -jar target/zoophy-genbank-factory-1.4.2-jar-with-dependencies.jar dump create -f gbvrl > genebank_dump_`date "+%Y-%m-%d_%H:%M:%S"`.log 2>&1 

#############################################################################################################################################################

# backup the old ui_index in /home/zoophy/old_versions/
echo "backup the old ui_index in /home/zoophy/old_versions/ "
cp -r /home/zoophy/ui_index /home/zoophy/old_versions/ui_index_before_`date +%b%Y`


# replace old lucence index with newly created
echo "replace old lucene index with newly created "
cp -r /home/zoophy/genbank/small_index /home/zoophy/ui_index

# Edit configuration file to update the new db name in zoophy-services config file
echo "updating zoophy services config "
sed -i "s|^\(spring\.datasource\.url\s*=\s*\).*\$|\1$JDBC_DB_URL|" /home/zoophy/zoophy-services/config/application.properties

# Build & restart the services
cd /home/zoophy/zoophy-services ; git pull ; ./build.sh

# deploy new zoophy services 
# if jar name is changes then edit jar name here
# kill the old zoophy services
process_id=`/bin/ps -fu $USER| grep "java -jar target/zoophy-rest-service-0.1.2.jar" | grep -v "grep" | awk '{print $2}'`
echo "kiiling the old process "$process_id
kill -9 $process_id

# start spring boot app
echo "starting zoophy rest services..! "
java -jar target/zoophy-rest-service-0.1.2.jar

# Setup the zoophy-ui
echo "fetching zoophy ui"
cd /home/zoophy/zoophy-ui ; git pull
# no config change unless services port is been changed
# restart app
pm2 restart zoophy
