# Run this script with sudo or add it to the cron job
# Genbank dump runs bimonthly
#

# Summary of the script
# 1. Create new database with name following the format GenBankViruses_<month><year>
# 2. Take backup of old UI datbase.
# 3. Fetch the latest genebankfactory code for github, make changes in configuration files, build the project and run the newly created jar. 
# 4. Backup the old lucence index and replace it with newly created. 
# 5. Fetch zoophy-services code from github, make changes in config files and then build the jar. 
# 6. Kill the old zoophy-services process and start with newly created zoophy-services spring boot app.
# 7. Fetch the latest code of zoophy-ui and restart the node app 

DB_USER=zoophyadmin
DB_HOST='zodo.asu.edu'
DB_NAME='GenBankViruses'_`date +%b%Y`
CREATE_DB="create database $DB_NAME;"
JDBC_DB_URL='jdbc:postgresql://zodo.asu.edu/GenBankViruses_UI'
UI_DB='GenBankViruses_UI'
# give postgres password here
export PGPASSWORD=

echo "psql -h $DB_HOST -U $DB_USER -c create database $DB_NAME;"
psql -h $DB_HOST -U $DB_USER template1 -c "create database \"$DB_NAME\";"

echo "created database ..!!"

# Taking backup of old UI db
echo "taking backup of old ui db ..!"
pg_dump -h $DB_HOST -U $DB_USER -F c $UI_DB > /home/zoophy/old_versions/GenBankViruses_UI_DB_before_`date +%b%Y`.backup

# Fetch latest code of zoophy-genbankfactory
echo "Fetching latest code for zoophy-genbankfactory"

cd /home/zoophy/genbank/zoophy-genbankfactory ; git pull

sleep 5

echo "Done fetching latest code ..!"

# Update zoophy-genbankfactory/src/GenBankFactory.local.properties file 
# update  DB.Big.Name
# update  DB.Small.Name

echo "Updating config file .!"   


sed -i "s/^\(DB\.Big\.Name\s*=\s*\).*\$/\1$DB_NAME/" GenBankFactory.local.properties 

sed -i "s/^\(DB\.Small\.Name\s*=\s*\).*\$/\1$UI_DB/" GenBankFactory.local.properties 

echo "Done updating the config file"

# Build jar  zoophy-genbankfactory/ run ./build.sh

echo "Building jar for genebankfactory"
./build.sh

echo "Jar built ..!"

# kick off the data dump  ‘nohup java -Xmx12G -Xms6G -jar target/zoophy-genbank-factory-<version>-jar-with-dependencies.jar dump create -f gbvrl &’.

echo "kick off the data dump"
echo "command"
java -Xms4G -Xmx8G -jar target/zoophy-genbank-factory-*-jar-with-dependencies.jar dump create -f gbvrl 

#############################################################################################################################################################
# we need to run the following part only when build and dump are successful
# it might be better to run these command manually as individual paths might change

# backup the old ui_index_main 
echo "backup the old ui_index in /home/zoophy/old_versions/ "
cp -r /home/zoophy/indices/ui_index_main /home/zoophy/old_versions/ui_index_before_`date +%b%Y`

rm /home/zoophy/indices/ui_index_main/*

# replace old lucence index with newly created
echo "replace old lucene index with newly created "

cp -r /home/zoophy/indices/small_index_main/* /home/zoophy/indices/ui_index_main 


if ["$1" != "" -a $1 == "restart_services"];
    # Edit configuration file to update the new db name in zoophy-services config file
    echo "updating zoophy services config "
    sed -i "s|^\(spring\.datasource\.url\s*=\s*\).*\$|\1$JDBC_DB_URL|" /home/zoophy/zoophy-services/config/application.properties

    # Build & restart the services
    echo "Building zoophy-services.."
    cd /home/zoophy/zoophy-services ; git pull ; ./build.sh

    # deploy new zoophy services 
    # if jar name is changed then edit jar name here
    # kill the old zoophy services
    process_id=`/bin/ps -fu $USER| grep "java -jar target/zoophy-rest-service*" | grep -v "grep" | awk '{print $2}'`
    echo "killing the old zoophy rest service process "$process_id
    kill -9 $process_id

    # start spring boot app
    echo "starting zoophy rest services..! "
    java -jar target/zoophy-rest-service*.jar > zoophy_services_`date "+%Y-%m-%d_%H:%M:%S"`.log 2>&1 & 

    # Setup the zoophy-ui
    echo "fetching zoophy ui"
    cd /home/zoophy/zoophy-ui ; git pull
    # no config change unless services port is been changed
    # restart app
    pm2 restart zoophy
    echo "End of Script"

echo "replace old lucene index with newly created "