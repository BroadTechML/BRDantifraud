USER="root"
PASS="Zaq12wsx()"
DATA_DIR="/data/roaming_hour_data/"

for file in $DATA_DIR*.csv;
do
echo $file
mysql -u$USER -p$PASS -e "load data infile '$file' INTO TABLE mydata.roaminglog FIELDS TERMINATED BY ',';
quit"
done