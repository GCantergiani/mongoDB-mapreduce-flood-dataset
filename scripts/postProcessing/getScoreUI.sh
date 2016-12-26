#!/bin/bash 

mongoexport --db tweets --collection nUserRTtoUser --csv --fields _id.userid1 --out ./nUserRTtoUser.csv
mongoexport --db tweets --collection nUserMTtoUser --csv --fields _id.userid1 --out ./nUserMTtoUser.csv
mongoexport --db tweets --collection nTweetMTandRT --csv --fields _id.userid1,value.A --out ./nTweetMTandRT.csv
mongoexport --db tweets --collection tweetsUsersOriginales --csv --fields _id.userid1 --out ./tUserOriginales.csv
mongoexport --db tweets --collection userfollowers --csv --fields _id.userid1,value.A --out ./followers.csv

sed -e 's/\.0//g' -i followers.csv
sed -i '1d' followers.csv
sed -e 's/\.0//g' -i nTweetMTandRT.csv
sed -i '1d' nTweetMTandRT.csv
sed -e 's/\.0//g' -i nUserMTtoUser.csv
sed -i '1d' nUserMTtoUser.csv
sed -e 's/\.0//g' -i nUserRTtoUser.csv
sed -i '1d' nUserRTtoUser.csv

sed -i '1d' tUserOriginales.csv
sed -i '1d' tUserOriginales.csv

cat tUserOriginales.csv | cut -f1 -d"|" >  tUserOrigin.csv
rm tUserOriginales.csv

sed -e 's/\"//g' -i tUserOrigin.csv

sed -i 's/,/ /g' followers.csv
sed -i 's/,/ /g' nTweetMTandRT.csv

cat tUserOrigin.csv | uniq -c > tUserOTw.csv
rm tUserOrigin.csv 

cat nUserMTtoUser.csv | uniq -c > nUserMTtoUserC.csv
cat nUserRTtoUser.csv | uniq -c > nUserRTtoUserC.csv
rm nUserMTtoUser.csv 
rm nUserRTtoUser.csv 

sed -i 's/^ *//' tUserOTw.csv
sed -i 's/^ *//' nUserMTtoUserC.csv
sed -i 's/^ *//' nUserRTtoUserC.csv

awk ' { t = $1; $1 = $2; $2 = t; print; } ' nUserMTtoUserC.csv  > nUserMTtoUser.csv
rm nUserMTtoUserC.csv

awk ' { t = $1; $1 = $2; $2 = t; print; } ' nUserRTtoUserC.csv  > nUserRTtoUser.csv
rm nUserRTtoUserC.csv

awk ' { t = $1; $1 = $2; $2 = t; print; } ' tUserOTw.csv  > tUserOrig.csv
rm tUserOTw.csv

sort -k 1,1 nUserRTtoUser.csv -o nUserRTtoUser.csv
sort -k 1,1 nUserMTtoUser.csv -o nUserMTtoUser.csv
sort -k 1,1 tUserOrig.csv -o tUserOrig.csv
sort -k 1,1 followers.csv -o followers.csv
sort -k 1,1 nTweetMTandRT.csv -o nTweetMTandRT.csv


join -a1 -a2 -1 1 -2 1 -o 0 1.2 2.2 -e "0" nUserRTtoUser.csv nUserMTtoUser.csv > URT-UMT.csv
join -a1 -a2 -1 1 -2 1 -o 0 1.2 1.3 2.2 -e "0"  URT-UMT.csv followers.csv > URT-UMT-FF.csv
join -a1 -a2 -1 1 -2 1 -o 0 1.2 1.3 1.4 2.2 -e "0"  URT-UMT-FF.csv nTweetMTandRT.csv > URT-UMT-FF-NTRM.csv
join -a1 -a2 -1 1 -2 1 -o 0 1.2 1.3 1.4 1.5 2.2 -e "0"  URT-UMT-FF-NTRM.csv tUserOrig.csv > URT-UMT-FF-NTRM-TO.csv

rm nUserRTtoUser.csv
rm nUserMTtoUser.csv
rm followers.csv
rm nTweetMTandRT.csv
rm tUserOrig.csv
rm URT-UMT.csv
rm URT-UMT-FF.csv
rm URT-UMT-FF-NTRM.csv

mongo tweets --eval "db.nUserRTtoUser.drop()"
mongo tweets --eval "db.nUserMTtoUser.drop()"
mongo tweets --eval "db.nTweetMTandRT.drop()"
mongo tweets --eval "db.tweetsUsersOriginales.drop()"
mongo tweets --eval "db.userfollowers.drop()"
mongo tweets --eval "db.timestamp.drop()"