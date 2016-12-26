#!/bin/bash 

mongoexport --db tweets --collection tweetWords --csv --fields _id.tweetId,value.A --out ./tweetWords.csv
mongoexport --db tweets --collection containsURL --csv --fields _id.tweetId,value.A --out ./containsURL.csv
mongoexport --db tweets --collection nReplyRetweet --csv --fields _id.tweetId,value.A --out ./nReplyRetweet.csv
mongoexport --db tweets --collection importantFFTweets --csv --fields _id.tweetId,value.A --out ./importantFFTweets.csv

sed -e 's/\.0//g' -i tweetWords.csv
sed -i '1d' tweetWords.csv
sed -e 's/\.0//g' -i containsURL.csv
sed -i '1d' containsURL.csv
sed -e 's/\.0//g' -i nReplyRetweet.csv
sed -i '1d' nReplyRetweet.csv
sed -e 's/\.0//g' -i importantFFTweets.csv
sed -i '1d' importantFFTweets.csv

sed -i 's/,/ /g' tweetWords.csv
sed -i 's/,/ /g' containsURL.csv
sed -i 's/,/ /g' nReplyRetweet.csv
sed -i 's/,/ /g' importantFFTweets.csv

sort -k 1,1 tweetWords.csv -o tweetWords.csv
sort -k 1,1 containsURL.csv -o containsURL.csv
sort -k 1,1 nReplyRetweet.csv -o nReplyRetweet.csv
sort -k 1,1 importantFFTweets.csv -o importantFFTweets.csv


join -a1 -a2 -1 1 -2 1 -o 0 1.2 2.2 -e "0" tweetWords.csv containsURL.csv > TW-URL.csv
join -a1 -a2 -1 1 -2 1 -o 0 1.2 1.3 2.2 -e "0"  TW-URL.csv nReplyRetweet.csv > URT-UMT-NReRT.csv
join -a1 -a2 -1 1 -2 1 -o 0 1.2 1.3 1.4 2.2 -e "0"  URT-UMT-NReRT.csv importantFFTweets.csv > URT-UMT-NReRT-IF.csv

rm tweetWords.csv
rm containsURL.csv
rm nReplyRetweet.csv
rm importantFFTweets.csv

rm TW-URL.csv
rm URT-UMT-NReRT.csv

sed -i -e 's/^/="/' URT-UMT-NReRT-IF.csv
sed -i 's/ /" /' URT-UMT-NReRT-IF.csv 

mongo tweets --eval "db.tweetWords.drop()"
mongo tweets --eval "db.containsURL.drop()"
mongo tweets --eval "db.nReplyRetweet.drop()"
mongo tweets --eval "db.importantFFTweets.drop()"