var mapMention = function() {
    userRetweet = "gugPgYSYyWaNYm2S1xAK"
    var time = parseInt(this.timestamp_ms);
    if (this.user != undefined && this.user.time_zone == "Santiago" && this.lang == "es" && (parseInt(this.timestamp_ms) < parseInt(1460988563000))){
        user = this.user.id

        if(this.retweeted_status != undefined){           
            userRetweet = this.retweeted_status.user.id;
        }

        if(this.entities != undefined){
            if(this.entities.user_mentions != undefined){
                    this.entities.user_mentions.forEach(function(mention){
                        if(userRetweet != mention.id){
                            emit({userid1: mention.id, userid2: user}, {A: 1});
                        }
                })
            }   
        }
    }
};


var mapRT = function() {

    if (this.user != undefined && this.user.time_zone == "Santiago" && this.lang == "es" && (parseInt(this.timestamp_ms) < parseInt(1460988563000))){
        user = this.user.id
        if(this.retweeted_status != undefined){           
            emit({userid1: this.retweeted_status.user.id , userid2: this.user.id},  { A: 1});
        }
    }
};


var mapFollowers = function() {
    if (this.user != undefined && this.user.time_zone == "Santiago" && this.lang == "es" && (parseInt(this.timestamp_ms) < parseInt(1460988563000))){
        
       emit({userid1: this.user.id},  { A: this.user.followers_count});
	
	   if(this.retweeted_status != undefined){
            user = this.retweeted_status.user.id;
            emit({userid1: user},  {A: this.retweeted_status.user.followers_count});
        }
    }
};


var mapTweetOriginales = function() {
    if (this.user != undefined && this.user.time_zone == "Santiago" && this.lang == "es" && (parseInt(this.timestamp_ms) < parseInt(1460988563000))){

        userTweet = this.user.id + "|" + this.id;
        emit({userid1: user},  {A: 1});   

        if(this.retweeted_status != undefined){           
            user = this.retweeted_status.user.id + "|" + this.retweeted_status.id;
            emit({userid1: user},  {A: 1});   
        }

        if(this.in_reply_to_user_id != undefined  && this.in_reply_to_status_id != undefined){
            user = this.in_reply_to_user_id + "|" + this.in_reply_to_status_id;
            emit({userid1: user},  {A: 1});      
        }
    }
};

var tweetMentionRT = function() {

    userRetweet = "gugPgYSYyWaNYm2S1xAK"
    if (this.user != undefined && this.user.time_zone == "Santiago" && this.lang == "es" && (parseInt(this.timestamp_ms) < parseInt(1460988563000))){
        user = this.user.id

        if(this.retweeted_status != undefined){     
            userRetweet = this.retweeted_status.user.id;
            emit({userid1: this.retweeted_status.user.id },  { A: 1});
        }

        if(this.entities != undefined){
            if(this.entities.user_mentions != undefined){
                    this.entities.user_mentions.forEach(function(mention){
                        if(userRetweet != mention.id){
                            emit({userid1: mention.id}, {A: 1});
                        }
                })
            }   
        }
    }
};


var timestamp = function() {
    if (this.timestamp_ms != undefined && this.user.time_zone == "Santiago" && this.lang == "es" && (parseInt(this.timestamp_ms) < parseInt(1460988563000))){
        str = this.timestamp_ms.substring(0, this.timestamp_ms.length - 3);
        emit({time: str}, {A: 1});  
        //cut       
    }
};


var reduceFunctionSUM = function(key, values) {
    reducedVal = { A: 0}

    for (var idx = 0; idx < values.length; idx++) {
        reducedVal.A += values[idx].A;
    }

    return reducedVal;
};


var reduceFunction = function(key, values) {
    reducedVal = { A: 0}

    for (var idx = 0; idx < values.length; idx++) {
        reducedVal.A = values[idx].A;
    }

    return reducedVal;
};


db.inundacion.mapReduce( mapRT, reduceFunction, { out:  "nUserRTtoUser" })
db.inundacion.mapReduce( mapMention, reduceFunction, { out:  "nUserMTtoUser" })
db.inundacion.mapReduce( mapFollowers, reduceFunction, { out:  "userfollowers" })
db.inundacion.mapReduce( mapTweetOriginales, reduceFunction, { out:  "tweetsUsersOriginales" })
db.inundacion.mapReduce( tweetMentionRT, reduceFunctionSUM, { out:  "nTweetMTandRT" })
db.inundacion.mapReduce( timestamp, reduceFunctionSUM, { out: "timestamp"})
