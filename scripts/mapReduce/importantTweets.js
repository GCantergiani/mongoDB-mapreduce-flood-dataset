var CountWordsTweets = function() {
    
    if (this.user != undefined && this.user.time_zone == "Santiago" && this.lang == "es" && (parseInt(this.timestamp_ms) < parseInt(1460909363000))){
        
        tweetLength = this.text.split(' ').length;

        emit({tweetId: this.id}, {A: tweetLength});

        if(this.retweeted_status != undefined){           
            tweetLengthRT = this.retweeted_status.text.split(' ').length;
            emit({tweetId: this.retweeted_status.id}, {A: tweetLengthRT});        
        }
    }
};


var containsURL = function() {

    if (this.user != undefined && this.user.time_zone == "Santiago" && this.lang == "es" && (parseInt(this.timestamp_ms) < parseInt(1460909363000))){
        
        if(this.entities.urls.length > 1){
            emit({tweetId: this.id}, {A: 1});    
        }

        if(this.retweeted_status != undefined){           
            if(this.retweeted_status.entities.urls.length > 1){
                emit({tweetId: this.retweeted_status.id}, {A: 1});
            }
        }
    }
};


var replyAndRTweet = function() {
     if (this.user != undefined && this.user.time_zone == "Santiago" && this.lang == "es" && (parseInt(this.timestamp_ms) < parseInt(1460909363000))){

        emit({tweetId: this.id}, {A: 1});    
            
            if(this.in_reply_to_status_id != undefined){
                emit({tweetId: this.in_reply_to_status_id}, {A: 1});    
            }

            if(this.retweeted_status != undefined){           
                emit({tweetId: this.retweeted_status.id}, {A: 1});    
            }
        }
};


var importantFollower = function() {
     if (this.user != undefined && this.user.time_zone == "Santiago" && this.lang == "es" && (parseInt(this.timestamp_ms) < parseInt(1460909363000))){

            emit({tweetId: this.id}, {A: this.user.followers_count});    

            if(this.retweeted_status != undefined){           
                emit({tweetId: this.retweeted_status.id}, {A: this.retweeted_status.user.followers_count});
            }
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



var reduceMaxFunction = function(key, values) {
    reducedVal = { A: 0}

    for (var idx = 0; idx < values.length; idx++) {
        if(reducedVal.A < values[idx].A){
            reducedVal.A = values[idx].A;
        }
    }

    return reducedVal;
};

db.inundacion.mapReduce( CountWordsTweets, reduceFunction, { out:  "tweetWords" })
db.inundacion.mapReduce( containsURL, reduceFunction, { out:  "containsURL" })
db.inundacion.mapReduce( replyAndRTweet, reduceFunctionSUM, { out:  "nReplyRetweet" })
db.inundacion.mapReduce( importantFollower, reduceMaxFunction , { out:  "importantFFTweets" })