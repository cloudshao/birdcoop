import urllib2

def getFollowersJson(user_id):
	return urllib2.urlopen("http://api.twitter.com/1/statuses/followers.json?user_id="+str(user_id)).read()

def getFolloweesJson(user_id):
	return urllib2.urlopen("http://api.twitter.com/1/statuses/friends.json?user_id="+str(user_id)).read()
