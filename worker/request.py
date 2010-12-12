import urllib2
import simplejson as json

FOLLOWERS_URL = 'http://api.twitter.com/1/statuses/followers.json?'
FOLLOWEES_URL = 'http://api.twitter.com/1/statuses/friends.json?'

def get_followers(user_id):
   """
   Returns all the followers of a user
   Raises HTTPError if something went wrong during the HTTP request

   Keyword arguments:
   user_id -- the id of the user
   """
   return __get_user_list(user_id, FOLLOWERS_URL)

def get_followees(user_id):
   """
   Returns all the followees (friends) of a user
   Raises HTTPError if something went wrong during the HTTP request

   Keyword arguments:
   user_id -- the id of the user
   """
   return __get_user_list(user_id, FOLLOWEES_URL)

def __get_user_list(user_id, url):
   cursor = -1
   users = []
   while cursor:
      response = urllib2.urlopen(url +
                                 'user_id=' + str(user_id) +
                                 '&cursor=' + str(cursor))
      object = json.loads(response.read())
      users.extend(object['users'])
      cursor = object['next_cursor']
   return __clean(users)

def __clean(user_list):
   '''
   Returns a copy of user_list with users that only have the fields we need
   '''
   temp_list = []
   for user in user_list:
      temp = {}
      if 'id' in user: temp['id'] = user['id']
      if 'name' in user: temp['name'] = user['name']
      if 'screen_name' in user: temp['screen_name'] = user['screen_name']
      if 'location' in user: temp['location'] = user['location']
      if 'description' in user: temp['description'] = user['description']
      if 'protected' in user: temp['protected'] = user['protected']
      if 'status' in user: temp['status'] = user['status'].copy()
      temp_list.append(temp)
   return temp_list
