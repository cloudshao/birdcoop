import random

def get_followers(user_id):
   """
   Returns all the followers of a user
   Raises HTTPError if something went wrong during the HTTP request

   Keyword arguments:
   user_id -- the id of the user
   """
   return __get_user_list(user_id)

def get_followees(user_id):
   """
   Returns all the followees (friends) of a user
   Raises HTTPError if something went wrong during the HTTP request

   Keyword arguments:
   user_id -- the id of the user
   """
   return __get_user_list(user_id)

def __get_user_list(user_id):

   users = []
   for j in range(100):
      temp_u = {}
      temp_u['id'] = random.randint(1, 99999999)
      temp_u['name'] = 'John Doe'
      temp_u['location'] = 'Here'
      temp_u['description'] = 'My name is john doe hello everyone'
      users.append(temp_u)

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
