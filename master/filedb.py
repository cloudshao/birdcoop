import time

class AwesomeDatabase():

   def __init__(self):

      # An in-memory dict of all users
      # User not in dict: they have not been written to file
      # User in dict, False: they have been written to file but not crawled
      # User in dict, True: they have been written to file and crawled
      self.crawled = {}

      # Output files that contain the two tables we had in the DB
      self.userfile = open('awesome_users', 'w')
      self.followerfile = open('awesome_followers', 'w')

   def save(self): pass
      # Don't need to do anything special to persist writes

   def close(self):
      self.userfile.close()
      self.followerfile.close()

   def create_tables(self): pass
      # No tables, just flat files

   def get_unfollowed_users(self):
      ''' Gets a list of users that have not been crawled '''
      uncrawled_list = []
      for k in self.crawled:
         if not self.crawled[k]: uncrawled_list.append(k)
      return uncrawled_list

   def insert_user(self, user_id, screen_name, name, location, crawled):
      ''' Inserts a user into the file '''

      if user_id not in self.crawled:

         # Add user_id to list and set it to not crawled
         self.crawled[user_id] = False

         # Write user to output file
         values = [unicode(user_id), unicode(screen_name), unicode(name),
                   unicode(location), unicode(crawled),
                   unicode(time.time()),]
         string_to_write = (u','.join(values) + u'\n').encode('utf-8')
         self.userfile.write(string_to_write)

   def set_crawled(self, user_id):
      ''' Sets a user to 'crawled' '''
      self.crawled[user_id] = True

   def insert_follower(self, user_id, follower_id):
      ''' Inserts a follower-followee relation '''
      values = [unicode(user_id), unicode(follower_id),
                unicode(time.time()),]
      string_to_write = (u','.join(values) + u'\n').encode('utf-8')
      self.followerfile.write(string_to_write)

