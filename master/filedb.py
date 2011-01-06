import time

class AwesomeDatabase():

   def __init__(self):
      self.crawled = {}
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
      values = [unicode(user_id), unicode(screen_name), unicode(name),
                unicode(location), unicode(crawled), unicode(time.time()),]
      self.userfile.write(u','.join(values) + u'\n')
      if user_id not in self.crawled: self.crawled[user_id] = False

   def set_crawled(self, user_id):
      ''' Sets a user to 'crawled' '''
      self.crawled[user_id] = True

   def insert_follower(self, user_id, follower_id):
      ''' Inserts a follower-followee relation '''
      values = [unicode(user_id), unicode(follower_id),
                unicode(time.time()),]
      self.followerfile.write(u','.join(values) + u'\n')

