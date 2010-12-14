import sqlite3
import time

class AwesomeDatabase():

   def __init__(self):
      self.conn = sqlite3.connect('awesomeDB')

   def save(self):
      self.conn.commit()

   def close(self):
      self.conn.close()

   def table_exists(self, table_name):
      cursor = self.conn.cursor()
      cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=(?)", (table_name,))
      exists = cursor.fetchone() != None
      cursor.close()
      return exists

   def create_tables(self):
      cursor = self.conn.cursor()

      if not self.table_exists('user_table'):
         print"Creating user table"
         cursor.execute("CREATE TABLE user_table (user_id INTEGER PRIMARY KEY, name CHAR(20), location CHAR(30), bio CHAR(160), crawled INTEGER, currTime INTEGER)")

      if not self.table_exists('follower_table'):
         print"Creating follower table"
         cursor.execute("CREATE TABLE follower_table (user_id INTEGER, follower_id INTEGER, currTime INTEGER, PRIMARY KEY(user_id, follower_id))")

      if not self.table_exists('tweet_table'):
         print"Creating tweet table"
         cursor.execute("CREATE TABLE tweet_table (user_id INTEGER, time CHAR(20), tweet CHAR(140), currTime INTEGER, PRIMARY KEY(user_id, time))")

      cursor.close()

   def get_unfollowed_users(self):
      ''' Gets a list of users that have not been crawled '''
      cursor = self.conn.cursor()
      cursor.execute('SELECT user_id FROM user_table WHERE crawled=0')
      users = cursor.fetchall()
      cursor.close()
      return users

   def insert_user(self, user_id, name, location, bio, crawled):
      ''' Inserts a user '''
      cursor = self.conn.cursor()
      try: cursor.execute('INSERT INTO user_table(user_id, name, location, bio, crawled, currTime) VALUES(?,?,?,?,?,?)',
                          (user_id, name, location, bio, crawled, int(time.time()), ))
      except sqlite3.IntegrityError: pass
      finally: cursor.close()

   def set_crawled(self, user_id):
      ''' Sets a user to 'crawled' '''
      cursor = self.conn.cursor()
      try: cursor.execute('UPDATE user_table SET crawled=1 WHERE user_id=(?)', (user_id,))
      finally: cursor.close()

   def insert_follower(self, user_id, follower_id):
      ''' Inserts a follower-followee relation '''
      cursor = self.conn.cursor()
      try: cursor.execute('INSERT INTO follower_table(user_id, follower_id, currTime) VALUES(?,?,?)',
                          (user_id, follower_id, int(time.time()), ))
      except sqlite3.IntegrityError: pass
      finally: cursor.close()

   def insert_tweet(self, user_id, tweet_time, tweet):
      cursor = self.conn.cursor()
      try: cursor.execute('INSERT INTO tweet_table(user_id, time, tweet, currTime) VALUES(?,?,?,?)',
                          (user_id, tweet_time, tweet, int(time.time()), ))
      except sqlite3.IntegrityError: pass
      finally: cursor.close()
