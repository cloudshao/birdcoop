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
         cursor.execute("CREATE TABLE user_table (user_id INTEGER PRIMARY KEY, screen_name CHAR(20), name CHAR(20), location CHAR(30), crawled INTEGER, currTime INTEGER)")

      if not self.table_exists('follower_table'):
         print"Creating follower table"
         cursor.execute("CREATE TABLE follower_table (user_id INTEGER, follower_id INTEGER, currTime INTEGER, PRIMARY KEY(user_id, follower_id))")

      cursor.close()

   def get_unfollowed_users(self):
      ''' Gets a list of users that have not been crawled '''
      cursor = self.conn.cursor()
      cursor.execute('SELECT user_id FROM user_table WHERE crawled=0 LIMIT 10000')
      users = cursor.fetchall()
      cursor.close()
      return users

   def insert_user(self, user_id, screen_name, name, location, crawled):
      ''' Inserts a user '''
      cursor = self.conn.cursor()
      try: cursor.execute('INSERT INTO user_table(user_id, screen_name, name, location, crawled, currTime) VALUES(?,?,?,?,?,?)',
                          (user_id, screen_name, name, location, crawled, int(time.time()), ))
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

