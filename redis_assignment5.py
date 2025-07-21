import csv
import re
from traceback import print_stack
from pyparsing import Regex
import redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition
from redis.commands.search.query import Query

class Redis_Client():
    redis = None
    
    def __init__(self):
        self.redis = self.redis

    """
    Connect to redis with "host", "port", "db", "username" and "password".
    """
    def connect(self):
        try:
            self.redis = redis.Redis(
                host='redis-12345.c123.us-east-1-2.ec2.cloud.redislabs.com',  # Replace with your Redis host
                port=12345,  # Replace with your Redis port
                db=0,
                username='default',  # Replace with your username
                password='your_password_here',  # Replace with your password
                decode_responses=True
            )
            # Test the connection
            self.redis.ping()
            print("Connect to Redis.")
            return self.redis
        except Exception as e:
            print(f"Connection failed: {e}")
            print_stack()

    """
    Load the users dataset into Redis DB.
    """
    def load_users(self, file):
        result = 0
        try:
            pipe = self.redis.pipeline()
            with open(file, 'r') as f:
                content = f.read()
                # Split by user entries - each starts with "user:
                user_entries = content.split(' "user:')[1:]  # Skip first empty element
                
                for entry in user_entries:
                    # Parse each user entry
                    parts = entry.strip().split('" "')
                    if len(parts) >= 11:  # Ensure we have enough parts
                        user_id = parts[0]  # First part is the ID
                        user_key = f"user:{user_id}"
                        
                        # Create hash mapping from the parsed data
                        user_data = {
                            'first_name': parts[2] if len(parts) > 2 else '',
                            'last_name': parts[4] if len(parts) > 4 else '',
                            'email': parts[6] if len(parts) > 6 else '',
                            'gender': parts[8] if len(parts) > 8 else '',
                            'ip_address': parts[10] if len(parts) > 10 else '',
                            'country': parts[12] if len(parts) > 12 else '',
                            'country_code': parts[14] if len(parts) > 14 else '',
                            'city': parts[16] if len(parts) > 16 else '',
                            'longitude': parts[18] if len(parts) > 18 else '',
                            'latitude': parts[20] if len(parts) > 20 else '',
                            'last_login': parts[22].rstrip('"') if len(parts) > 22 else ''
                        }
                        
                        # Store in Redis hash
                        pipe.hset(user_key, mapping=user_data)
                        result += 1
                        
            pipe.execute()
            print("Load data for user")
            print(result)
            return result
        except Exception as e:
            print(f"Error loading users: {e}")
            print_stack()
            return 0

    """
    Load the scores dataset into Redis DB.
    """
    def load_scores(self):
        pipe = self.redis.pipeline()
        result_count = 0
        
        try:
            with open('userscores.csv', 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    user_id = row['user:id']
                    score = int(row['score'])
                    leaderboard = row['leaderboard']
                    
                    # Add to sorted set for each leaderboard
                    leaderboard_key = f"leaderboard:{leaderboard}"
                    pipe.zadd(leaderboard_key, {user_id: score})
                    result_count += 1
            
            result = pipe.execute()
            print("load data for scores")
            return result
        except Exception as e:
            print(f"Error loading scores: {e}")
            print_stack()
            return []

    """
    Return all the attribute of the user by usr
    """
    def query1(self, usr):
        print("Executing query 1.")
        try:
            user_key = f"user:{usr}"
            result = self.redis.hgetall(user_key)
            print(result)
            return result
        except Exception as e:
            print(f"Error in query1: {e}")
            print_stack()
            return {}

    """
    Return the coordinate (longitude and latitude) of the user by the usr.
    """
    def query2(self, usr):
        print("Executing query 2.")
        try:
            user_key = f"user:{usr}"
            longitude = self.redis.hget(user_key, 'longitude')
            latitude = self.redis.hget(user_key, 'latitude')
            coordinates = {'longitude': longitude, 'latitude': latitude}
            print(coordinates)
            return coordinates
        except Exception as e:
            print(f"Error in query2: {e}")
            print_stack()
            return {}

    """
    Get the keys and last names of the users whose ids do not start with an odd number.
    """
    def query3(self):
        print("Executing query 3.")
        try:
            cursor = 1280  # Starting cursor as specified
            userids = []
            result_lastnames = []
            
            # Use SCAN to iterate through keys starting at cursor 1280
            cursor, keys = self.redis.scan(cursor=cursor, match="user:*", count=100)
            
            for key in keys:
                # Extract user ID from key (format: user:ID)
                user_id = key.split(':')[1]
                if user_id.isdigit():
                    first_digit = int(user_id[0])
                    # Check if first digit is even (not odd)
                    if first_digit % 2 == 0:
                        userids.append(key)
                        lastname = self.redis.hget(key, 'last_name')
                        result_lastnames.append(lastname)
            
            print(userids, result_lastnames)
            return userids, result_lastnames
        except Exception as e:
            print(f"Error in query3: {e}")
            print_stack()
            return [], []

    """
    Return the female in China or Russia with the latitude between 40 and 46.
    """
    def query4(self):
        print("Executing query 4.")
        try:
            # First try to create the search index
            try:
                self.redis.ft('user_idx').info()
            except:
                # Create index with specified fields
                schema = [
                    TextField('gender'),
                    TagField('country'),
                    NumericField('latitude'),
                    TextField('first_name')
                ]
                
                definition = IndexDefinition(prefix=['user:'])
                self.redis.ft('user_idx').create_index(schema, definition=definition)
            
            # Search for female users in China or Russia with latitude 40-46
            query_str = "@gender:female (@country:{China|Russia}) @latitude:[40 46]"
            query = Query(query_str)
            
            result = self.redis.ft('user_idx').search(query)
            
            for doc in result.docs:
                print(doc)
            
            return result
        except Exception as e:
            print(f"Error in query4: {e}")
            print_stack()
            return None

    """
    Get the email ids of the top 10 players(in terms of score) in leaderboard:2
    """
    def query5(self):
        print("Executing query 5.")
        try:
            # Get top 10 players from leaderboard:2 (highest scores first)
            top_players = self.redis.zrevrange('leaderboard:2', 0, 9, withscores=False)
            
            result = []
            for player in top_players:
                # Get email for each top player
                email = self.redis.hget(player, 'email')
                if email:
                    result.append(email)
            
            print(result)
            return result
        except Exception as e:
            print(f"Error in query5: {e}")
            print_stack()
            return []

# Main execution
if __name__ == "__main__":
    rs = Redis_Client()
    rs.connect()
    
    # Load data - uncomment these lines after setting up Redis connection
    # rs.load_users("users.txt")
    # rs.load_scores()
    
    # Execute all queries
    rs.query1(299)
    rs.query2(2836)
    rs.query3()
    rs.query4()
    rs.query5()
