import random
import pymongo

NUM_OF_USERS = 100000
MAX_RATING = 5
BATCH_SIZE = 10000
MAX_MOVIE_ID = 23350
NUM_OF_RATINGS_PER_USER = 10
DB_NAME = "sample_mflix"
RATINGS_COLLECTION = "ratings"
MOVIES_COLLECTION = "movies"


conn = pymongo.MongoClient('mongodb+srv://main_user:main_user@mediatest.5tka5.mongodb.net/')
collection_ratings = conn[DB_NAME][RATINGS_COLLECTION]
collection_movies = conn[DB_NAME][MOVIES_COLLECTION]
collection_ratings.delete_many({})

def generate_movie_ids():
    print("movie_id s are being generated ...")
    docs = list(collection_movies.find({},{"_id":1}))
    index = 0
    updates = []
    for doc in docs:
        
        index = index + 1
        update_one = pymongo.UpdateOne({"_id": doc["_id"]},{"$set": {"movie_id": index}})
        updates.append(update_one)
        if (index % BATCH_SIZE == 0):
            collection_movies.bulk_write(updates)
            updates = []
    if len(updates) > 0:
        collection_movies.bulk_write(updates)
    print("movie_ids have been generated...")

def generate_ratings(num_of_users, num_of_ratings_per_user, max_rating=5):

    print("ratings are being generated ...")
    temp_user_rating_array  = []
    current_batch_element = 0
    for i in range(1,num_of_users+1):
        rating_object = {
            "user_id": i
        }

        ratings = []
        for j in range(1,num_of_ratings_per_user+1):
            random_movie_id = random.randint(1,MAX_MOVIE_ID) 
            random_rating = random.randint(1,max_rating)
            rating_object = {
                "movie_id": random_movie_id,
                "rating": random_rating
            }
            ratings.append(rating_object)
            # TODO: we are not checking movie_id has already been added or not
        
        user_rating_object = {
            "user_id" : i,
            "ratings": ratings
        }
        temp_user_rating_array.append(user_rating_object)
        current_batch_element = current_batch_element + 1
        if (current_batch_element % BATCH_SIZE == 0):
            collection_ratings.insert_many(temp_user_rating_array)
            temp_user_rating_array = []
            print(f"{BATCH_SIZE} ratings have been inserted.")
    if len(temp_user_rating_array) > 0:
        collection_ratings.insert_many(temp_user_rating_array)
    print("ratings have been generated")


generate_movie_ids()
generate_ratings(NUM_OF_USERS, NUM_OF_RATINGS_PER_USER, MAX_RATING)