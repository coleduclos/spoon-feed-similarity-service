import json
from decimal import *
import config
import dynamodb_client

def main():
    similarity_queue = config.sqs.get_queue_by_name(QueueName=config.similarity_queue_name)
    process_messages(similarity_queue)

def process_messages(similarity_queue):
    recommendation_queue = config.sqs.get_queue_by_name(QueueName=config.recommendation_queue_name)
    for message in similarity_queue.receive_messages(MaxNumberOfMessages=config.max_queue_messages):
        # print("SQS Message: {}".format(message.body))
        event = json.loads(message.body)
        rating = json.loads(event['body'])
        update_similar_users(rating)
        recommendation_queue.send_message(MessageBody=json.dumps(rating))
        message.delete()
        
def update_similar_users(rating):
    compared_user_list = [] 
    user_ratings_map = {}
    user_id = rating['user-id']

    # Get all ratings by the user
    user_ratings = dynamodb_client.get_all_ratings_by_user_id(user_id)['Items']
    
    # Get similar users
    user_similarity_index_map = dynamodb_client.get_similarity_index_map_by_user_id(user_id)['Items']
    if len(user_similarity_index_map) > 0:
        user_similarity_index_map = user_similarity_index_map[0]['similarity-index-map']
    else:
        user_similarity_index_map = {}
    
    # For each user rating get a unique set of users who have also rated the same restaurants
    for u_rating in user_ratings:
        user_ratings_map[u_rating['restaurant-id']] = u_rating['rating-value'] 
        compared_user_list += \
            dynamodb_client.get_ratings_attribute_by_restaurant_id(u_rating['restaurant-id'],'user-id')['Items']
    compared_user_set = set([x['user-id'] for x in compared_user_list])
    compared_user_set.discard(user_id)

    # For each comparing user compute the similarity index
    for compared_user in compared_user_set:
        compared_user_ratings_map = {}
        similarity_idx_numerator = 0

        # Get the ratings of the compared user
        compared_user_ratings = dynamodb_client.get_all_ratings_by_user_id(compared_user)['Items']

        # Find the intersection and union of the user and compared user rating set to 
        # compute the similarity index
        for compared_user_rating in compared_user_ratings:
            compared_user_ratings_map[compared_user_rating['restaurant-id']] = compared_user_rating['rating-value']
        intersecting_restaurants = set(user_ratings_map.keys()) & set(compared_user_ratings_map.keys()) 
        similarity_idx_demoninator = len(set(user_ratings_map.keys()) | set(compared_user_ratings_map.keys()))
        for r in intersecting_restaurants:
             if user_ratings_map[r] == compared_user_ratings_map[r]:
                 similarity_idx_numerator += 1 
             else:
                 similarity_idx_numerator -= 1
        similarity_idx = Decimal(similarity_idx_numerator) / Decimal(similarity_idx_demoninator)
        user_similarity_index_map[compared_user] = similarity_idx

    # Update the user similarity index map
    dynamodb_client.update_user_similarity_index_map(user_id, user_similarity_index_map)

if __name__ == "__main__":
    main()
