import config

def get_all_ratings_by_user_id (user_id):
    print('Querying DynamoDB for all ratings from user: {} in table {}'
        .format(user_id, config.user_ratings_dynamodb_table_name))
    response = config.user_ratings_dynamodb_table.query(
        IndexName=config.user_id_rating_value_index,
        KeyConditionExpression='#partitionkey = :partitionkeyval',
        ExpressionAttributeNames={
            '#partitionkey' : config.user_id_rating_value_index_pkey 
        },
        ExpressionAttributeValues={
            ':partitionkeyval' : user_id
        }
    )
    return response

def get_all_ratings_by_restaurant_id (restaurant_id):
    print('Querying DynamoDB for all ratings of restaurant: {} in table {}'
        .format(restaurant_id, config.user_ratings_dynamodb_table_name))
    response = config.user_ratings_dynamodb_table.query(
        IndexName=config.restaurant_id_rating_value_index,
        KeyConditionExpression='#partitionkey = :partitionkeyval',
        ExpressionAttributeNames={
            '#partitionkey' : config.restaurant_id_rating_value_index_pkey
        },
        ExpressionAttributeValues={
            ':partitionkeyval' : restaurant_id
        }
    )
    return response

def get_ratings_attribute_by_restaurant_id (restaurant_id, attribute):
    print('Querying DynamoDB for ratings attribute: {} of restaurant: {} in table {}'
        .format(attribute, restaurant_id, config.user_ratings_dynamodb_table_name))
    response = config.user_ratings_dynamodb_table.query(
        IndexName=config.restaurant_id_rating_value_index,
        KeyConditionExpression='#partitionkey = :partitionkeyval',
        ProjectionExpression='#attribute', 
        ExpressionAttributeNames={
            '#partitionkey' : config.restaurant_id_rating_value_index_pkey,
            '#attribute' : attribute
        },
        ExpressionAttributeValues={
            ':partitionkeyval' : restaurant_id
        }
    )
    return response

def get_similarity_index_map_by_user_id (user_id):
    print('Querying DynamoDB for the similiarity index map of user: {} in table {}'
        .format(user_id, config.similar_users_dynamodb_table_name))
    response = config.similar_users_dynamodb_table.query(
        KeyConditionExpression='#partitionkey = :partitionkeyval',
        ExpressionAttributeNames={
            '#partitionkey' : config.similar_users_pkey
        },
        ExpressionAttributeValues={
            ':partitionkeyval' : user_id
        }
    )
    return response

def update_user_similarity_index_map (user_id, similarity_index_map):
    print('Updating DynamoDB with similarity index map for  user: {} in table {}'
        .format(user_id, config.similar_users_dynamodb_table_name))
    response = config.similar_users_dynamodb_table.update_item(
        Key={
            config.similar_users_pkey : user_id
        },
        UpdateExpression='SET #attribute = :val',
        ExpressionAttributeNames={
            '#attribute' : config.similarity_index_map_attribute
        },
        ExpressionAttributeValues={
            ':val' : similarity_index_map
        }
    )

