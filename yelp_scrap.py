import requests
import json
import time
import pprint
import boto3

# list of variables
string_attributes = ['id', 'name']
int_attributes = ['review_count', 'rating']
cuisines = ['chinese%20restaurants', 'japanese%20restaurants', 'italian%20restaurants', 'french%20restaurants', 'spanish%20restaurants', 'indian%20restaurants']

for cuisine in cuisines:
    cuisine_type = cuisine.split('%20')[0]
    for offset in range(20):
        # Yelp request
        url = f"https://api.yelp.com/v3/businesses/search?location=New%20York%20City&term={cuisine}&categories=&sort_by=best_match&limit=50"
        if offset > 0:
            url += f'&offset={50*offset}'
        print(url)
        headers = {
            "accept": "application/json",
            "Authorization": "Bearer F4IvHaELGvnpNuWez_rVx9ftQlr5_P9d6DJYuQ5qn3SUZNAG3EWg9FrMIB82m9BvgXFv6pMBLkDxPk1BBBPG3bSfcOwDQaxe_oBDOahT9LZdeuguWj5gMAZc-yz1Y3Yx"
        }

        response = requests.get(url, headers=headers)
        json_object = json.loads(response.text)

        # Store restaurants into DynamoDB table
        dynamodb_client = boto3.client("dynamodb")
        table_name = "yelp-restaurants"
        if 'error' in json_object.keys():
            print(json_object)
        for buz in json_object['businesses']:
            business = {}
            business['insertedAtTimestamp'] = {'S': time.ctime()}
            business['cuisine'] = {'S': cuisine_type}
            for attribute, values in buz.items():
                if attribute in string_attributes:
                    if attribute == 'id':
                        business['businessID'] = {'S': values}
                    else:
                        business[attribute] = {'S': values}
                elif attribute == 'coordinates':
                    business['coordinates'] = {'M': {}}
                    business['coordinates']['M']['latitude'] = {'N': str(values['latitude'])}
                    business['coordinates']['M']['longitude'] = {'N': str(values['longitude'])}
                elif attribute == 'location':
                    business['location'] = {'M': {}}
                    for attr, val in values.items():
                        if attr == 'display_address':
                            business['location']['M']['display_address'] = {'SS': val}
                        else:
                            business['location']['M'][attr] = {'S': str(val)}
                elif attribute in int_attributes:
                    business[attribute] = {'N': str(values)}
                    
            while True:
                try:
                    dynamodb_client.put_item(
                        TableName=table_name,
                        Item=business,
                    )
                except:
                    time.sleep(5)
                    continue
                break
