import json
import boto3
import os
import time
from datetime import datetime, date

locations = ['manhattan', 'new york', 'brooklyn', 'queens']
cuisines = ['chinese', 'japanese', 'italian', 'french', 'spanish', 'indian']

def validate_order(slots):
    # Validate location
    if slots['location'] and slots['location']['value']['originalValue'].lower() not in locations:
        print('Invalid location')

        return {
            'isValid': False,
            'invalidSlot': 'location',
            'message': f"Please select a location from {', '.join(locations)}."
        }
    
    # Validate cuisine
    if slots['cuisine'] and slots['cuisine']['value']['originalValue'].lower() not in cuisines:
        print('Invalid cuisine')

        return {
            'isValid': False,
            'invalidSlot': 'cuisine',
            'message': f"Please select cuisine from {', '.join(cuisines)}."
        }
    
    # Validate date
    if slots['date']:
        time = datetime.strptime(slots['date']['value']['originalValue'], '%Y-%m-%d').date()
        if time < date.today():
            print('Invalid date')
    
            return {
                'isValid': False,
                'invalidSlot': 'date',
                'message': 'Please choose a future date or today'
            }
            
    # Validate time
    if slots['time']:
        time = float(slots['time']['value']['originalValue'])
        if not time.is_integer() or time < 0 or time > 23 or (time <= datetime.now().hour and datetime.strptime(slots['date']['value']['originalValue'], '%Y-%m-%d').date() == date.today()):
            print('Invalid time')
    
            return {
                'isValid': False,
                'invalidSlot': 'time',
                'message': 'Please choose a valid future time between 0 and 23 inclusive'
            }
    
    # Validate number of people
    if slots['numberOfPeople'] and (not float(slots['numberOfPeople']['value']['originalValue']).is_integer() or float(slots['numberOfPeople']['value']['originalValue']) <= 0):
        print('Invalid number of people')

        return {
            'isValid': False,
            'invalidSlot': 'numberOfPeople',
            'message': 'Please select a positive number of people.'
        }
    
    # Validate email
    if slots['email'] and ('@' not in slots['email']['value']['originalValue']):
        print('Invalid email')

        return {
            'isValid': False,
            'invalidSlot': 'email',
            'message': 'Please select a valid email.'
        }
    
    # Valid Order
    return {'isValid': True}
    
def sendSQS(slots):
    location = slots['location']['value']['interpretedValue']
    cuisine = slots['cuisine']['value']['interpretedValue']
    numberOfPeople =  slots['numberOfPeople']['value']['interpretedValue']
    time = slots['time']['value']['interpretedValue']
    email = slots['email']['value']['interpretedValue']
    
    request = {
        'location':location,
        'cuisine': cuisine,
        'numberOfPeople': numberOfPeople,
        'time': time,
        'email': email
    }
    print(request)
    
    sqs = boto3.client('sqs')
    sqs_url = 'https://sqs.us-east-1.amazonaws.com/246831235319/user-preference'
    response = sqs.send_message(
        QueueUrl=sqs_url,
        MessageBody=json.dumps(request)
    )
    print(response)

def lambda_handler(event, context):
    os.environ['TZ'] = 'US/Eastern'
    time.tzset()
    print(event)
    bot = event['bot']['name']
    slots = event['sessionState']['intent']['slots']
    intent = event['sessionState']['intent']['name'] 
    
    order_validation_result = validate_order(slots)
    if event['invocationSource'] == 'DialogCodeHook':
        if not order_validation_result['isValid']:
            response = {
                "sessionState": {
                    "dialogAction": {
                        "slotToElicit": order_validation_result['invalidSlot'],
                        "type": "ElicitSlot"
                    },
                    "intent": {
                        "name": intent,
                        "slots": slots
                    }
                },
                "messages": [
                    {
                        "contentType": "PlainText",
                        "content": order_validation_result['message']
                    }
                ]
            }
        else:
            print(intent)
            print(slots)
            response = {
                "sessionState": {
                    "dialogAction": {
                        "type": "Delegate"
                    },
                    "intent": {
                        'name': intent,
                        'slots': slots
                    }
                }
            }
    
    if event['invocationSource'] == 'FulfillmentCodeHook':
        # TODO: Send event to SQS
        sendSQS(slots)
        response = {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"
                },
                "intent": {
                    "name": intent,
                    "slots": slots,
                    "state": "Fulfilled"
                }

            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "You request is processed. You will receive an email for the suggestions."
                }
            ]
        }
            
    print(response)
    return response
