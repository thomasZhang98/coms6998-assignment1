import json
import os
import boto3
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

REGION = 'us-east-1'
HOST = 'search-restaurants-bz7uhwq6p6aj3mkgbslvcsvebm.us-east-1.es.amazonaws.com'
INDEX = 'restaurants'

sqs = boto3.client('sqs')
queue_url = "https://sqs.us-east-1.amazonaws.com/246831235319/user-preference"

class SesDestination:
    """Contains data about an email destination."""
    def __init__(self, tos, ccs=None, bccs=None):
        """
        :param tos: The list of recipients on the 'To:' line.
        :param ccs: The list of recipients on the 'CC:' line.
        :param bccs: The list of recipients on the 'BCC:' line.
        """
        self.tos = tos
        self.ccs = ccs
        self.bccs = bccs

    def to_service_format(self):
        """
        :return: The destination data in the format expected by Amazon SES.
        """
        svc_format = {'ToAddresses': self.tos}
        if self.ccs is not None:
            svc_format['CcAddresses'] = self.ccs
        if self.bccs is not None:
            svc_format['BccAddresses'] = self.bccs
        return svc_format

class SesMailSender:
    """Encapsulates functions to send emails with Amazon SES."""
    def __init__(self, ses_client):
        """
        :param ses_client: A Boto3 Amazon SES client.
        """
        self.ses_client = ses_client

    def send_email(self, source, destination, subject, text, reply_tos=None):
        """
        Sends an email.

        Note: If your account is in the Amazon SES  sandbox, the source and
        destination email accounts must both be verified.

        :param source: The source email account.
        :param destination: The destination email account.
        :param subject: The subject of the email.
        :param text: The plain text version of the body of the email.
        :param html: The HTML version of the body of the email.
        :param reply_tos: Email accounts that will receive a reply if the recipient
                          replies to the message.
        :return: The ID of the message, assigned by Amazon SES.
        """
        send_args = {
            'Source': source,
            'Destination': destination.to_service_format(),
            'Message': {
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': text}}}}
        if reply_tos is not None:
            send_args['ReplyToAddresses'] = reply_tos
        try:
            response = self.ses_client.send_email(**send_args)
            message_id = response['MessageId']
            print(
                "Sent mail %s from %s to %s." % (message_id, source, destination.tos))
        except ClientError:
            print(
                "Couldn't send mail from %s to %s." % (source, destination.tos))
            raise
        else:
            return message_id

def lambda_handler(event, context):
    # Get message from SQS
    response = receive_message()
    print(response)
    if 'Messages' in response.keys():
        # Parse message
        message = json.loads(response['Messages'][0]['Body'])
        if 'email' in message.keys() and 'cuisine' in message.keys():
            email = message['email']
            cuisine = message['cuisine']
        
            # Get recommendations from OpenSearch
            results = query(cuisine)
            
            # Get full restaurants details from DynamoDB
            full_results = get_full_results(results)
            
            # Send email to user
            send_ses(full_results, cuisine, email)
        
            # Delete message from SQS
            delete_message(response['Messages'][0]['ReceiptHandle'])
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                },
                'body': json.dumps({'results': full_results})
            }
        else:    
            # Delete message from SQS
            delete_message(response['Messages'][0]['ReceiptHandle'])
    else:
        print('No messages in SQS')
        
    return
        

def query(term):
    q = {'size': 5, 'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results


def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)

def get_full_results(results):
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
    table = dynamodb.Table('yelp-restaurants')
    full_results = []
    
    for result in results:
        response = table.get_item(
            Key= {
                'businessID': f"{result['restaurantID']}"
            }
        )
        item = response['Item']
        address1 = item['location']['address1']
        address2 = item['location']['address2']
        address3 = item['location']['address3']
        address = address1
        if address2 and address2 != 'None':
            address += '' + address2
        if address3 and address3 != 'None':
            address += '' + address3
        city = item['location']['city']
        state = item['location']['state']
        zip_code = item['location']['zip_code']
        city_line = city + ', ' + state + ' ' + zip_code
        whole_address = address + ' ' + city_line
        name = item['name']
        sanitized_item = {'name': name, 'address': whole_address}
        full_results.append(sanitized_item)
        
    return full_results
        
def receive_message():
    response = sqs.receive_message(QueueUrl=queue_url)
    return response
    
def delete_message(receipt_handle):
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    
def send_ses(results, cuisine, email):
    ses_sender = SesMailSender(boto3.client('ses'))
    message = 'Hello! Here are all the suggestions:\n'
    for i, result in enumerate(results):
        newline = f'{i+1}. '+ result['name'] + ', ' + result['address'] + '\n'
        message += newline

    ses_sender.send_email(email, 
                          SesDestination([email]),
                          f'{cuisine.capitalize()} restaurants recommendations',
                          message)