"""General Program Flow
1. Monitor Request Queue
2. If present, pull message
3. Extract query from message
4. Perform Searches
    - Upload anything that has contact information to results DDB
5. Complete message pull
"""
import os, re, boto3
from Spotify import Spotify

EMAIL_REGEX = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
URL_REGEX = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
HANDLE_REGEX = r'(?<![\w])@(?:(?:[\w][\.]{0,1})*[\w]){1,29}'

def direct_search(query, api):
    discovered = []
    for x in range(0, 1000, 50):
        res = api.search(query, 'playlist', offset=x)

        # Going to just hard code the 1000 Offset Limit for now
        for playlist in res['playlists']['items']:
            item = {
                'name': playlist['name'],
                'tracks': playlist['tracks']['total'],
                'ownerDetails': {
                    'email': [i for i in re.findall(EMAIL_REGEX, playlist['description']) if i],
                    'handles': [i for i in re.findall(HANDLE_REGEX, playlist['description']) if i],
                    'links': [i for i in re.findall(URL_REGEX, playlist['description']) if i]
                }
            }
            if item['ownerDetails']['email'] or item['ownerDetails']['handles'] or item['ownerDetails']['links']:
                discovered.append(item.copy())
    return discovered


def upload_results(ddb, msgId, results, pageNum):
    table = ddb.Table('pullTable')
    item = {
        'msgId': msgId,
        'pageNum': str(pageNum),
        'playlists': results
    }
    serializer = boto3.dynamodb.types.TypeSerializer()
    ddb_item = serializer.serialize(item)
    print(ddb_item['M'])
    response = table.put_item(
        # Item=ddb_item['M'],
        Item=item,
        ReturnValues='NONE',
        ReturnConsumedCapacity='NONE',
        ReturnItemCollectionMetrics='NONE'
    )
    print(response)
    return response



def read_queue(sqs):
    # TODO Need to extract the Message ID along with Body for publishing to DDB
    queue = sqs.Queue('https://sqs.us-east-2.amazonaws.com/677532242987/submitq')
    queries = []
    msgs = queue.receive_messages(MaxNumberOfMessages=10)
    for msg in msgs:
        # [0] = msgId, [1] = body
        queries.append((msg.message_id, msg.body))
        msg.delete()
    return queries


def main():
    client_id = os.environ.get('spApiClientId')
    client_secret = os.environ.get('spApiClientSecret')
    sqs = boto3.resource('sqs')
    ddb = boto3.resource('dynamodb')

    while(True):
        queries = read_queue(sqs)
        if queries:
            spotify = Spotify(client_id, client_secret)
            for query in queries:
                thread_number = 0
                results = direct_search(query[1], spotify)
                print(results)
                upload_results(ddb, query[0], results, thread_number)
        break
    return

if __name__ == '__main__':
    main()