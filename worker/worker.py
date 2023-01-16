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
            discovered.append(item.copy())
    return discovered

def read_queue(sqs):
    # TODO Need to extract the Message ID along with Body for publishing to DDB
    queue = sqs.Queue('https://sqs.us-east-2.amazonaws.com/677532242987/submitq')
    queries = []
    msgs = queue.receive_messages(MaxNumberOfMessages=10)
    for msg in msgs:
        queries.append(msg.body)
        msg.delete()
    return queries


def main():
    client_id = os.environ.get('spApiClientId')
    client_secret = os.environ.get('spApiClientSecret')
    sqs = boto3.resource('sqs')

    while(True):
        queries = read_queue(sqs)
        print(queries)
        if queries:
            spotify = Spotify(client_id, client_secret)
            for query in queries:
                results = direct_search(query, spotify)
        break
    return

if __name__ == '__main__':
    main()