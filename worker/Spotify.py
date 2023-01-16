from base64 import b64encode
from json import loads
from urllib.parse import urlencode
from urllib.request import Request, urlopen

class Spotify:
    def __init__(self, clientId, clientSecret):
        self._token_endpoint = "https://accounts.spotify.com/api/token"
        self._api_endpoint = "https://api.spotify.com/v1"
        self._client_id = clientId
        self._client_secret = clientSecret
        self._authorization = self._retrieve_access_token()

    def _retrieve_access_token(self):
        body = bytes(urlencode([('grant_type','client_credentials')]), "utf-8")
        buffer = bytes(f"{self._client_id}:{self._client_secret}", "utf-8")
        headers = {
            "Authorization": f"Basic {b64encode(buffer).decode('utf-8')}",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
        }
        req = Request(
            self._token_endpoint,
            method="POST",
            data=body,
            headers=headers
        )

        # print('"token" request sent: { }')
        res = urlopen(req)
        return loads(res.read().decode('utf-8'))['access_token']

    def search(self, q, type, offset=0, limit=50):
        params = urlencode([
            ('q', q),
            ('type', type),
            ('offset', offset),
            ('limit', limit)
        ])
        headers = {
            "Authorization": f'Bearer {self._authorization}',
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
        }
        req = Request(
            f'{self._api_endpoint}/search?{params}',
            method="GET",
            headers=headers
        )

        # print(f'"search" request sent: {params}')
        res = urlopen(req)
        return loads(res.read().decode('utf-8'))