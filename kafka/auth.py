import requests
import json

bearer_token = 'AAAAAAAAAAAAAAAAAAAAAPMlVAEAAAAAqRYReWQ4Nwf8MJe7QNgP%2F72NW6U%3Dnx4gp1j9w8EVFs2B9m8bUOyUJURhHlpUES3dzhuk76PtlOLHaw'

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r
