from kafka import KafkaProducer
import requests
import json
import auth

kafka_producer = KafkaProducer(bootstrap_servers="localhost:9092")
topic = "Twitter-Kafka"

def connect_to_endpoint():
    url_base = "https://api.twitter.com/2/tweets/sample/stream"
    tweet_infos = "?tweet.fields=created_at,entities"
    url = url_base + tweet_infos
    response = requests.request("GET", url, auth=auth.bearer_oauth, stream=True)

    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            entities_keys = list(json_response["data"]["entities"])
            if "hashtags" in entities_keys:
                parse_json(json_response)
        else:
            continue
    if response.status_code != 200:
        return Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )

def parse_json(tweet_json):
    parsed_json = dict()

    parsed_json["ID"] = tweet_json["data"]["id"]
    parsed_json["Date"] = tweet_json["data"]["created_at"]
    parsed_json["Hashtags"] = []
    for hashtag in tweet_json["data"]["entities"]["hashtags"]:
            parsed_json["Hashtags"].append(f'#{hashtag["tag"]}')


    parsed_tweet = json.dumps(parsed_json,ensure_ascii = False)
    kafka_producer.send(topic, str.encode(parsed_tweet))
    return 

if __name__ == '__main__':
    connect_to_endpoint()

