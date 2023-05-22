import requests
import os
import json
import boto3
import datetime
from botocore.exceptions import ClientError
from cryptography.fernet import Fernet

key = Fernet.generate_key()
cipher_suite = Fernet(key)

REGION_NAME = "eu-central-1"
stream_name = "TweetStreamInput"
secret_name = "test-token"

ACCESS_KEY=os.environ.get('AWS_ACCESS_KEY')
SECRET_KEY=os.environ.get('AWS_SECRET_KEY')


# glue_client = boto3.client('glue')
# sm_client = boto3.client('secretsmanager')
# kinesis_client = boto3.client("kinesis")
glue_client = boto3.client('glue', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,region_name=REGION_NAME)
sm_client = boto3.client('secretsmanager',aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,region_name=REGION_NAME)
kinesis_client = boto3.client("kinesis", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,region_name=REGION_NAME)

try:
    get_secret_value_response = sm_client.get_secret_value(
        SecretId=secret_name
    )
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    BEARER_TOKEN = json.loads(secret)['bearer_token']
except ClientError as e:
    raise e


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    # GET Rules to filter tweet by tag ChargeNow
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print("Get Rules: ", json.dumps(response.json()))
    return response.json()

# Delete filter rule
def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print("Delete Rule: ", json.dumps(response.json()))

# Setting up tweet fitler rules
def set_rules(delete):
    # Filter tweet based on tag chargenow
    sample_rules = [
        {"value": "chargenow", "tag": "chargenow"},
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print("Set Rules: ", json.dumps(response.json()))

# Get tweet details from tweet ID
def get_tweet(tweet_id):
    tweet_fields = "tweet.fields=author_id,created_at,public_metrics,entities"
    tweet_url = "https://api.twitter.com/2/tweets?{}&{}".format(tweet_id, tweet_fields)

    response = requests.request("GET", tweet_url, auth=bearer_oauth)
    print("Tweet status: ", response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get tweet (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

# Get user details from user ID
def get_user(user_id):
    if user_id is not None:
        user_url = "https://api.twitter.com/2/users"
        user_param = {'ids': user_id, 'user.fields': 'public_metrics,location'}
        response = requests.get(user_url, auth=bearer_oauth, params=user_param)
        print("User status: ", response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot get user (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

# Load data filtered data into kinesis stream
def put_to_stream(payload):
    try:
        response = kinesis_client.put_record(
            StreamName=stream_name, Data=json.dumps(payload), PartitionKey=payload['location']
        )
        print("Kinesis Stream Payload: ", payload)
        return response
    except Exception as e:
        print("Error while publishing data kinesis stream: " + e)


def trigger_glue_application():
    try:
        get_iob_run_response = glue_client.get_job_runs(
            JobName='twitter_stream_processing'
        )
        if (len(get_iob_run_response['JobRuns']) > 0):
            job_status = get_iob_run_response['JobRuns'][0]['JobRunState']
            # Trigger glue job only current status of the job is not runnign
            if(job_status != 'RUNNING'):
                response = glue_client.start_job_run(
                    JobName='twitter_stream_processing',
                    Arguments={
                        '--batch_time_interval': '10 seconds'
                    }
                )
                if response['JobRunId']:
                    print("Glue job triggered successfully!")
            else:
                print("Glue Job is already running")
        else:
            response = glue_client.start_job_run(
                JobName='twitter_stream_processing',
                Arguments={
                    '--batch_time_interval': '10 seconds'
                }
            )
            if response['JobRunId']:
                print("Glue job triggered successfully!")
    except Exception as e:
        print("Error while triggering Glue job: ", e)

# Get stream data from twitter api
def get_stream(set):
    print("Print Set: ", set)
    print("Get Stream Called!")
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print("Status: ", response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )

    for response_line in response.iter_lines():
        print(f"{datetime.datetime.now()} Response Line: ", response_line)
        if response_line:
            json_response = json.loads(response_line)
            ids = f"ids={json_response['data']['id']}"
            tweet_response = get_tweet(ids)
            if 'errors' not in tweet_response:
                user_id = tweet_response['data'][0]['author_id']
                user_response = get_user(user_id)

                for tweet, user in zip(tweet_response['data'], user_response['data']):
                    author_id = tweet.get('author_id', None)
                    created_at = tweet.get('created_at', None)
                    is_retweet = 'Yes' if tweet.get('public_metrics', {}).get('retweet_count', {}) > 0 else 'No'
                    location = user.get('location', 'location is not enabled')
                    followers_count = user.get('public_metrics',{}).get('followers_count', {})
                    tweet_count = user.get('public_metrics',{}).get('tweet_count',{})
                    tweet_text = tweet.get('text', None)

                    hashtag = tweet.get('entities', {}).get('hashtags', {})
                    tags = []
                    if hashtag:
                        tags = [ht['tag'] for ht in hashtag]
                
                # anonymized user id
                encoded_user_id = cipher_suite.encrypt(str.encode(str(author_id))).decode("utf-8")

                result = {}
                result['user_id'] = encoded_user_id
                result['created_at'] = created_at
                result['is_retweet'] = is_retweet
                result['hashtags'] = ','.join(tags)
                result['location'] = location
                result['followers_count'] = followers_count
                result['tweet_count'] = tweet_count
                result['tweet_text'] = tweet_text

                stream_response = put_to_stream(result)

                print("Get Tweet: ", json.dumps(tweet_response, indent=4, sort_keys=True))
                print("Get User: ", json.dumps(user_response, indent=4, sort_keys=True))
                print("Get Result: ", json.dumps(result, indent=4, sort_keys=True))
                print("Get Stream Response: ", json.dumps(stream_response, indent=4, sort_keys=True))



def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    trigger_glue_application()
    get_stream(set)


if __name__ == "__main__":
    main()