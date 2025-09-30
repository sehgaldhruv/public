from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.session import Session
import requests

def check_neptune_status(endpoint, region='us-east-1'):
    service = 'neptune-db'

    # Get AWS credentials from environment or instance metadata
    session = Session()
    credentials = session.get_credentials()
    if credentials is None:
        raise Exception("No AWS credentials found. Configure your environment.")
    frozen_credentials = credentials.get_frozen_credentials()

    url = f'https://{endpoint}:8182/status'

    # Prepare the AWS signed request
    request = AWSRequest(method='GET', url=url, data=None)
    SigV4Auth(frozen_credentials, service, region).add_auth(request)

    # Send the request with signed headers
    headers = dict(request.headers)
    response = requests.get(url, headers=headers)

    print(f"HTTP Status Code: {response.status_code}")
    print("Response Content:")
    print(response.text)

    if response.status_code == 200:
        print("Connection to Neptune private endpoint successful.")
    else:
        print("Failed to connect to Neptune private endpoint.")

if __name__ == "__main__":
    # Replace with your Neptune cluster's private endpoint DNS name
    neptune_endpoint = 'g-ix941ccjv6.us-east-1.neptune-graph.amazonaws.com'
    region = 'us-east-1'

    try:
        check_neptune_status(neptune_endpoint, region)
    except Exception as e:
        print(f"Error: {e}")