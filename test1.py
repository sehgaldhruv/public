from boto3 import Session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection


def main():
    print("Starting AWS Neptune Gremlin connection...")

    # Update this with your Neptune endpoint
    endpoint = "your.cluster.endpoint.neptune.amazonaws.com"
    default_region = "us-east-1"
    service = "neptune-db"

    # Build URLs
    url_for_sigv4 = f"https://{endpoint}:8182/gremlin"
    conn_string = f"wss://{endpoint}:8182/gremlin"

    print(f"Endpoint: {endpoint}")
    print(f"Signing URL: {url_for_sigv4}")
    print(f"Connection string: {conn_string}")

    # Get AWS credentials
    session = Session()
    credentials = session.get_credentials()
    if credentials is None:
        raise Exception("No AWS credentials found (check AWS CLI config or env vars)")
    creds = credentials.get_frozen_credentials()
    print(f"Using AWS Access Key: {creds.access_key}")

    # Pick region (session region or fallback)
    region = session.region_name if session.region_name else default_region
    print(f"Region selected: {region}")

    # Prepare request for signing
    request = AWSRequest(method="GET", url=url_for_sigv4)
    SigV4Auth(creds, service, region).add_auth(request)

    print("Request signed. Headers:")
    for key, value in request.headers.items():
        print(f"  {key}: {value}")

    # Establish Gremlin connection
    print("Establishing Gremlin connection...")
    rc = DriverRemoteConnection(conn_string, "g", headers=dict(request.headers))
    g = traversal().with_remote(rc)
    print("Gremlin connection established.")

    # Run a test query
    print("Running test query: g.V().count().next()")
    try:
        count = g.V().count().next()
        print("Vertex count:", count)
    except Exception as e:
        print("Query failed:", e)

    # Close connection
    print("Closing connection...")
    rc.close()
    print("Connection closed.")


if __name__ == "__main__":
    main()
