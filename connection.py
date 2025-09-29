from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

# Replace with your Neptune endpoint and port (default port is 8182)
neptune_endpoint = "your-neptune-endpoint"
neptune_port = "your-neptune-port"

# Create the WebSocket URL for Gremlin connection
database_url = f"wss://{neptune_endpoint}:{neptune_port}/gremlin"

# Create remote connection and traversal source
remote_conn = DriverRemoteConnection(database_url, "g")
g = traversal().withRemote(remote_conn)

# Simple test query: inject 1 and return it in a list
result = g.inject(1).toList()
print("Test query result:", result)

# Close connection
remote_conn.close()
