import json

def read_json_file(file_path):
    with open(file_path, 'r') as file:
        json_data = json.load(file)
    return json_data
	
file_path = 'example.json'
json_data = read_json_file(file_path)
print(json_data)
	
    
import requests
import json

def create_databricks_cluster(api_token, cluster_name, cluster_config):
    api_endpoint = "https://<databricks-instance>/api/2.0/clusters/create"  # Replace <databricks-instance> with your Databricks instance URL

    headers = {
        "Authorization": "Bearer " + api_token,
        "Content-Type": "application/json"
    }

    payload = {
        "cluster_name": cluster_name,
        "cluster_source": "new",
        "spark_version": cluster_config["spark_version"],
        "node_type_id": cluster_config["node_type_id"],
        "num_workers": cluster_config["num_workers"],
        "spark_conf": cluster_config["spark_conf"],
        "aws_attributes": {
            "availability": cluster_config["availability"],
            "zone_id": cluster_config["zone_id"]
        }
    }

    response = requests.post(api_endpoint, headers=headers, data=json.dumps(payload))
    response_json = response.json()

    if response.status_code == 200:
        print("Cluster created successfully with ID:", response_json["cluster_id"])
    else:
        print("Error creating cluster:", response_json["message"])
        
        
To use this function, you need to provide the following parameters:

api_token: Your Databricks API token.
cluster_name: The name for the new cluster.
cluster_config: A dictionary containing the cluster configuration details, such as the Spark version, node type ID, number of workers, Spark configuration, availability, and zone ID.
Here's an example of how you can use this function:

api_token = "your-api-token"
cluster_name = "my-cluster"
cluster_config = {
    "spark_version": "7.3.x-scala2.12",
    "node_type_id": "Standard_D3_v2",
    "num_workers": 2,
    "spark_conf": {
        "spark.executor.memory": "4g",
        "spark.driver.memory": "4g"
    },
    "availability": "SPOT",
    "zone_id": "us-west-2a"
}

create_databricks_cluster(api_token, cluster_name, cluster_config)


import requests
import json

def create_databricks_cluster(api_token, cluster_name, config_file_path):
    api_endpoint = "https://<databricks-instance>/api/2.0/clusters/create"  # Replace <databricks-instance> with your Databricks instance URL

    headers = {
        "Authorization": "Bearer " + api_token,
        "Content-Type": "application/json"
    }

    with open(config_file_path, 'r') as file:
        config_data = json.load(file)

    config_data["cluster_name"] = cluster_name

    response = requests.post(api_endpoint, headers=headers, json=config_data)
    response_json = response.json()

    if response.status_code == 200:
        print("Cluster created successfully with ID:", response_json["cluster_id"])
    else:
        print("Error creating cluster:", response_json["message"])

In this version, the config_file_path parameter is used to specify the path to the JSON file containing the cluster configuration details. The function reads the JSON file, updates the "cluster_name" field with the provided cluster_name, and sends the updated JSON payload in the request body.

Here's an example of how you can use this function:

api_token = "your-api-token"
cluster_name = "my-cluster"
config_file_path = "cluster_config.json"  # Path to the JSON file containing the cluster configuration

create_databricks_cluster(api_token, cluster_name, config_file_path)


Make sure to replace "your-api-token" with your actual Databricks API token and "cluster_config.json" with the path to your JSON configuration file.

Note: You'll need to replace <databricks-instance> in the api_endpoint variable with the URL of your Databricks instance.


Can you please give me python function for calling databricks REST API for resize a cluster?

import requests

def resize_databricks_cluster(api_token, cluster_id, num_workers):
    api_endpoint = f"https://<databricks-instance>/api/2.0/clusters/edit"

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "cluster_id": cluster_id,
        "num_workers": num_workers
    }

    response = requests.post(api_endpoint, headers=headers, json=payload)
    response_json = response.json()

    if response.status_code == 200:
        print("Cluster resized successfully")
    else:
        print("Error resizing cluster:", response_json["message"])

To use this function, you need to provide the following parameters:

api_token: Your Databricks API token.
cluster_id: The ID of the cluster you want to resize.
num_workers: The desired number of workers for the cluster.
Here's an example of how you can use this function:

api_token = "your-api-token"
cluster_id = "12345"  # Replace with the actual cluster ID
num_workers = 5

resize_databricks_cluster(api_token, cluster_id, num_workers)


Make sure to replace "your-api-token" with your actual Databricks API token and "12345" with the ID of the cluster you want to resize. Set num_workers to the desired number of workers for the cluster.

Note: You'll need to replace <databricks-instance> in the api_endpoint variable with the URL of your Databricks instance.

import requests
import json




def create_databricks_cluster_policy(api_token, policy_name, config_file_path):
    api_endpoint = "https://<databricks-instance>/api/2.0/policies/clusters/create"  # Replace <databricks-instance> with your Databricks instance URL

    headers = {
        "Authorization": "Bearer " + api_token,
        "Content-Type": "application/json"
    }

    with open(config_file_path, 'r') as file:
        config_data = json.load(file)

    config_data["name"] = policy_name

    response = requests.post(api_endpoint, headers=headers, json=config_data)
    response_json = response.json()

    if response.status_code == 200:
        print("Cluster policy created successfully with ID:", response_json["policy_id"])
    else:
        print("Error creating cluster policy:", response_json["message"])

In this function, the config_file_path parameter is used to specify the path to the JSON file containing the cluster policy configuration. The function reads the JSON file, updates the "name" field with the provided policy_name, and sends the updated JSON payload in the request body.

Here's an example of how you can use this function:

api_token = "your-api-token"
policy_name = "my-cluster-policy"
config_file_path = "cluster_policy_config.json"  # Path to the JSON file containing the cluster policy configuration

create_databricks_cluster_policy(api_token, policy_name, config_file_path)


Make sure to replace "your-api-token" with your actual Databricks API token, "my-cluster-policy" with the desired name for the cluster policy, and "cluster_policy_config.json" with the path to your JSON configuration file.

Note: You'll need to replace <databricks-instance> in the api_endpoint variable with the URL of your Databricks instance.

from databricks_api import DatabricksAPI

def create_databricks_cluster(num_workers = num_workers,autoscale=None, cluster_name, spark_version, node_type_id, spark_env_vars):
    db = DatabricksAPI(
        host="example.cloud.databricks.com",
        token="your-api-token"
    )

    try:
        cluster = db.cluster.create_cluster(
            num_workers=num_workers
            autoscale=None
            cluster_name=cluster_name,
            spark_version=spark_version,
            node_type_id=node_type_id,
            spark_env_vars=spark_env_vars
        )
        return cluster
    except Exception as e:
        print('Error creating Databricks cluster:', str(e))
        return None

# Example usage
cluster_name = input('Enter the cluster name: ')
spark_version = input('Enter the Spark version: ')
node_type_id = input('Enter the node type ID: ')
spark_env_vars = input('Enter the Spark environment variables: ')

create_databricks_cluster(cluster_name, spark_version, node_type_id, spark_env_vars)


db.cluster.resize_cluster(
    cluster_id,
    num_workers=None,
    autoscale=None,
    headers=None,
)

