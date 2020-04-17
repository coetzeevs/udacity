import time

import pandas as pd


def pretty_redshift_props(props):
    """
    Function to extract fields/properties from a set of Redshift cluster properties and return string values
    Args:
        props: Redshift cluster properties object

    Returns: status (str) & host (str)

    """
    if isinstance(props, tuple):
        return props[0], props[1]
    else:
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                      "NumberOfNodes", 'VpcId']
        x = [(k, v) for k, v in props.items() if k in keysToShow]

        df = pd.DataFrame(data=x, columns=["Key", "Value"])

        status = df['Value'][df['Key'] == 'ClusterStatus'].values[0]

        if status != 'available':
            host = None
        else:
            host = df['Value'][df['Key'] == 'Endpoint'].values[0]['Address']

        return status, host


def fetch_props(client, cluster_id):
    """
    Function to get Redshift cluster properties from the Redshift client object
    Args:
        client: Redshift client (object)
        cluster_id: Cluster identifier (str)

    Returns: Redshift cluster properties object

    """
    try:
        res = client.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]
        time.sleep(10)
        return res
    except client.exceptions.ClusterNotFoundFault as e:
        return ('deleted', None)
