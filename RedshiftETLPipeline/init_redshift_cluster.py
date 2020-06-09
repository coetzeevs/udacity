import configparser
import json

import boto3

from helpers.functions import fetch_props, pretty_redshift_props

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

try:
    session = boto3.session.Session(aws_access_key_id=config.get('AWS', 'KEY'),
                                    aws_secret_access_key=config.get('AWS', 'SECRET'))
    iam = session.client('iam', config.get('AWS', 'REGION'))
    redshift = session.client('redshift', config.get('AWS', 'REGION'))
except ValueError as e:
    print(f'ValueError: {e}')
except Exception as e:
    print(f'Uncaught exception: {e}')


def create_iam_role(role_name=config.get('IAM_ROLE', 'NAME')):
    """
    Function to create a new AWS IAM role
    Args:
        role_name: Name for the new IAM role (str) [optional]

    Returns: None

    """
    try:
        iam.create_role(
            Path='/',
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(
                dict(
                    Statement=[
                        dict(
                            Action='sts:AssumeRole',
                            Effect='Allow',
                            Principal=dict(
                                Service='redshift.amazonaws.com'
                            )
                        )
                    ],
                    Version='2012-10-17'
                )
            ),
            Description='Allow Redshift clusters to call AWS services on my behalf',
        )

        return
    except iam.exceptions.EntityAlreadyExistsException as e:
        print(f'IAM role already exists: {e}')
    except Exception as e:
        print(f'Uncaught exception when creating IAM role: {e}')


def attach_policy(role_name=config.get('IAM_ROLE', 'NAME')):
    """
    Function to attach an AWS role policy to a IAM role
    Args:
        role_name: Name for the new IAM role (str) [optional]

    Returns: None

    """
    try:
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )

        return
    except Exception as e:
        print(f'Uncaught exception when creating attaching role policy: {e}')


def get_role_arn(role_name=config.get('IAM_ROLE', 'NAME')):
    """
    Function to retrieve the IAM role ARN associated with an IAM role
    Args:
        role_name: Name for the new IAM role (str) [optional]

    Returns: IAM role ARN (str)

    """
    try:
        role_arn = iam.get_role(
            RoleName=role_name
        )['Role']['Arn']

        return role_arn
    except Exception as e:
        print(f'Uncaught exception when fetching role ARN: {e}')


def create_cluster(
        cluster_type=config.get("CLUSTER", "TYPE"),
        node_type=config.get("CLUSTER", "NODE_TYPE"),
        num_nodes=config.get("CLUSTER", "NUM_NODES"),
        cluster_id=config.get("CLUSTER", "IDENTIFIER"),
        user=config.get("CLUSTER", "DB_USER"),
        pwd=config.get("CLUSTER", "DB_PASSWORD"),
        db_name=config.get("CLUSTER", "DB_NAME"),
):
    """
    Function to instantiate a new Redshift cluster

    Args: All optional - values inferred from configuration file
        cluster_type: Type of Redshift cluster (str)
        node_type: Type of Redshift node (str)
        num_nodes: Number of Redshift nodes (str)
        cluster_id: Cluster identifier (str)
        user: Master user name for DB access (str)
        pwd: Master user password for DB access (str)
        db_name: Database name (str)

    Returns: None

    """
    try:
        redshift.create_cluster(
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_nodes),

            DBName=db_name,
            ClusterIdentifier=cluster_id,
            MasterUsername=user,
            MasterUserPassword=pwd,

            IamRoles=[get_role_arn()]
        )
        return
    except redshift.exceptions.ClusterAlreadyExistsFault as e:
        print(f'Redshift cluster already exists: {e}')
    except Exception as e:
        print(f'Uncaught exception when creating Redshift cluster: {e}')


def get_host(cluster_id=config.get("CLUSTER", "IDENTIFIER")):
    """
    Function to retrieve host string from Redshift cluster properties
    Args:
        cluster_id: Cluster identifier (str)

    Returns: Redshift cluster endpoint (i.e. host) (str)

    """
    status, host = pretty_redshift_props(fetch_props(client=redshift, cluster_id=cluster_id))

    print('Waiting for cluster to become available...')

    while status != 'available':
        status, host = pretty_redshift_props(fetch_props(client=redshift, cluster_id=cluster_id))

    return host


if __name__ == "__main__":
    print('Creating cluster...')

    create_iam_role()
    attach_policy()
    create_cluster()
    host = get_host()

    print(f'Cluster created successfully. Host = {host}')
