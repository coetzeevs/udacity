import configparser

import boto3

from helpers import fetch_props, pretty_redshift_props

config = configparser.ConfigParser()
config.read('dwh.cfg')


def get_clients():
    """
    Function to initialise and return the IAM and Redshift clients from boto3
    Returns: Redshift and IAM client objects
    """
    try:
        session = boto3.session.Session(aws_access_key_id=config.get('AWS', 'KEY'),
                                        aws_secret_access_key=config.get('AWS', 'SECRET'))
        iam = session.client('iam', config.get('AWS', 'REGION'))
        redshift = session.client('redshift', config.get('AWS', 'REGION'))

        return iam, redshift
    except ValueError as err:
        print(f'ValueError: {err}')
    except Exception as err:
        print(f'Uncaught exception: {err}')


def clean_up(rs_client, iam_client, cluster_id=config.get("CLUSTER", "IDENTIFIER"), iam_role_name=config.get("IAM_ROLE", "NAME")):
    """
    Function to initiate and verify deletion of Redshift cluster along with IAM role

    Args:
        rs_client: Redshift client object
        iam_client: IAM client object
        cluster_id: Cluster identification string [optional - inferred from configuration file]
        iam_role_name: IAM role name [optional - inferred from configuration file]

    Returns: None

    """
    rs_client.delete_cluster(ClusterIdentifier=cluster_id, SkipFinalClusterSnapshot=True)

    status, host = pretty_redshift_props(fetch_props(client=rs_client, cluster_id=cluster_id))

    print('Waiting for status "deleted"...')

    while status != 'deleted':
        status, host = pretty_redshift_props(fetch_props(client=rs_client, cluster_id=cluster_id))

    print(f'Successfully deleted cluster. Final status: {status}.')

    print(f'Detaching policy and deleting IAM role...')

    iam_client.detach_role_policy(RoleName=iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam_client.delete_role(RoleName=iam_role_name)

    print('Role detached and deleted successfully.')

    return


if __name__ == "__main__":
    try:
        print('Get clients...')
        im, rs = get_clients()

        print(f'Checking if cluster exists...')

        res = fetch_props(client=rs, cluster_id=config.get("CLUSTER", "IDENTIFIER"))

        if isinstance(res, tuple):
            print(f'Cluster not found.')
        else:
            print(f'Cluster found. Deleting it...')
            clean_up(rs_client=rs, iam_client=im)
            print('Cluster clean up finished.')

    except Exception as e:
        print(f'Uncaught exception on clean up: {e}')
