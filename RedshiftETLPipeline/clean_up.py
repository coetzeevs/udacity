import configparser

import boto3

from helpers import fetch_props, prettyRedshiftProps

config = configparser.ConfigParser()
config.read('dwh.cfg')

try:
    session = boto3.session.Session(aws_access_key_id=config.get('AWS','KEY'), aws_secret_access_key=config.get('AWS','SECRET'))
    iam = session.client('iam', config.get('AWS','REGION'))
    redshift = session.client('redshift', config.get('AWS','REGION'))
except ValueError as e:
    print(f'ValueError: {e}')
except Exception as e:
    print(f'Uncaught exception: {e}')

def clean_up(cluster_id=config.get("CLUSTER","IDENTIFIER"), iam_role_name=config.get("IAM_ROLE", "NAME")):
    redshift.delete_cluster(ClusterIdentifier=cluster_id,  SkipFinalClusterSnapshot=True)
    
    status, host = prettyRedshiftProps(fetch_props(client=redshift, cluster_id=cluster_id))
    
    print('Waiting for status "deleted"...')

    while status != 'deleted':
        status, host = prettyRedshiftProps(fetch_props(client=redshift, cluster_id=cluster_id))

    print(f'Successfully deleted cluster. Final status: {status}.')

    print(f'Detaching policy and deleting IAM role...')          
    
    iam.detach_role_policy(RoleName=iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=iam_role_name)
          
    print('Role detached and deleted successfully.')
    
    return

if __name__ == "__main__":
    try:
        print(f'Checking if cluster exists...')
        
        res = fetch_props(client=redshift, cluster_id=config.get("CLUSTER","IDENTIFIER"))
        
        if isinstance(res, tuple):
            print(f'Cluster not found.')
        else:
            print(f'Cluster found. Deleting it...')
            status = clean_up()
            print('Cluster clean up finished.')
        
    except Exception as e:
        print(f'Uncaught exception on clean up: {e}')
