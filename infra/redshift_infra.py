# Import libraries
import os
import time
import sys
import json
import boto3
import configparser



# Configuration Variables
config = configparser.ConfigParser()
config_dir = os.path.dirname(os.getcwd()) + "\config\dwh.cfg"
config.read_file(open(config_dir))

# AWS Credentials
KEY                    = config.get("AWS", "KEY")
SECRET                 = config.get("AWS", "SECRET")

#Redshift Cluster
DWH_CLUSTER_TYPE       = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH", "DWH_DB")
DWH_DB_USER            = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH", "DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")


def create_redshift_role_s3_read():
    """Create an IAM role for Redshift and attach a policy for reading objects from the S3 bucket and saves the Role in the config/dwh.cfg file..
    
    Return
    ------
        iam: object
            IAM service connection object
    """
    try:
        print(("="*15) + " STEP 1.1: Creating IAM Role " + ("="*15))

        iam = boto3.client("iam",
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)
        
        iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                "Statement": [{"Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"}}],
                "Version": "2012-10-17"})
            )
        print("IAM Role Created successfully!")
    except Exception as e:
        print("Failed to create IAM Role! Exception: ",e)

    try:
        print(("="*15) + " STEP 1.2: Attaching Policy to IAM Role " + ("="*15))

        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )["ResponseMetadata"]["HTTPStatusCode"]
        print("Attached Policy to IAM Role")
    except Exception as e:
        print("Failed to create IAM Role! Exception: ",e)

    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]

    config.set("IAM_ROLE", "ARN", roleArn)

    with open(config_dir, "w") as configfile:
        config.write(configfile)

    return iam

def create_redshift_cluster():
    """Creates the Redshift cluster where the staging area and the Data Warehouse will be created and saves cluster connection variables in the config/dwh.cfg file.
    
    Return
    ------
        vpc_id: string
            The cluster's VPC ID.
        redshift: object
            Redshift service connection object
    """

    try:
        print(("="*15) + " STEP 3.1: Creating Redshift Cluster " + ("="*15))

        redshift = boto3.client("redshift",
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
        
        redshift.create_cluster(        
            #DHW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[config.get("IAM_ROLE", "ARN")]  
        )
    except Exception as e:
        print("Failed to create Redshift Cluster! Exception: ", e)

    # Get Cluster status
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)["Clusters"][0]

    # Wait for the Cluster Creation
    while myClusterProps["ClusterStatus"] != "available":
        print("Creating Cluster... This may take a while...")
        time.sleep(15)

    DWH_ENDPOINT = myClusterProps["Endpoint"]["Address"]

    config.set("CLUSTER", "HOST", DWH_ENDPOINT)
    config.set("CLUSTER", "DB_NAME", DWH_DB)
    config.set("CLUSTER", "DB_USER", DWH_DB_USER)
    config.set("CLUSTER", "DB_PASSWORD", DWH_DB_PASSWORD)
    config.set("CLUSTER", "DB_PORT", DWH_PORT)

    with open(config_dir, "w") as configfile:
        config.write(configfile)

    return myClusterProps["VpcId"], redshift

def open_cluster_port(vpc_id):
    """Opens port 5439 for remote connection to the Cluster.
    
    Parameter
    ----------
        vpc_id: string
            The cluster's VPC ID.
    """
    try:
        print(("="*15) + " STEP 4: Opening Port 5439 of Redshift Cluster " + ("="*15))
        ec2 = boto3.resource("ec2",
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
        
        vpc = ec2.Vpc(id=vpc_id)

        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print("Failed to open port 5439! Exception: ", e)

def delete_resources(redshift, iam):
    """Deletes the Redshift cluster and the IAM role."""

    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

def main():

    if len(sys.argv) == 1:
        print("""
            Choose action to be passed as parameter to be performed:
                --create - create infrastructure.
                --destroy - delete infrastructure services.
            """)
    
    elif sys.argv[1] == "--create":
        iam = create_redshift_role_s3_read()

        vpc_id, redshift = create_redshift_cluster()
        
        open_cluster_port(vpc_id)

    elif sys.argv[1] == "--destroy":
        delete_resources(redshift, iam)

    else:
        print("""
            Choose action to be passed as parameter to be performed:
                --create - create infrastructure.
                --destroy - delete infrastructure services.
            """)

if __name__ == "__main__":
    main()