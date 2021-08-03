import boto3
import iamzero

iamzero.init()

# loads the region that you have specified in your AWS CLI configuration
my_session = boto3.session.Session()
region = my_session.region_name

# if the user hasn't defined a region, choose a fallback region, since this is just an example
if not region:
    region = "us-west-1"
    print(f"Warning: AWS_REGION not found, defaulting to {region}...")


s3 = boto3.client("s3", region_name=region)

s3.create_bucket(
    Bucket="iamzero-test-access-bucket",
    CreateBucketConfiguration={"LocationConstraint": region},
)
