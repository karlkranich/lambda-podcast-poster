import json
import boto3
import urllib.parse
import os

# add_episode adds an episode to the DynamoDB table
def add_episode(episodeNum, description, pubDate):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ccc-podcast')
    response = table.update_item(
        ExpressionAttributeNames={
            '#D': 'description',
            '#P': 'pub-date'
        },
        Key={'episode-num': episodeNum},
        UpdateExpression="set #D=:d, #P=:p",
        ExpressionAttributeValues={
            ':d': description,
            ':p': pubDate
        },
        ReturnValues="UPDATED_NEW"
    )
    
def lambda_handler(event, context):
    # Get parameters. If any are missing, return an error
    try:
        bodyDict = json.loads(event['body'])
        episodeNum = bodyDict['episode-num']
        description = urllib.parse.unquote(bodyDict['description'])
        pubDate = bodyDict['pub-date']
        password = bodyDict['password']
    except Exception as e:
        print('Missing parameters')
        print(e)
        print('=====')
        print(event)
        return {
            'statusCode': 400,
            'body': json.dumps('must supply required parameters'),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
        
    # Check password (stored in Lambda environment variable userPassword)
    if password != os.environ['userPassword']:
        return {
            'statusCode': 403,
            'body': json.dumps('forbidden'),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
        
    # Add or update episode in Dynamodb
    add_episode(int(episodeNum), description, pubDate)
    
    # Generate and return pre-signed URL for POSTing media file to S3
    bucket = 'kwksolutions.com'
    mediaFileS3Key = 'ccc/media/ccc-{:03d}-{}.mp3'.format(int(episodeNum), pubDate)
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_post(bucket, mediaFileS3Key, 
            Fields = {"Content-Type": "audio/mpeg"}, Conditions = [{"Content-Type": "audio/mpeg"}], ExpiresIn=300)
    except ClientError as e:
        logging.error(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Problem creating presigned URL'),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
    print('Successful run of cccRssPoster Lambda')
    return {
        'statusCode': 200,
        'body': json.dumps(response),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
