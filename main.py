import decimal

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')

table = dynamodb.create_table(
    TableName='Movies',
    KeySchema=[
        {
            'AttributeName': 'year',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'title',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'year',
            'AttributeType': 'N',
        },
        {
            'AttributeName': 'title',
            'AttributeType': 'S',
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5,
    }
)

table = dynamodb.Table('Movies')

put_response = table.put_item(Item={'year': 2000, 'title': 'Star Wars 3', 'info': {
    'plot': 'star wars',
    'rating': decimal.Decimal('8.8')
}})

update_response = table.update_item(
    Key={'year': 2000, 'title': 'Star Wars 3'},
    UpdateExpression='remove info',
    ReturnValues='UPDATED_NEW'
)

get_response = table.get_item(Key={'year': 2000, 'title': 'Star Wars 3'})

delete_response = table.delete_item(Key={'year': 2000, 'title': 'Star Wars 3'})

query_response = table.query(
    ProjectionExpression="#yr, title, info.plot",
    ExpressionAttributeNames={"#yr": "year"},
    KeyConditionExpression=Key('year').eq(2000) & Key('title').begins_with('Star')
)

print(get_response['Item'], query_response['Items'], sep='\n')
