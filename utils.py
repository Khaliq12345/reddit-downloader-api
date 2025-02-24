from redvid import Downloader
import boto3
from dotenv import load_dotenv
import os
import uuid

load_dotenv()

ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
ACCESS_SECRET = os.getenv('S3_SECRET_KEY')

def session():
    return boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=ACCESS_SECRET,
        region_name="us-east-1"
    )

def generate_unique_id():
    return uuid.uuid4().fields[1]

def update_task(video_id: int, value: str, output: str = '', session: boto3.Session = session()):
    db = session.resource('dynamodb')
    table = db.Table('tasks')
    table.put_item(
        Item={
            'id': video_id,
            'status': value,
            'output': output
        }, 
    )
    return {'output': None, 'status': 'in progress', 'id': video_id}

def get_task(video_id: int, session: boto3.Session = session()):
    db = session.resource('dynamodb')
    table = db.Table('tasks')
    response = table.get_item(
        Key={
            'id': video_id
        }
    )
    if response.get('Item'):
        item = response.get('Item')
        item['id'] = int(item['id'])
        return item
    return {'output': None, 'status': 'not found', 'id': video_id}

def send_to_s3(output: str, session: boto3.Session = session()):
    s3 = session.resource("s3")
    output_file = output.replace('/', '_')
    file_key = f"videos/{output_file}"
    s3.Bucket("vxxxninput").upload_file(output, file_key)
    url = f"https://vxxxninput.s3.us-east-1.amazonaws.com/{file_key}"
    db = session.resource('dynamodb')
    table = db.Table('vxxxn_metadata')
    table.put_item(
        Item={
            'video_data': url
        }
    )
    os.remove(output)
    return url

def video_downloader(url: str):
    username = 'sp81iy5fpi'
    password = 'hv0Zzv7=7yYtE9xxPo'
    proxy = f"http://{username}:{password}@gate.smartproxy.com:10001"
    proxies = {
        'http': proxy,
        'https': proxy
    }
    reddit = Downloader(max_q=True, proxies=proxies, path='videos')
    reddit.url = url
    output = reddit.download()
    print(f'Output: {output}')
    if type(output) == int:
        return None
    else:
        return send_to_s3(output)
    
def task_handler(url: str, video_id: int):
    # Download the video
    try:
        output = video_downloader(url)
        if output:
            update_task(video_id, 'done', output)
    except (BaseException, Exception) as e:
        update_task(video_id, f'failed: {str(e)}')
    print('Task done')