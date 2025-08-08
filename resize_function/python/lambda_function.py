import boto3
import json
import os
from PIL import Image
import io
import urllib.parse  

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key    = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])

    image_obj = s3.get_object(Bucket=bucket, Key=key)
    image_content = image_obj['Body'].read()

    # فتح الصورة باستخدام PIL
    image = Image.open(io.BytesIO(image_content))

    # Resize
    resized_image = image.resize((200, 200))

    # حفظ الصورة في buffer
    buffer = io.BytesIO()

    # استخدم JPEG كافتراضي لو النوع غير معروف
    image_format = image.format if image.format else 'JPEG'

    resized_image.save(buffer, format=image_format)
    buffer.seek(0)

    # تحديد اسم الصورة المصغرة
    filename = key.split('/')[-1]
    resized_key = 'resized/' + filename

    # رفع الصورة المصغرة إلى bucket آخر أو نفس البكت
    s3.put_object(
        Bucket='vuyhrfv',
        Key=resized_key,
        Body=buffer,
        ContentType=image_obj['ContentType']
    )

    return {
        'statusCode': 200,
        'body': json.dumps(f'Resized image uploaded to resized/{filename}')
    }
