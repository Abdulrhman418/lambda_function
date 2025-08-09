import boto3
import csv
from io import StringIO

s3 = boto3.client('s3')

BUCKET_NAME = "abdul-rhman-123"
CSV_FOLDER = 'daily_sales/'
REPORT_KEY = 'reports/weekly_report.txt'

def lambda_handler(event, context):
    dic_data = {}
    report = ''


    objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=CSV_FOLDER)

    if 'Contents' in objects:
        for obj in objects['Contents']:
            csv_file_name = obj['Key']
            if csv_file_name.endswith('.csv'):
                csv_file = s3.get_object(Bucket=BUCKET_NAME, Key=csv_file_name)
                csv_file_body = StringIO(csv_file['Body'].read().decode('utf-8'))
                spamreader = csv.reader(csv_file_body, delimiter=',')
                next(spamreader)  
                

                for row in spamreader:
                    product = row[2]
                    quantity = int(row[3])
                    price = float(row[4])
                    if product not in dic_data:
                        dic_data[product] = {"Quantity": 0, "Price": 0.0}
                    dic_data[product]["Quantity"] += quantity
                    dic_data[product]["Price"] += quantity * price


    for item in dic_data:
        report += f"{item} :\n\tQuantity: {dic_data[item]['Quantity']}\n\tPrice : {dic_data[item]['Price']}\n"

    s3.put_object(Bucket=BUCKET_NAME, Key=REPORT_KEY, Body=report.encode('utf-8'))

    return {
        'statusCode': 200,
        'body': f"Report generated and uploaded to s3://{BUCKET_NAME}/{REPORT_KEY}"
    }
