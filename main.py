import csv
import boto3

s3 = boto3.resource('s3')

mybucket = s3.Bucket('BUCKET_NAME')

files: list = []

for s3_object in mybucket.objects.all():

    brute_filename: str = s3_object.key
    
    if brute_filename.startswith("monitor"):
        splited_path: list = brute_filename.split('/')
        filename = splited_path[-1]
        splited_filename = filename.split('-')
        
        file = {
            'stream': splited_filename[0],
            'channel': splited_filename[1],
            'phone_number': splited_filename[2],
            'date': splited_filename[3],
            'call_id': splited_filename[4],
            'filename': splited_filename[5],
            'raw_file': filename,
            'path': brute_filename
        }
        files.append(file)

header = files[0].keys()

with open ('export.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, header)
    dict_writer.writeheader()
    dict_writer.writerows(files)
