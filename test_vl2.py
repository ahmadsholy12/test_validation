import boto3

def list_files(bucket, prefix=''):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files = {}
    for page in page_iterator:
        if "Contents" in page:
            for obj in page['Contents']:

                files[obj['Key']] = {'Size': obj['Size'], 'ETag': obj['ETag'].strip('"')}
    return files

def compare_buckets(old_bucket_files, new_bucket_files):
    mismatches = []
    for key, old_data in old_bucket_files.items():
        new_data = new_bucket_files.get(key)
        if not new_data:
            mismatches.append((key, "Missing in new bucket"))
        elif old_data['Size'] != new_data['Size'] or old_data['ETag'] != new_data['ETag']:
            mismatches.append((key, "Mismatch"))

    extra_files = [key for key in new_bucket_files if key not in old_bucket_files]
    for key in extra_files:
        mismatches.append((key, "Extra in new bucket not in old"))
    return mismatches

def main():
    old_bucket = 'bloom-test-dir-spec'
    new_bucket = 'bali-dest-bucket'
    prefix = 'teamA/'

    old_bucket_files = list_files(old_bucket, prefix)
    new_bucket_files = list_files(new_bucket, prefix)

    mismatches = compare_buckets(old_bucket_files, new_bucket_files)

    if mismatches:
        print("There are mismatches or missing files:")
        for mismatch in mismatches:
            print(mismatch)
    else:
        print("All files match perfectly.")

if __name__ == '__main__':
    main()
