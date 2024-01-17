import csv
import time
import boto3
from botocore.exceptions import NoCredentialsError
from Bio import Entrez

def search(query, max_num_articles):
    handle = Entrez.esearch(db='pubmed',
                            sort='relevance',
                            retmax=max_num_articles,
                            retmode='xml',
                            term=query)
    results = Entrez.read(handle)
    return results

def fetch_details(id_list):
    ids = ','.join(id_list)
    handle = Entrez.efetch(db='pubmed',
                           retmode='xml',
                           id=ids)
    results = Entrez.read(handle)
    return results

def upload_to_s3(local_file, bucket, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_key)
        print(f"File uploaded to {bucket}/{s3_key}")
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False

def get_pubmed_data_as_csv(
    output_csv_file: str,
    start_date: str = "2023/12/1",
    end_date: str = "2023/12/31",
    email: str = 'your-mail-id@email.com',
    max_num_articles: int = 10000,
    s3_bucket: str = 'your-s3-bucket-name',
    s3_key: str = 'pubmed_data.csv'
):
    Entrez.email = email
    query = f"({start_date}[Date - Publication] : {end_date}[Date - Publication])"
    start_time = time.time()
    results = search(query, max_num_articles=max_num_articles)
    id_list = results['IdList']
    papers = fetch_details(id_list)

    header = ["Article Title", "Article Abstract", "Publication Year", "Publication Month", "Publication Day"]
    data_rows = []

    for paper in papers['PubmedArticle']:
        abstract = paper['MedlineCitation']['Article'].get('Abstract')
        date = paper['MedlineCitation']['Article']['ArticleDate']
        if abstract and date:
            data_row = [
                paper['MedlineCitation']['Article']['ArticleTitle'],
                abstract['AbstractText'][0],
                date[0]['Year'],
                date[0]['Month'],
                date[0]['Day'],
            ]
            data_rows.append(data_row)

    with open(output_csv_file, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(header)
        csv_writer.writerows(data_rows)

    if upload_to_s3(output_csv_file, s3_bucket, s3_key):
        print("Data uploaded to S3 successfully.")
    else:
        print("Failed to upload data to S3.")

if __name__ == "__main__":
    # Set your desired local CSV file path
    local_output_csv_file = "/path/to/local/fle/pubmed_data.csv"

    get_pubmed_data_as_csv(
        output_csv_file=local_output_csv_file,
        start_date="2023/12/1",
        end_date="2023/12/31",
        email="your-mail-id@email.com",
        max_num_articles=2000,
        s3_bucket="your-s3-bucket-name",
        s3_key="pubmed_data.csv"
    )
