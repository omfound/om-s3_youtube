# om-s3_youtube
Python code to migrate sessions from a Granicus S3 archive into YouTube and Open.Media. These scripts are currently used internally at Open.Media and many of the dependencies (db, service, etc) are not included in this repository.

1) Create a new folder with appropriate data files in client data folder

2) Manually insert an entry for the client in the s3_youtube MySQL based on the s3 credentials provided by Granicus, ex: INSERT INTO clients (email, label, s3_access_key, s3_secret_key, s3_prefix, s3_bucket, granicus_id) VALUES ("glendaleca@openmediafoundation.org", "Glendale CA", "ACCESSKEY", "SECRETKEY", "glendale.granicus.com/mema_export/", "granicus-client-offboarding-us-east", "glendale"); 

3) Make sure s3youtube service is running ex: sudo service s3youtube start

4) Authenticate the client at https://s3-youtube.open.media

5) Backup the s3_youtube database now in case you need to roll back

6) Run granicus_s3_harvest.py to test metadata harvesting, ex: python3 granicus_s3_harvest.py --email=glendaleca@openmediafoundation.org --limit=500

7) If no errors, run the same command with --commit ex: python3 granicus_s3_harvest.py --email=glendaleca@openmediafoundation.org --limit=500 --commit

8) Optionally select distinct session_folder from clients imported sessions in MySQL and determine folder labels. You can use this to create a granicus_folders file in the client_data folder. Look at some of the other folders for an example. After creating the file rollback and run the harvest again.

9) Run s3_youtube.py to push videos to YouTube

10) Run granicus_s3_harvest_docs.py to pull down agendas and minutes

11) Run s3_documents_download.py to download agendas and minutes locally

12) Run granicus_harvest_html_minutes.py to get granicus html minutes

13) Run granicus_harvest_html_agendas.py to get granicus html agendas

14) Visit admin/migration to pull from this server's API into the live site  
