import psycopg2

# Connecting to RedShift
con=psycopg2.connect(dbname= 'dev', host='redshift.amazonaws.com', port='5439',
                     user='awsuser', password='PLACEHOLDER')

# Copy Command as a Variable
copy_command ="""copy src_dimproductsubcategory (productsubcategorykey ,productsubcategoryalternatekey ,englishproductsubcategoryname ,spanishproductsubcategorysame ,frenchproductsubcategorysame ,productcategorykey )
from 's3://your-s3-bucket-name/public/DimProductSubcategory/DimProductSubcategory.csv'
iam_role 'arn:aws:iam::879408800400:role/'
DELIMETER ','
IGNOREHEADER 1;"""

# Opening a cursor and run copy quer
cur = con.cursor()
cur.execute("truncate table src_dimproductsubcategory;")
cur.execute(copy_command)
con.commit()

# Close the cursor and the connection
cur.close()
con.close()

print("Job has finished executing!")