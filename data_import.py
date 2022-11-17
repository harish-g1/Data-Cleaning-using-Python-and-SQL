import mysql.connector
import csv

conn = mysql.connector.connect(host="localhost",user="root",passwd="<enter your password>",database="project_house")
cur = conn.cursor()

# create a table
create_sql = """CREATE TABLE  project_house.nashvillehousing (UniqueID  varchar(255),
ParcelID varchar(255),
LandUse varchar(255),
PropertyAddress varchar(255),
SaleDate varchar(255),
SalePrice varchar(255),
LegalReference varchar(255),
SoldAsVacant varchar(255),
OwnerName varchar(255),
OwnerAddress varchar(255),
Acreage varchar(255),
TaxDistrict varchar(255),
LandValue varchar(255),
BuildingValue varchar(255),
TotalValue varchar(255),
YearBuilt varchar(255),
Bedrooms varchar(255),
FullBath varchar(255),
HalfBath varchar(255))"""

cur.execute(create_sql)

# Enter the file location 
loc = "C:\\Data and Analytics\\Data Analysis\\Projects\\House Data\\Nashville Housing Data for Data Cleaning.csv"
csv_data = csv.reader(open(loc))
header = next(csv_data)

print("Inserting the data")
for row in csv_data:
    print(row)
    cur.execute("INSERT INTO project_house.nashvillehousing (UniqueID,	ParcelID, LandUse, PropertyAddress, SaleDate, "
                "SalePrice, LegalReference, SoldAsVacant, OwnerName, OwnerAddress,	Acreage, TaxDistrict, LandValue, "
                "BuildingValue,	TotalValue,	YearBuilt,	Bedrooms, FullBath, HalfBath) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,"
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row)

conn.commit()
conn.close()
print("Data import completed")