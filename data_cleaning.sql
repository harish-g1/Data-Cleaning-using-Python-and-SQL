-- Below are the data cleaning process followed in SQL
  
-- Look for null PropertyAddress
select * from project_house.nashvillehousing where PropertyAddress is null;

select * from project_house.nashvillehousing where PropertyAddress = '';

-- ParcelID and PropertyAddress have one to one relationship

-- update all the null PropertyAddress 
update project_house.nashvillehousing a, project_house.nashvillehousing b
set a.PropertyAddress = coalesce(a.propertyaddress,b.PropertyAddress) 
where a.ParcelID = b.ParcelID and a.UniqueID <> b.UniqueID and a.PropertyAddress is null;

----------------------------------------------------------------------------------------------------------------------------------------------
-- Fetching the Address and City from PropertyAddress.
-- PropertyAddress is seperated by a comma delimeter

-- add PropertySplitAddress and PropertySplitCity columns
alter table project_house.nashvillehousing
add (PropertySplitAddress varchar(255), PropertySplitCity varchar(255));

update  project_house.nashvillehousing  
set PropertySplitAddress = substr(propertyaddress, 1, instr(propertyaddress,',')-1), 
PropertySplitCity = substr(PropertyAddress, instr(propertyaddress, ',') + 1);
commit;


------------------------------------------------------------------------------------------------------------------------------------------
-- Fetching Address, City and State from OwnerAddress
-- OwnerAddress is seperated by a comma delimeter

-- Add three columns OwnerSplitAddress, OwnerSplitCity and OwnerSplitState
alter table project_house.nashvillehousing
add (OwnerSplitAddress varchar(255), OwnerSplitCity varchar(255), OwnerSplitState varchar(255));

-- fill the newly added columns
update project_house.nashvillehousing
set OwnerSplitAddress = substr(owneraddress,1,instr(owneraddress, ',') -1),
OwnerSplitCity = substr(OwnerAddress, instr(owneraddress, ',') +2, locate(',', owneraddress, instr(owneraddress,',') +1 ) - (instr(owneraddress, ',') +2)   ),
OwnerSplitState = substr(OwnerAddress, locate(',', owneraddress, instr(owneraddress,',') +1 ) +2 );
commit;

select * from project_house.nashvillehousing ;

-------------------------------------------------------------------------------------------------------------------------------------------
-- convert Soldasvacant column to have distinct value of Yes and No
-- Convert Y to Yes and N to No

update project_house.nashvillehousing
set soldasvacant = (case when trim(soldasvacant) = 'N' then 'No' when trim(soldasvacant) = 'Y' then 'Yes' else trim(soldasvacant) end );

commit;

select soldasvacant, count(SoldAsVacant) 
from project_house.nashvillehousing 
group by soldasvacant;

--------------------------------------------------------------------------------------------------------------------------------------------
-- delete duplicate records from nashvillehousing table
-- unique records can be identified using the combination of parcelID, Propertyaddress, saleprice, saledate, legalreference, ownername, owneraddress

-- using the window function row_number() we can identify duplicates
-- UniqueID is the unique per record, so deleting on the basis of uniqueID
delete from project_house.nashvillehousing
where UniqueID in (
select UniqueID from (
select tbl.*, 
row_number() over (partition by parcelID, Propertyaddress, saleprice, saledate, legalreference, ownername, owneraddress order by parcelid) row_num 
from project_house.nashvillehousing tbl) tbl2
where row_num > 1);

commit;

--------------------------------------------------------------------------------------------------------------------------------------------

-- We don't need OnwerAdress and PropertyAdress column as we have split them into seperate columns
-- We also don't need TaxDistrict

-- Create a view with only the columns needes for further analysis and visualization
create or replace view project_house.v_nashvillehousing  as
(
select uniqueID, ParcelID, LandUse,  cast(SalePrice as float) SalePrice, LegalReference, SoldAsVacant, OwnerName, 
Acreage, LandValue, BuildingValue, TotalValue, yearBuilt, Bedrooms, FullBath, HalfBath, saledate, PropertySplitAddress, PropertySplitCity,
OwnerSplitAddress, OwnerSplitCity, OwnerSplitState
from project_house.nashvillehousing );

select * from project_house.v_nashvillehousing;

