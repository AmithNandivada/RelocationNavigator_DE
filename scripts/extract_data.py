import requests
import pandas as pd
import time
from google.oauth2 import service_account
from utils import GCPUtils


class ExtractData:
    def __init__(self, api_key, bucket_name, gcp_utils):
        self.api_key = api_key
        self.bucket_name = bucket_name
        self.gcp_utils = gcp_utils  # Pass the GCPUtils instance

        self.base_url = 'https://api.gateway.attomdata.com'
        self.property_version = "/propertyapi/v1.0.0"
        self.property_endpoint = '/property/basicprofile'
        self.base_params = {'pageSize': 500}
        self.zip_codes = ['35203', '99501', '85001', '72201', '90001', '80201', 
                        '06101', '19901', '33101', '30301', '96801', '83701', 
                        '60601', '46201', '52801', '66601']


    def extract_data_from_api(self, params):
        headers = {'accept': "application/json", 'apikey': self.api_key}
        response = requests.get(f'{self.base_url}{self.property_version}{self.property_endpoint}', params=params, headers=headers)

        if response.status_code != 200:
            response_data = response.json()
            if response_data.get('status', {}).get('msg') == 'SuccessWithoutResult':
                return response_data
            else:
                print("Failed to fetch data: ", response.text)
                response.raise_for_status()

        return response.json()

    def fetch_and_save_property_data(self):
        for zip_code in self.zip_codes:
            params = self.base_params.copy()
            params['postalcode'] = zip_code
            page = 1

            while True:
                params['page'] = page
                property_data = self.extract_data_from_api(params)

                if 'property' not in property_data or not property_data['property']:
                    print(f"No more data available for ZIP code: {zip_code} at page {page}")
                    break  # Break if no more data

                # df = pd.json_normalize(property_data['property'])
                clean_data_lst = self.clean_extracted_data(property_data)
                final_data_df = self.final_data(clean_data_lst)

                file_name = f"property_data_zip_{zip_code}_page_{page}.csv"
                self.gcp_utils.save_data_to_gcs(final_data_df, self.bucket_name, file_name)
                page += 1
                time.sleep(1)  # To respect API rate limits

        print("Data fetching completed.")

    def clean_extracted_data(self, property_data):
        """
        Function to clean the extracted API data.
        """
        property_id_lst = []
        lot_size_acres_lst = []
        lot_size_sqrft_lst = []
        pool_access_lst = []
        property_address_lst = []
        property_country_lst = []
        latitude_lst = []
        longitude_lst = []
        property_type_lst = []
        propsubtype_lst = []
        propIndicator_lst = []
        proptype_lst = []
        property_year_built_lst = []
        absenteeInd_lst = []
        coolingtype_lst = []
        heatingtype_lst = []
        wallType_lst = []
        baths_total_lst = []
        baths_partial_lst = []
        baths_full_lst = []
        bed_lst = []
        rooms_total_lst = []
        bsmtsize_lst = []
        bsmttype_lst = []
        bsmtFinishedPercent_lst = []
        universalsize_lst = []
        bldgsize_lst = []
        grosssize_lst = []
        grosssizeadjusted_lst = []
        groundfloorsize_lst = []
        livingSize_lst = []
        condition_lst = []
        constructiontype_lst = []
        frameType_lst = []
        roofcover_lst = []
        garagetype_lst = []
        prkgSize_lst = []
        prkgType_lst = []
        prkgSpaces_lst = []
        archStyle_lst = []
        bldgType_lst = []
        levels_lst = []
        quality_lst = []
        view_lst = []
        yearbuilteffective_lst = []
        lastModified_lst = []
        pubDate_lst = []
        saleSearchDate_lst = []
        saleTransDate_lst = []
        transactionIdent_lst = []
        saleAmt_lst= []
        saleCode_lst = []
        saleRecDate_lst = []
        saleDocType_lst = []
        saleDocNum_lst = []
        saleTransType_lst = []
        assdImprValue_lst = []
        assdLandValue_lst = []
        assdTtlValue_lst = []
        mktImprValue_lst = []
        mktLandValue_lst = []
        mktTtlValue_lst = []
        taxAmt_lst = []
        taxPerSizeUnit_lst = []
        taxYear_lst = []
        delinquentyear_lst = []
        improvementPercent_lst = []


        for property_info in property_data["property"]:
            # Access specific sub-branches within each property dictionary
            property_id = property_info['identifier']['Id'] # ATTOM's legacy property property identifier


            lot = property_info['lot']
            lot_size_acres = lot.get('lotSize1', 'N/A') # lot size in acres
            lot_size_sqrft = lot.get('lotSize2', 'N/A') # Indicates the lot size, in square feet
            pool_access = lot.get('poolType', 'N/A') # Indicates the type of pool if it exists


            address = property_info['address']
            # address - oneLine, country
            property_address = address.get('oneLine', 'N/A') # Full property address on a single line
            property_country = address.get('country', 'N/A') # The ISO-3166-2 country code


            location = property_info["location"]
            # location - latitude, longitude
            latitude = location.get("latitude", "N/A") # The latitude of the property in degrees
            longitude = location.get("longitude", "N/A") # The longitude of the property in degrees


            summary = property_info["summary"]
            # summary - propertyType, propsubtype, propIndicator, proptype, yearbuilt
            absenteeInd = summary.get('absenteeInd', 'N/A') # Owner status description - "Absentee Owner" or "Owner Occupied" - Logic based
            propsubtype = summary.get("propSubType", "N/A") # A sub-classification of the property
            proptype = summary.get("propType", "N/A") # A specific property classification such as "Detached Single Family"
            property_type = summary.get("propertyType", "N/A") # General property type description; residential, commercial, other, etc.
            propIndicator = summary.get("propIndicator", "N/A") # Generalized property type grouping
            property_year_built = summary.get('yearBuilt', 'N/A') # Year built of the primary structure


            utilities = property_info["utilities"]
            # utilities - cool, heat
            coolingtype = utilities.get('coolingType', 'N/A') # Indicates the method or system used to provide cooling
            heatingtype = utilities.get('heatingType', 'N/A') # Indicates the method or system used to provide heat
            wallType = utilities.get('wallType', 'N/A') # Indicates the primary exterial wall covering material


            sale = property_info["sale"]
            saleSearchDate = sale.get("saleSearchDate", "N/A") # The standardized date for search purposes
            saleTransDate = sale.get("saleTransDate", "N/A") # The signature date on the recorded document (May be the same or pre-date the recording date)
            transactionIdent = sale.get("transactionIdent", "N/A")

            saleAmountData = sale["saleAmountData"]
            saleAmt = saleAmountData.get("saleAmt", "N/A") # Sale Price
            saleCode = saleAmountData.get("saleCode", "N/A") # Code indicating whether the sale amount is actual or estimated
            saleRecDate = saleAmountData.get("saleRecDate", "N/A") # The recorded date on the recorded document
            saleDocType = saleAmountData.get("saleDocType", "N/A") # Code identifying the type of document; grant deed, quit claim, etc...
            saleDocNum = saleAmountData.get("saleDocNum", "N/A") # The Recorded Instrument's Document Number
            saleTransType = saleAmountData.get("saleTransType", "N/A") # Code identifying the type of transaction; transfer, loan, etc..


            building = property_info["building"]
            # building - rooms, interior, size, construction, parking, summary

            size = building["size"]
            bldgsize = size.get("bldgSize", "N/A") # Total square feet of all structures on the property
            grosssize = size.get("grossSize", "N/A") # Gross square feet of all structures on the property
            grosssizeadjusted = size.get("grossSizeAdjusted", "N/A") # Adjusted Gross Square Footage where applicable, otherwise populated with Building Square Footage
            groundfloorsize = size.get("groundFloorSize", "N/A") # The sum of ground floor living square footage (May include unfinished square footage)
            universalsize = size.get("universalSize", "N/A") # Derived living or building square footage
            livingSize = size.get('livingSize', 'N/A') # Living square feet of all structures on the property

            rooms = building["rooms"]
            baths_total = rooms.get('bathsTotal', 'N/A') # The total number of rooms that are utilized as bathrooms (Includes partial bathrooms and value may be interpreted)
            baths_partial = rooms.get('bathsPartial', 'N/A') # The total number of rooms that are utilized as bathrooms and are partial bathroom by common real estate definition.
            baths_full = rooms.get('bathsFull', 'N/A') # Number of Full Baths - tyically sink, toilet, shower and/or bath tub
            bed = rooms.get('beds', 'N/A') # The total number of rooms that can be qualified as bedrooms
            rooms_total = rooms.get('roomsTotal', 'N/A') # The total number of rooms for all the buildings on the property (If multiple buildings exist, the values are aggregated)

            interior = building["interior"]
            bsmtsize = interior.get('bsmtSize', 'N/A') # The total area of the basement, in square feet
            bsmttype = interior.get('bsmtType', 'N/A') # Indicates if the basement is finished or unfinished
            bsmtFinishedPercent = interior.get('bsmtFinishedPercent', 'N/A')

            construction = building["construction"]
            condition = construction.get("condition", "N/A") # Indicates the Building Condition
            constructiontype = construction.get("constructionType", "N/A") # Construction Type
            frameType = construction.get("frameType", "N/A") # Construction materials used for the interior walls
            roofcover = construction.get("roofCover", "N/A") # Indicates the primary finish material of which the roof is made

            parking = building["parking"]
            garagetype = parking.get("garageType", "N/A") # Indicates if a garage exists on the property and any additional information about the garage such as attached / detached, etc...
            prkgSize = parking.get("prkgSize", "N/A") # Garage square footage
            prkgType = parking.get("prkgType", "N/A") # Indicates if parking type is garage or carport
            prkgSpaces = parking.get('prkgSpaces', 'N/A') # The total number of parking spaces exclusive to the property

            build_summary = building["summary"]
            levels = build_summary.get("levels", "N/A") # The number of stories for the buildings on the property (If multiple buildings exist, the values are aggregated)
            bldgType = build_summary.get("storyDesc", "N/A") # Indicates the Building Style
            view = build_summary.get("view", "N/A") # Indicates whether there is an ocean or mountain or other pleasant or unpleasant View from the property.
            archStyle = build_summary.get("archStyle", "N/A") # Indicates the structural style or the presence of specific style elements in the structure
            quality = build_summary.get("quality", "N/A") # Indicates the quality of the home/structure
            yearbuilteffective = build_summary.get("yearbuilteffective", "N/A") # Adjusted year built based on condition and / or major structural changes of the structure


            assessment = property_info["assessment"]

            assessed = assessment["assessed"]
            assdImprValue = assessed.get("assdImprValue", "N/A") # Assessed Value of the improvement(s)
            assdLandValue = assessed.get("assdLandValue", "N/A") # Assessed Value of the land
            assdTtlValue = assessed.get("assdTtlValue", "N/A") # Total Assessed Value

            market = assessment["market"]
            mktImprValue = market.get("mktImprValue", "N/A") # Market Value of the improvement(s)
            mktLandValue = market.get("mktLandValue", "N/A") # Market Value of the land
            mktTtlValue = market.get("mktTtlValue", "N/A") # Total Market Value

            tax = assessment["tax"]
            taxAmt = tax.get("taxAmt", "N/A") # Tax Amount billed for the respective Tax Year
            taxPerSizeUnit = tax.get("taxPerSizeUnit", "N/A") # The total taxes per square foot for the property
            taxYear = tax.get("taxYear", "N/A") # The respective year of the property taxes being provided

            delinquentyear = assessment.get("delinquentyear", "N/A") # The tax year for which property taxes have not been paid by the owner or impound account
            improvementPercent = assessment.get("improvementPercent", "N/A") # Percentage of the improvement value against the total value


            vintage = property_info["vintage"]
            lastModified = vintage["lastModified"] # The date the record was last modified
            pubDate = vintage["pubDate"] # The publish date of the source data this record belongs to

            property_id_lst.append(property_id)

            lot_size_acres_lst.append(lot_size_acres)
            lot_size_sqrft_lst.append(lot_size_sqrft)
            pool_access_lst.append(pool_access)

            property_address_lst.append(property_address)
            property_country_lst.append(property_country)

            latitude_lst.append(latitude)
            longitude_lst.append(longitude)

            property_type_lst.append(property_type)
            propsubtype_lst.append(propsubtype)
            propIndicator_lst.append(propIndicator)
            proptype_lst.append(proptype)
            property_year_built_lst.append(property_year_built)
            absenteeInd_lst.append(absenteeInd)

            coolingtype_lst.append(coolingtype)
            heatingtype_lst.append(heatingtype)
            wallType_lst.append(wallType)

            saleSearchDate_lst.append(saleSearchDate)
            saleTransDate_lst.append(saleTransDate)
            transactionIdent_lst.append(transactionIdent)
            saleAmt_lst.append(saleAmt)
            saleCode_lst.append(saleCode)
            saleRecDate_lst.append(saleRecDate)
            saleDocType_lst.append(saleDocType)
            saleDocNum_lst.append(saleDocNum)
            saleTransType_lst.append(saleTransType)

            universalsize_lst.append(universalsize)
            bldgsize_lst.append(bldgsize)
            grosssize_lst.append(grosssize)
            grosssizeadjusted_lst.append(grosssizeadjusted)
            groundfloorsize_lst.append(groundfloorsize)
            livingSize_lst.append(livingSize)

            baths_total_lst.append(baths_total)
            baths_partial_lst.append(baths_partial)
            baths_full_lst.append(baths_full)
            bed_lst.append(bed)
            rooms_total_lst.append(rooms_total)

            bsmtsize_lst.append(bsmtsize)
            bsmttype_lst.append(bsmttype)
            bsmtFinishedPercent_lst.append(bsmtFinishedPercent)

            condition_lst.append(condition)
            constructiontype_lst.append(constructiontype)
            frameType_lst.append(frameType)
            roofcover_lst.append(roofcover)

            garagetype_lst.append(garagetype)
            prkgSize_lst.append(prkgSize)
            prkgType_lst.append(prkgType)
            prkgSpaces_lst.append(prkgSpaces)

            levels_lst.append(levels)
            bldgType_lst.append(bldgType)
            view_lst.append(view)
            archStyle_lst.append(archStyle)
            quality_lst.append(quality)
            yearbuilteffective_lst.append(yearbuilteffective)

            assdImprValue_lst.append(assdImprValue)
            assdLandValue_lst.append(assdLandValue)
            assdTtlValue_lst.append(assdTtlValue)
            mktImprValue_lst.append(mktImprValue)
            mktLandValue_lst.append(mktLandValue)
            mktTtlValue_lst.append(mktTtlValue)
            taxAmt_lst.append(taxAmt)
            taxPerSizeUnit_lst.append(taxPerSizeUnit)
            taxYear_lst.append(taxYear)
            delinquentyear_lst.append(delinquentyear)
            improvementPercent_lst.append(improvementPercent)

            lastModified_lst.append(lastModified)
            pubDate_lst.append(pubDate)

        return [property_id_lst,
                lot_size_acres_lst,
                lot_size_sqrft_lst,
                pool_access_lst,
                property_address_lst,
                property_country_lst,
                latitude_lst,
                longitude_lst,
                property_type_lst,
                propsubtype_lst,
                propIndicator_lst,
                proptype_lst,
                property_year_built_lst,
                absenteeInd_lst,
                coolingtype_lst,
                heatingtype_lst,
                wallType_lst,
                saleSearchDate_lst,
                saleTransDate_lst,
                transactionIdent_lst,
                saleAmt_lst,
                saleCode_lst,
                saleRecDate_lst,
                saleDocType_lst,
                saleDocNum_lst,
                saleTransType_lst,
                universalsize_lst,
                bldgsize_lst,
                grosssize_lst,
                grosssizeadjusted_lst,
                groundfloorsize_lst,
                livingSize_lst,
                baths_total_lst,
                baths_partial_lst,
                baths_full_lst,
                bed_lst,
                rooms_total_lst,
                bsmtsize_lst,
                bsmttype_lst,
                bsmtFinishedPercent_lst,
                condition_lst,
                constructiontype_lst,
                frameType_lst,
                roofcover_lst,
                garagetype_lst,
                prkgSize_lst,
                prkgType_lst,
                prkgSpaces_lst,
                levels_lst,
                bldgType_lst,
                view_lst,
                archStyle_lst,
                quality_lst,
                yearbuilteffective_lst,
                assdImprValue_lst,
                assdLandValue_lst,
                assdTtlValue_lst,
                mktImprValue_lst,
                mktLandValue_lst,
                mktTtlValue_lst,
                taxAmt_lst,
                taxPerSizeUnit_lst,
                taxYear_lst,
                delinquentyear_lst,
                improvementPercent_lst,
                lastModified_lst,
                pubDate_lst]



    def final_data(self, extracted_data_lst):
        """
        This function will have final extracted data.
        """
        col_names_lst = ["property_id",
                        "lot_size_acres",
                        "lot_size_sqrft",
                        "pool_access",
                        "property_address",
                        "property_country",
                        "latitude",
                        "longitude",
                        "property_type",
                        "propsubtype",
                        "propIndicator",
                        "proptype",
                        "property_year_built",
                        "absenteeInd",
                        "coolingtype",
                        "heatingtype",
                        "wallType",
                        "saleSearchDate",
                        "saleTransDate",
                        "transactionIdent",
                        "saleAmt",
                        "saleCode",
                        "saleRecDate",
                        "saleDocType",
                        "saleDocNum",
                        "saleTransType",
                        "universalsize",
                        "bldgsize",
                        "grosssize",
                        "grosssizeadjusted",
                        "groundfloorsize",
                        "livingsize",
                        "baths_total",
                        "baths_partial",
                        "baths_full",
                        "bed",
                        "rooms_total",
                        "bsmtsize",
                        "bsmttype",
                        "bsmtFinishedPercent",
                        "condition",
                        "constructiontype",
                        "frameType",
                        "roofcover",
                        "garagetype",
                        "prkgSize",
                        "prkgType",
                        "prkgSpaces",
                        "levels",
                        "bldgType",
                        "view",
                        "archStyle",
                        "quality",
                        "yearbuilteffective",
                        "assdImprValue",
                        "assdLandValue",
                        "assdTtlValue",
                        "mktImprValue",
                        "mktLandValue",
                        "mktTtlValue",
                        "taxAmt",
                        "taxPerSizeUnit",
                        "taxYear",
                        "delinquentyear",
                        "improvementPercent",
                        "lastModified",
                        "pubDate"]

        property_data_df = pd.DataFrame()
        
        for col, data in zip(col_names_lst, extracted_data_lst):
            property_data_df[col]=data

        return property_data_df


def extract_data_main():
    # e2f6093efe653fa0cf74a5902912f64a, a32aebb47b8078c9d90e487e5f816cf7
    api_key = 'a32aebb47b8078c9d90e487e5f816cf7'
    gcs_bucket_name = 'landing_bucket17'

    gcp_utils = GCPUtils()

    extract_data = ExtractData(api_key, gcs_bucket_name, gcp_utils)
    extract_data.fetch_and_save_property_data()

if __name__ == "__main__":
    extract_data_main()