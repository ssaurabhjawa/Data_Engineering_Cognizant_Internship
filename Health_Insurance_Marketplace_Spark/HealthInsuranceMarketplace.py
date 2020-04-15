import requests, os
from kafka import KafkaProducer
from pyspark.sql import SparkSession, Row

# from pyspark.sql import SparkSession    
# spark = SparkSession.builder.getOrCreate()

def kafka_BenefitCost_1():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partOne.txt")
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
       producer.send('benefitCostSharing_1', data.encode('utf-8'))
    producer.flush()

def spark_BenefitCost_1():
   os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
   spark = SparkSession.builder.getOrCreate()
   raw_kafka_df = spark.readStream \
                       .format("kafka") \
                       .option("kafka.bootstrap.servers", "localhost:9092") \
                       .option("subscribe", 'benefitCostSharing_1') \
                       .option("startingOffsets", "earliest") \
                       .load()
   kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
   output_query = kafka_value_df.writeStream \
                         .queryName("benefit_cost_sharing_1") \
                         .format("memory") \
                         .start()
   output_query.awaitTermination(10)
   value_df = spark.sql("select * from benefit_cost_sharing_1")
   value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
   value_row_rdd = value_rdd.map(lambda i: Row(BenefitName=i[0], \
                                               BusinessYear=i[1], \
                                               EHBVarReason=i[2], \
                                               IsCovered=i[3], \
                                               IssuerId=i[4], \
                                               LimitQty=i[5], \
                                               LimitUnit=i[6], \
                                               MinimumStay=i[7], \
                                               PlanId=i[8], \
                                               SourceName=i[9], \
                                               StateCode=i[10]
                                               ))

   df = spark.createDataFrame(value_row_rdd)
   df.show(5)
   df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','Health_Insurance_Marketplace') \
        .option('collection', 'benefit_cost') \
        .option('uri', "mongodb://127.0.0.1/Health_Insurance_Marketplace.dbs") \
        .save()

def kafka_BenefitCost_2():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partTwo.txt")
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
       producer.send('benefitCostSharing_2', data.encode('utf-8'))
    producer.flush()

def spark_BenefitCost_2():
   os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
   spark = SparkSession.builder.getOrCreate()
   raw_kafka_df = spark.readStream \
                       .format("kafka") \
                       .option("kafka.bootstrap.servers", "localhost:9092") \
                       .option("subscribe", 'benefitCostSharing_2') \
                       .option("startingOffsets", "earliest") \
                       .load()
   kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
   output_query = kafka_value_df.writeStream \
                         .queryName("benefit_cost_sharing_2") \
                         .format("memory") \
                         .start()
   output_query.awaitTermination(10)
   value_df = spark.sql("select * from benefit_cost_sharing_2")
   value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
   value_row_rdd = value_rdd.map(lambda i: Row(BenefitName=i[0], \
                                               BusinessYear=i[1], \
                                               EHBVarReason=i[2], \
                                               IsCovered=i[3], \
                                               IssuerId=i[4], \
                                               LimitQty=i[5], \
                                               LimitUnit=i[6], \
                                               MinimumStay=i[7], \
                                               PlanId=i[8], \
                                               SourceName=i[9], \
                                               StateCode=i[10]
                                               ))

   df = spark.createDataFrame(value_row_rdd)
   df.show(5)
   df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','Health_Insurance_Marketplace') \
        .option('collection', 'benefit_cost') \
        .option('uri', "mongodb://127.0.0.1/Health_Insurance_Marketplace.dbs") \
        .save()

def kafka_BenefitCost_3():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partThree.txt")
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
       producer.send('benefitCostSharing_3', data.encode('utf-8'))
    producer.flush()

def spark_BenefitCost_3():
   os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
   spark = SparkSession.builder.getOrCreate()
   raw_kafka_df = spark.readStream \
                       .format("kafka") \
                       .option("kafka.bootstrap.servers", "localhost:9092") \
                       .option("subscribe", 'benefitCostSharing_3') \
                       .option("startingOffsets", "earliest") \
                       .load()
   kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
   output_query = kafka_value_df.writeStream \
                         .queryName("benefit_cost_sharing_3") \
                         .format("memory") \
                         .start()
   output_query.awaitTermination(10)
   value_df = spark.sql("select * from benefit_cost_sharing_3")
   value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
   value_row_rdd = value_rdd.map(lambda i: Row(BenefitName=i[0], \
                                               BusinessYear=i[1], \
                                               EHBVarReason=i[2], \
                                               IsCovered=i[3], \
                                               IssuerId=i[4], \
                                               LimitQty=i[5], \
                                               LimitUnit=i[6], \
                                               MinimumStay=i[7], \
                                               PlanId=i[8], \
                                               SourceName=i[9], \
                                               StateCode=i[10]
                                               ))

   df = spark.createDataFrame(value_row_rdd)
   df.show(5)
   df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','Health_Insurance_Marketplace') \
        .option('collection', 'benefit_cost') \
        .option('uri', "mongodb://127.0.0.1/Health_Insurance_Marketplace.dbs") \
        .save()

def kafka_BenefitCost_4():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partFour.txt")
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
       producer.send('benefitCostSharing_4', data.encode('utf-8'))
    producer.flush()
   
def spark_BenefitCost_4():
   os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
   spark = SparkSession.builder.getOrCreate()
   raw_kafka_df = spark.readStream \
                       .format("kafka") \
                       .option("kafka.bootstrap.servers", "localhost:9092") \
                       .option("subscribe", 'benefitCostSharing_4') \
                       .option("startingOffsets", "earliest") \
                       .load()
   kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
   output_query = kafka_value_df.writeStream \
                         .queryName("benefit_cost_sharing_4") \
                         .format("memory") \
                         .start()
   output_query.awaitTermination(10)
   value_df = spark.sql("select * from benefit_cost_sharing_4")
   value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
   value_row_rdd = value_rdd.map(lambda i: Row(BenefitName=i[0], \
                                               BusinessYear=i[1], \
                                               EHBVarReason=i[2], \
                                               IsCovered=i[3], \
                                               IssuerId=i[4], \
                                               LimitQty=i[5], \
                                               LimitUnit=i[6], \
                                               MinimumStay=i[7], \
                                               PlanId=i[8], \
                                               SourceName=i[9], \
                                               StateCode=i[10]
                                               ))

   df = spark.createDataFrame(value_row_rdd)
   df.show(5)
   df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','Health_Insurance_Marketplace') \
        .option('collection', 'benefit_cost') \
        .option('uri', "mongodb://127.0.0.1/Health_Insurance_Marketplace.dbs") \
        .save()

def kafka_network():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/Network.csv")
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
       producer.send('network', data.encode('utf-8'))
    producer.flush()

def spark_network():
   os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
   spark = SparkSession.builder.getOrCreate()
   raw_kafka_df = spark.readStream \
                       .format("kafka") \
                       .option("kafka.bootstrap.servers", "localhost:9092") \
                       .option("subscribe", 'network') \
                       .option("startingOffsets", "earliest") \
                       .load()
   kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
   output_query = kafka_value_df.writeStream \
                         .queryName("network_data") \
                         .format("memory") \
                         .start()
   output_query.awaitTermination(10)
   value_df = spark.sql("select * from network_data")
   value_rdd = value_df.rdd.map(lambda i: i['value'].split(","))
   value_row_rdd = value_rdd.map(lambda i: Row(BusinessYear=i[0], StateCode=i[1], IssuerId=i[2], SourceName=i[3],  VersionNum=i[4], ImportDate=i[5],IssuerId2=i[6], StateCode2=i[7], NetworkName=i[8], NetworkId=i[9], NetworkURL=i[10], RowNumber=i[11], MarketCoverage=i[12], DentalOnlyPlan=i[13]))

   df = spark.createDataFrame(value_row_rdd)
   df.show(5)
   df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','Health_Insurance_Marketplace') \
        .option('collection', 'network') \
        .option('uri', "mongodb://127.0.0.1/Health_Insurance_Marketplace.dbs") \
        .save()

def kafka_service_area():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/ServiceArea.csv")
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
       producer.send('service_area', data.encode('utf-8'))
    producer.flush()

def spark_service_area():
   os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
   spark = SparkSession.builder.getOrCreate()
   raw_kafka_df = spark.readStream \
                       .format("kafka") \
                       .option("kafka.bootstrap.servers", "localhost:9092") \
                       .option("subscribe", 'service_area') \
                       .option("startingOffsets", "earliest") \
                       .load()
   kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
   output_query = kafka_value_df.writeStream \
                         .queryName("q1_service_area") \
                         .format("memory") \
                         .start()
   output_query.awaitTermination(10)
   value_df_s = spark.sql("select * from q1_service_area")
   value_rdd_schema_1 = value_df_s.rdd.map(lambda i: i['value'].split(","))
   value_row_rdd_sch = value_rdd_schema_1.map(lambda i: Row(BusinessYear=i[0], \
                                               StateCode=i[1], \
                                               IssuerId=i[2], \
                                               SourceName=i[3], \
                                               VersionNum=i[4], \
                                               ImportDate=i[5], \
                                               IssuerId2=i[6], \
                                               StateCode2=i[7], \
                                               ServiceAreaId=i[8], \
                                               ServiceAreaName=i[9], \
                                               CoverEntireState=i[10], \
                                               County=i[11], \
                                               PartialCounty=i[12], \
                                               ZipCodes=i[13], \
                                               PartialCountyJustification=i[14], \
                                               RowNumber=i[15], \
                                               MarketCoverage=i[16], \
                                               DentalOnlyPlan=i[17]
                                               ))

   df = spark.createDataFrame(value_row_rdd_sch)
   df.show(5)
   df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','Health_Insurance_Marketplace') \
        .option('collection', 'service_area') \
        .option('uri', "mongodb://127.0.0.1/Health_Insurance_Marketplace.dbs") \
        .save()

def kafka_insurance():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/insurance.txt")
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
       producer.send('t_insurance', data.encode('utf-8'))
    producer.flush()

def spark_insurance():
   os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
   spark = SparkSession.builder.getOrCreate()
   raw_kafka_df = spark.readStream \
                       .format("kafka") \
                       .option("kafka.bootstrap.servers", "localhost:9092") \
                       .option("subscribe", 't_insurance') \
                       .option("startingOffsets", "earliest") \
                       .load()
   kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
   output_query = kafka_value_df.writeStream \
                         .queryName("q_insurance") \
                         .format("memory") \
                         .start()
   output_query.awaitTermination(10)
   value_df = spark.sql("select * from q_insurance")
   value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
   value_row_rdd = value_rdd.map(lambda i: Row(age=i[0], \
                                               sex=i[1], \
                                               bmi=i[2], \
                                               children=i[3], \
                                               smoker=i[4], \
                                               region=i[5], \
                                               charges=i[6]
                                               ))

   df = spark.createDataFrame(value_row_rdd)
   df.show(5)
   df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','Health_Insurance_Marketplace') \
        .option('collection', 'insurance') \
        .option('uri', "mongodb://127.0.0.1/Health_Insurance_Marketplace.dbs") \
        .save()
  
def kafka_planAttributes():
   producer = KafkaProducer(bootstrap_servers='localhost:9092')
   response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/PlanAttributes.csv")
   data_list = [data for data in response.text.splitlines()[1:]]
   for data in data_list:
      producer.send('t_planAttributes', data.encode('utf-8'))
   producer.flush()

def spark_planAttributes():
   os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
   spark = SparkSession.builder.getOrCreate()
   raw_kafka_df = spark.readStream \
                       .format("kafka") \
                       .option("kafka.bootstrap.servers", "localhost:9092") \
                       .option("subscribe", 't_planAttributes') \
                       .option("startingOffsets", "earliest") \
                       .load()
   kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
   output_query = kafka_value_df.writeStream \
                         .queryName("q_planAttributes") \
                         .format("memory") \
                         .start()
   output_query.awaitTermination(10)
   value_df = spark.sql("select * from q_planAttributes")
   value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
   value_row_rdd = value_rdd.map(lambda i: Row(AttributesID=i[0], \
                                                BeginPrimaryCareCostSharingAfterNumberOfVisits=i[1], \
                                                BeginPrimaryCareDeductibleCoinsuranceAfterNumberOfCopays=i[2], \
                                                BenefitPackageId=i[3], \
                                                BusinessYear=i[4], \
                                                ChildOnlyOffering=i[5], \
                                                CompositeRatingOffered=i[6], \
                                                CSRVariationType=i[7], \
                                                DentalOnlyPlan=i[8], \
                                                DiseaseManagementProgramsOffered=i[9], \
                                                FirstTierUtilization=i[10], \
                                                HSAOrHRAEmployerContribution=i[11], \
                                                HSAOrHRAEmployerContributionAmount=i[12], \
                                                InpatientCopaymentMaximumDays=i[13], \
                                                IsGuaranteedRate=i[14], \
                                                IsHSAEligible=i[15], \
                                                IsNewPlan=i[16], \
                                                IsNoticeRequiredForPregnancy=i[17], \
                                                IsReferralRequiredForSpecialist=i[18], \
                                                IssuerId=i[19], \
                                                MarketCoverage=i[20], \
                                                MedicalDrugDeductiblesIntegrated=i[21], \
                                                MedicalDrugMaximumOutofPocketIntegrated=i[22], \
                                                MetalLevel=i[23], \
                                                MultipleInNetworkTiers=i[24], \
                                                NationalNetwork=i[25], \
                                                NetworkId=i[26], \
                                                OutOfCountryCoverage=i[27], \
                                                OutOfServiceAreaCoverage=i[28], \
                                                PlanEffectiveDate=i[29], \
                                                PlanExpirationDate=i[30], \
                                                PlanId=i[31], \
                                                PlanLevelExclusions=i[32], \
                                                PlanMarketingName=i[33], \
                                                PlanType=i[34], \
                                                QHPNonQHPTypeId=i[35], \
                                                SecondTierUtilization=i[36], \
                                                ServiceAreaId=i[37], \
                                                sourcename=i[38], \
                                                SpecialtyDrugMaximumCoinsurance=i[39], \
                                                StandardComponentId=i[40], \
                                                StateCode=i[41], \
                                                WellnessProgramOffered=i[42]
                                               ))

   df = spark.createDataFrame(value_row_rdd)
   df.show(5)
   df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','Health_Insurance_Marketplace') \
        .option('collection', 'planAttribute') \
        .option('uri', "mongodb://127.0.0.1/Health_Insurance_Marketplace.dbs") \
        .save()

def cdw_branch():
    branch_df = spark.read.format("jdbc").options(
        url      = "jdbc:mysql://localhost:3306/cdw_sapp",
        driver= "com.mysql.cj.jdbc.Driver",
        dbtable = "cdw_sapp_branch",
        user="root",
        password="root").load()
    return branch_df

def cdw_branch_t():
    branch_df = cdw_branch()
    branch_df.createOrReplaceTempView('cdw_branch_temp')
    branch_df_t = spark.sql('SELECT BRANCH_CODE, \
                      BRANCH_NAME, \
                      BRANCH_STREET, \
                      BRANCH_CITY, \
                      BRANCH_STATE, \
                      BRANCH_ZIP, \
                      concat(substring(BRANCH_PHONE,1,3), \'-\', substring(BRANCH_PHONE,4,3), \'-\', substring(BRANCH_PHONE,7,4)) AS BRANCH_PHONE FROM cdw_branch_temp')
    return branch_df_t
   
def branch_mongo():
    uri="mongodb://127.0.0.1/credit_card_system.dbs" 
    spark_mongodb = SparkSession \
        .builder \
        .config("spark.mongodb.input.uri", uri) \
        .config("spark.mongodb.output.uri", uri) \
        .getOrCreate()

    df_branch_w = cdw_branch_t()
    print(type(df_branch_w))
    print(df_branch_w)
    df_branch_w.write.format("com.mongodb.spark.sql.DefaultSource") \
                .mode("overwrite") \
                .option('collection','cdw_sapp_branch') \
                .save()

def cdw_sapp_creditcard():
    creditcard_df = spark.read.format("jdbc").options(
        url      = "jdbc:mysql://localhost:3306/cdw_sapp",
        driver= "com.mysql.cj.jdbc.Driver",
        dbtable = "cdw_sapp_creditcard",
        user="root",
        password="root").load()
    return creditcard_df

def cdw_sapp_creditcard_t():
    creditcard_df = cdw_sapp_creditcard()
    creditcard_df.createOrReplaceTempView('creditcard_temp')
    creditcard_df_t = spark.sql('SELECT CREDIT_CARD_NO, \
                concat(YEAR,MONTH,DAY) as TIMEID, \
                CUST_SSN, \
                BRANCH_CODE, \
                TRANSACTION_TYPE, \
                TRANSACTION_VALUE, \
                TRANSACTION_ID \
                FROM creditcard_temp')
    return creditcard_df_t

def creditcard_mongo():
    uri="mongodb://127.0.0.1/credit_card_system.dbs" 
    spark_mongodb = SparkSession \
        .builder \
        .config("spark.mongodb.input.uri", uri) \
        .config("spark.mongodb.output.uri", uri) \
        .getOrCreate()

    df_creditcard_w = cdw_sapp_creditcard_t()
    df_creditcard_w.write.format("com.mongodb.spark.sql.DefaultSource") \
                .mode("overwrite") \
                .option('collection','cdw_sapp_creditcard') \
                .save()

def cdw_sapp_customer():
    customer_df = spark.read.format("jdbc").options(
        url      = "jdbc:mysql://localhost:3306/cdw_sapp",
        driver = "com.mysql.cj.jdbc.Driver",
        dbtable = "cdw_sapp_customer",
        user = "root",
        password = "root").load()
    return customer_df

def cdw_sapp_customer_t():
    customer_df = cdw_sapp_customer()
    customer_df.createOrReplaceTempView('cdw_sapp_customer_temp')
    customer_df_t = spark.sql('SELECT initcap(FIRST_NAME), \
                      initcap(MIDDLE_NAME), \
                      initcap(LAST_NAME), \
                      SSN, \
                      CREDIT_CARD_NO, \
                      concat(APT_NO, \' \', STREET_NAME) AS CUST_STREET, \
                      CUST_CITY, \
                      CUST_STATE, \
                      CUST_COUNTRY, \
                      CUST_ZIP, \
                      CUST_PHONE, \
                      CUST_EMAIL FROM cdw_sapp_customer_temp')
    return customer_df_t

def customer_mongo():
    uri="mongodb://127.0.0.1/credit_card_system.dbs" 
    spark_mongodb = SparkSession \
        .builder \
        .config("spark.mongodb.input.uri", uri) \
        .config("spark.mongodb.output.uri", uri) \
        .getOrCreate()

    df_customer_w = cdw_sapp_customer_t()
    df_customer_w.write.format("com.mongodb.spark.sql.DefaultSource") \
                .mode("overwrite") \
                .option('collection','cdw_sapp_customer') \
                .save()

def main(): 
    print('Welcome!')
    entry = None
    option = None
    while option != '3': 
        print("1- Credit Card data from MariaDB \n2- Health Insurance Marketplace \n3- Quit")
        option = input("Enter your option: ")
        if option == '1':
            print("CreditCard transfer started")
            branch_mongo()
            creditcard_mongo()
            customer_mongo()
            print("CreditCard transfer complete")
        elif option == '2':       
            while entry != '6':
                print("Select from the following options:\n1. Benefit Cost, \n2. Network, \n3. Service Area,\n4. Insurance, \n5. Plan Attribute, \n6. Quit \n")
                option = input("Enter your option here: ")
                if option == '1':
                    kafka_BenefitCost_1()
                    spark_BenefitCost_1()

                    kafka_BenefitCost_2()
                    spark_BenefitCost_2()

                    kafka_BenefitCost_3()
                    spark_BenefitCost_3()

                    kafka_BenefitCost_4()
                    spark_BenefitCost_4()
                
                elif option == '2':
                    kafka_network()
                    spark_network()

                elif option == '3':
                    kafka_service_area()
                    spark_service_area()
                
                elif option == '4':
                    kafka_insurance()
                    spark_insurance()

                elif option == '5':
                    kafka_planAttributes()
                    spark_planAttributes()

                elif option == '6':
                    break
        elif option == '3':
            break

if __name__ == '__main__':
    main()