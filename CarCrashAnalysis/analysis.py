import os.path
import logging
from CarCrashAnalysis import utils
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

class Analysis:
    def __init__(self, spark, config_path):
        self.spark = spark
        self.df_dict = {}
        self.config = utils.read_json(config_path)
        self.data_path = self.config['data']['path']

    def start(self):
        self._load_data()
        self.execute_analytics()

    def _load_data(self):
        for df_name, filename in self.config['data']['files'].items():
            path = os.path.join(self.data_path, filename)
            self.df_dict[df_name] = self._read_csv_file(path)

    def _read_csv_file(self, path):
        return self.spark.read.csv(path, header="true")

    def execute_analytics(self):
        self.accidents_by_gender()
        self.two_wheelers_booked_for_crashes()
        self.state_with_highest_accidents()
        self.top_vehicle_ids_contributing_to_injuries()
        self.top_ethnic_user_group_by_body_style()
        self.zip_codes_with_high_alcohol_contributions()
        self.no_damaged_property_with_high_damage_level_and_insurance()
        self.top_vehicle_makes_for_speeding_offenses()

    def accidents_by_gender(self):
        gender = self.config['analytics']['accidents_by_gender']['gender']
        count_of_accidents = self.df_dict['driver'].filter(F.col('PRSN_GNDR_ID') == gender).count()
        logger.info(f"Number of crashes involving {gender} drivers: {count_of_accidents}")

    def two_wheelers_booked_for_crashes(self):
        vehicle_types = ['MOTORCYCLE', 'POLICE MOTORCYCLE']  # Modify as per your dataset.
        count_of_vehicles = self.df_dict['vehicle'].filter(F.col('UNIT_DESC_ID').isin(vehicle_types)).count()
        logger.info(f"Number of two-wheelers involved in accidents: {count_of_vehicles}")

    def state_with_highest_accidents(self):
        gender = self.config['analytics']['state_with_highest_accidents']['gender']
        state_df = self.df_dict['driver'].filter(F.col('PRSN_GNDR_ID') == gender).groupBy('DRVR_LIC_STATE_ID').count()
        top_state = state_df.orderBy(F.col('count').desc()).limit(1).collect()[0][0]
        logger.info(f"State with the highest number of accidents involving {gender} drivers: {top_state}")

    def top_vehicle_ids_contributing_to_injuries(self):
        top_n_vals = self.config['analytics']['top_vehicle_ids_contributing_to_injuries']['top_n_vals']
        injury_df = self.df_dict['vehicle'].groupBy('VEH_MAKE_ID').agg(F.sum('TOT_INJRY_CNT').alias('total_injuries'))
        top_vehicle_ids = injury_df.orderBy(F.col('total_injuries').desc()).limit(top_n_vals)
        for row in top_vehicle_ids.collect():
            logger.info(f"Vehicle ID {row['VEH_MAKE_ID']} caused {row['total_injuries']} injuries.")

    def top_ethnic_user_group_by_body_style(self):
        df = self.df_dict['vehicle'].join(self.df_dict['driver'], 'crash_id', 'inner')
        result_df = df.groupBy('VEH_BODY_STYL_ID').agg(F.expr("percentile_approx(PRSN_ETHNICITY_ID, 0.5)").alias('top_ethnic_group'))
        for row in result_df.collect():
            logger.info(f"For body style {row['VEH_BODY_STYL_ID']}, top ethnic user group is {row['top_ethnic_group']}.")

    def zip_codes_with_high_alcohol_contributions(self):
        top_n_zip_codes = self.config['analytics']['zip_codes_with_high_alcohol_contributions']['top_n_zip_codes']
        alcohol_result = self.config['analytics']['zip_codes_with_high_alcohol_contributions']['alcohol_result']
        filtered_df = self.df_dict['driver'].filter(F.col('PRSN_ALC_RSLT_ID') == alcohol_result)
        zip_code_df = filtered_df.groupBy('DRVR_ZIP').count().orderBy(F.col('count').desc()).limit(top_n_zip_codes)
        for row in zip_code_df.collect():
            logger.info(f"Zip Code {row['DRVR_ZIP']} has {row['count']} crashes due to alcohol.")

    def no_damaged_property_with_high_damage_level_and_insurance(self):
        damage_df = self.df_dict['damages']
        veh_df = self.df_dict['vehicle']
        df_no_dmg = damage_df.where(F.col("DAMAGED_PROPERTY").like("%NO DAMAGE%") | F.col("DAMAGED_PROPERTY").isNull())
        df = veh_df.select("CRASH_ID", "FIN_RESP_TYPE_ID", "VEH_DMAG_SCL_1_ID") \
                .where(F.col("FIN_RESP_TYPE_ID").like(" % INSURANCE % ")) \
                .withColumn("damage_rate", F.regexp_extract("VEH_DMAG_SCL_1_ID", "\\d+", 0)) \
                .where(F.col("damage_rate").cast("int") >= 4)

        joined_df = df_no_dmg.join(df, on='CRASH_ID', how='inner')
        unique_crash_count = joined_df.select("CRASH_ID").distinct().count()
        
        logger.info(f"Number of unique crashes with no property damage, damage level >= 4, and insurance: {unique_crash_count}")



    def top_vehicle_makes_for_speeding_offenses(self):
        condition = (F.col('CHARGE').like('%SPEED%')) & (F.col('DRVR_LIC_TYPE_ID').like('%DRIVER LIC%'))
        df = self.df_dict['vehicle'].join(self.df_dict['driver'], 'CRASH_ID', 'inner') \
            .join(self.df_dict['charges'], 'CRASH_ID', 'inner') \
            .filter(condition)
        top_makes_df = df.groupBy('VEH_MAKE_ID').count() \
            .orderBy(F.col('count').desc()) \
            .limit(self.config['analytics']['top_vehicle_makes_for_speeding_offenses']['top_n_makes'])
        for row in top_makes_df.collect():
            logger.info(f"Vehicle make {row['VEH_MAKE_ID']} has {row['count']} speeding offenses.")
