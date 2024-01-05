import logging
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import arrays_zip, explode, expr, col, date_format, when
import os

# Set the default level for the root logger to INFO.
logging.basicConfig(level=logging.INFO)

from annalect_data_framework.ingestion_pipeline import IngestionPipeline
from annalect_data_framework.pipeline_runner import PipelineRunner

SCRIPT_PATH = os.path.dirname(__file__)

COLS_TO_DROP = ["_airbyte_ab_id", "_airbyte_emitted_at", "optimization_goal", "cost_per_estimated_ad_recallers",
                "cost_per_unique_action_type", "cost_per_thruplay", "unique_outbound_clicks",
                "instant_experience_outbound_clicks", "estimated_ad_recall_rate_upper_bound",
                "cost_per_unique_inline_link_click", "canvas_avg_view_time", "inline_link_click_ctr", "ctr",
                "video_avg_time_watched_actions", "labels", "cost_per_ad_click", "unique_link_clicks_ctr",
                "catalog_segment_value", "cost_per_unique_outbound_click", "auction_competitiveness",
                "catalog_segment_value_omni_purchase_roas", "unique_outbound_clicks_ctr", "estimated_ad_recall_rate",
                "attribution_setting", "cost_per_action_type", "outbound_clicks_ctr", "unique_inline_link_click_ctr",
                "catalog_segment_actions", "cost_per_15_sec_video_view", "estimated_ad_recallers_upper_bound",
                "wish_bid", "cost_per_conversion", "converted_product_quantity",
                "qualifying_question_qualify_answer_rate", "website_purchase_roas",
                "estimated_ad_recall_rate_lower_bound", "instant_experience_clicks_to_open", "age_targeting",
                "action_values", "cpc", "cpm", "cpp", "conversion_rate_ranking", "cost_per_inline_link_click",
                "video_play_retention_graph_actions", "purchase_roas", "estimated_ad_recallers",
                "cost_per_unique_click", "website_ctr", "auction_max_competitor_bid", "converted_product_value",
                "auction_bid", "catalog_segment_value_website_purchase_roas", "cost_per_inline_post_engagement",
                "catalog_segment_value_mobile_purchase_roas", "cost_per_outbound_click", "canvas_avg_view_percent",
                "quality_ranking", "unique_ctr", "instant_experience_clicks_to_start",
                "estimated_ad_recallers_lower_bound", "cost_per_2_sec_continuous_video_view", "location",
                "engagement_rate_ranking", "gender_targeting", "ad_impression_actions", "mobile_app_purchase_roas",
                "_airbyte_additional_properties"]


class FacebookTransformation(IngestionPipeline):
    def __init__(self, name, event, **kwargs):
        super().__init__(name, event, **kwargs)

    def additional_transformations(self, df: DataFrame) -> DataFrame:
        """facebook ads insights requires pivoting to extract action_types"""
        self.log.info(f"calling additional transformations: {self.name}")
        df_orig = df

        # deal with actions
        df_actions = self.unnest(df, "actions")
        df_actions = self.pivot_on_action_type(df_actions, "_airbyte_ab_id", "action_type")
        df_first_join = self.join_pivot_to_df(df_orig, df_actions, "_airbyte_ab_id")

        # deal with unique_actions
        df_unique = self.unnest(df_orig, "unique_actions")
        df_unique = self.pivot_on_action_type(df_unique, "_airbyte_ab_id", "action_type")
        df_unique_renamed = self.rename_cols(df_unique)
        df_second_join = self.join_pivot_to_df(df_first_join, df_unique_renamed, "_airbyte_ab_id")

        # unnest dates and video views
        # final_df = df_second_join.withColumn("date", col("date_start.member0"))
        final_df = self.unnest_date(df_second_join, "date_start")
        final_df = self.unnest_date(final_df, "date_stop")

        # unnest all video_x cols
        # 1. list all col names to unnest
        video_fields = [col for col in final_df.schema.names if col.startswith("video_p") and col[7].isdigit()]
        video_fields.append("video_play_actions")
        # 2. generate a list of unnested dataframes
        df_list = [self.agg_col_value(final_df, field) for field in video_fields]
        # 3. join together all unnested dfs
        mass_join = reduce(lambda x, y: x.join(y, '_airbyte_ab_id', "leftouter"), df_list)
        # 4. drop old col names to avoid conflicts
        final_df = final_df.drop(*video_fields).drop("actions").drop("unique_actions")
        # 5. join back on original df
        final_df = final_df.join(mass_join, "_airbyte_ab_id", "leftouter")


        # drop unnecessary cols
        final_df = self.drop_cols(final_df)
        final_df = self.rename_bad_cols(final_df)

        # fill blanks with null, cast to string for this to work
        df_string = final_df.select([col(c).cast("string") for c in final_df.columns])
        all_cols = final_df.schema.names
        final_df = reduce(lambda df, x: df.withColumn(x, self.cast_nulls(x)), all_cols, df_string)
        return final_df

    @staticmethod
    def cast_nulls(x):
        """"""
        return when(col(x.strip()) == "", None).otherwise(col(x))

    @staticmethod
    def unnest(df: DataFrame, col_name: str) -> DataFrame:
        """Horizontally explode a nested column"""
        return df.withColumn("bc", arrays_zip(f"{col_name}.action_type",
                                              f"{col_name}.value")).select("*", explode(
            "bc").alias("tbc")).select("*", "tbc.action_type", "tbc.value")

    @staticmethod
    def pivot_on_action_type(df: DataFrame, id: str, pivot: str) -> DataFrame:
        """return a pivot table from a dataframe given groupby field and pivot field"""
        return df.groupBy(id).pivot(pivot).agg(expr("coalesce(sum(value), \"\")"))

    @staticmethod
    def join_pivot_to_df(df: DataFrame, pivot: DataFrame, key: str) -> DataFrame:
        """return a single dataframe from a pivot table joined on key, back to original df"""
        return df.join(pivot, key, "leftouter")

    @staticmethod
    def rename_cols(df: DataFrame) -> DataFrame:
        """prefix unique actions cols with 'unique_' """
        old_cols = list(df.schema.names)
        new_cols = map(lambda x: "unique_" + x if x != "_airbyte_ab_id" else "_airbyte_ab_id", old_cols)
        return df.toDF(*new_cols)

    @staticmethod
    def drop_cols(df: DataFrame) -> DataFrame:
        """return a dataframe with unnecessary cols removed"""
        offsite_cols = filter(lambda x: x.startswith("offsite"), df.schema.names)
        to_drop = COLS_TO_DROP + list(offsite_cols)
        return df.drop(*to_drop)

    @staticmethod
    def unnest_date(df: DataFrame, col_name: str) -> DataFrame:
        """date_start arrives nested as two members, we need only the first"""
        return df.select("*", date_format(f"{col_name}.member0", "yyyy-MM-dd").alias(f"{col_name}2")).drop(col_name).withColumnRenamed(f"{col_name}2", col_name)

    # @staticmethod
    # def unnest_date(df: DataFrame, col_name: str) -> DataFrame:
    #     return df.withColumn(f"{col_name}2", col(f"{col_name}.member0")).drop(col_name).withColumnRenamed(f"{col_name}2", col_name)

    @staticmethod
    def extract_value(df: DataFrame, col_name: str) -> DataFrame:
        """all video fields need unnesting"""
        return df.withColumn(f"{col_name}2", col(f"{col_name}.value")).drop(f"{col_name}").withColumnRenamed(
            f"{col_name}2", col_name)

    @staticmethod
    def agg_col_value(df: DataFrame, col_name: str) -> DataFrame:
        """return a pivot with sum of unnested value"""
        exploded = df.select("_airbyte_ab_id", explode(f"{col_name}.value").alias(col_name))
        return exploded.groupBy("_airbyte_ab_id").agg(expr(f"sum({col_name})").alias(col_name))

    @staticmethod
    def rename_bad_cols(df: DataFrame) -> DataFrame:
        """"""
        new_cols = map(lambda x: x.replace(".", "_"), list(df.schema.names))
        return df.toDF(*new_cols)


facebook_ads_insights_params = {
    "root_path": "ann40mmm-aew1-volkswagen-airbyte-ingestion/raw-airbyte/vwg/porsche/",
    "db": "vwg_raw",
    "prefix_base": "facebook/*/ads_insights/*.parquet",
    "table_name": "por_fcb_raw_performance",
    "catalog": "dev",
    "partition_column": "date_start",
}

facebook_ad_accounts_params = {
    "root_path": "ann40mmm-aew1-volkswagen-airbyte-ingestion/raw-airbyte/vwg/porsche/",
    "db": "vwg_raw",
    "prefix_base": "facebook/*/ad_account*/*.parquet",
    "table_name": "por_fcb_raw_adaccount",
    "catalog": "dev",
    "partition_column": "created_time",
}


facebook_ad_sets_params = {
    "root_path": "ann40mmm-aew1-volkswagen-airbyte-ingestion/raw-airbyte/vwg/porsche/",
    "db": "vwg_raw",
    "prefix_base": "facebook/*/ad_sets/*.parquet",
    "table_name": "por_fcb_raw_adset",
    "catalog": "dev",
    "partition_column": "updated_time",
}

facebook_campaign_params = {
    "root_path": "ann40mmm-aew1-volkswagen-airbyte-ingestion/raw-airbyte/vwg/porsche/",
    "db": "vwg_raw",
    "prefix_base": "facebook/*/campaigns/*.parquet",
    "table_name": "por_fcb_raw_campaign",
    "catalog": "dev",
    "partition_column": "updated_time",
}


def main():
    print("start")
    runner = PipelineRunner(
        [
            {
                "pipeline_class": FacebookTransformation,
                "args": facebook_ads_insights_params,
                "name": "Porsche Facebook ads_insights",
            },
            {
                "pipeline_class": IngestionPipeline,
                "args": facebook_ad_accounts_params,
                "name": "Porsche Facebook ad_accounts",
            },
            {
                "pipeline_class": IngestionPipeline,
                "args": facebook_ad_sets_params,
                "name": "Porsche Facebook ad_sets",
            },
            {
                "pipeline_class": IngestionPipeline,
                "args": facebook_campaign_params,
                "name": "Porsche Facebook campaigns",
            }
        ]
    )
    runner.go()
    print("Done")


if __name__ == "__main__":
    main()
