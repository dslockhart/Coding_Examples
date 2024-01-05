"""
Module housing the API Caller class and associated errors and enums
"""
import multiprocessing as mp
import time
from io import StringIO
from datetime import datetime, timedelta
import logging
from typing import Optional, Union, Dict, List, Any
from enum import Enum
from dataclasses import fields
import json
import pandas as pd
from tenacity import retry, wait_exponential
import requests as r
from requests.models import Response
from .schemas.schemas import ZemantaSchema

# pylint: disable=C0103
# pylint: disable=R1732
URL = "https://oneapi.zemanta.com/rest/v1/reports/"


class Params(Enum):
    """The parameters required for the api call"""

    Options = {"showArchived": True}
    Filters = [
        {"field": "Date", "operator": "between", "from": "start", "to": "end"},
        {"field": "Account Id", "operator": "=", "value": [""]},
    ]
    Fields = [
        "Day",
        "Account Id",
        "Account",
        "Campaign",
        "Campaign Id",
        "Ad Group",
        "Ad Group Id",
        "Content Ad",
        "Content Ad Id",
        "URL",
        "Display URL",
        "Brand Name",
        "Account Status",
        "Clicks",
        "CTR",
        "Avg. CPC",
        "Avg. CPM",
        "Yesterday Spend",
        "Media Spend",
        "Data Cost",
        "License Fee",
        "Total Spend",
        "Margin",
        "Visits",
        "Unique Users",
        "New Users",
        "Returning Users",
        "% New Users",
        "Pageviews",
        "Pageviews per Visit",
        "Bounced Visits",
        "Non-Bounced Visits",
        "Bounce Rate",
        "Total Seconds",
        "Time on Site",
        "Avg. Cost per Visit",
        "Avg. Cost per New Visitor",
        "Avg. Cost per Pageview",
        "Avg. Cost per Non-Bounced Visit",
        "Avg. Cost per Minute",
        "Avg. Cost per Unique User",
        "Account Status",
        "Campaign Status",
        "Ad Group Status",
        "Content Ad Status",
        "Media Source Status",
        "Publisher Status",
        "Video Start",
        "Video First Quartile",
        "Video Midpoint",
        "Video Third Quartile",
        "Video Complete",
        "Video Progress 3s",
        "Avg. CPV",
        "Avg. CPCV",
        "Measurable Impressions",
        "Viewable Impressions",
        "Not-Measurable Impressions",
        "Not-Viewable Impressions",
        "% Measurable Impressions",
        "% Viewable Impressions",
        "Impression Distribution (Viewable)",
        "Impression Distribution (Not-Measurable)",
        "Impression Distribution (Not-Viewable)",
        "Avg. VCPM",
        "Impressions",
    ]


class Advertisers(Enum):
    """Mapping of advertser Ids"""

    VWB_Italy = "4862"
    VWB_Spain = "22432"
    AUDI_Italy = "6717"
    AUDI_France = "7215"


class ApiCallerError(Exception):
    """Custom exception for this class"""

    def __init__(self, message="Exception during the api call process"):
        self.message = message
        super().__init__(self.message)


class ApiCaller:
    """Class encapsulating the call to Zemanta API"""

    def __init__(
        self,
        advertiser_id: str,
        access_token: str,
        start: Optional[str],
        end: Optional[str],
    ):
        self.advertiser_id = advertiser_id
        self.logger = logging.getLogger("airflow")
        self.access_token = access_token
        self.start_date = self.format_date(start)
        self.end_date = self.format_date(end)
        self.schema = ZemantaSchema
        self.params = Params

    def __repr__(self):
        return f"Zemanta Api Caller class: {self.advertiser_id}"

    @staticmethod
    def parse_ad_id(advertiser_id: Union[str, List[str]]) -> List[str]:
        """we need to ensure that the config passes a list of ints"""
        assert advertiser_id
        if isinstance(advertiser_id, List):
            return advertiser_id
        return [str(x) for x in advertiser_id.split(",")]

    @staticmethod
    def format_date(date: Union[str, datetime, None]) -> Optional[str]:
        """parse the date from variety of formats"""
        if not date or "1970" in str(date):
            return None
        if isinstance(date, str) and len(date) == 10:
            try:
                date_ob = datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                date_ob = datetime.strptime(date, "%d-%m-%Y")
        elif isinstance(date, str) and len(date) > 10:
            try:  # datetime format
                date_ob = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:  # UTC
                date_ob = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(date, datetime):
            date_ob = date
        else:
            raise NotImplementedError
        return datetime.strftime(date_ob, "%Y-%m-%d")

    def get_columns(self) -> List[str]:
        """iterates over the dataclass object for col names"""
        return [field.name for field in fields(self.schema)]

    @staticmethod
    def get_today_str():
        """return a string with today's date formatted in yyyy-mm-dd"""
        date = datetime.today()
        return datetime.strftime(date, "%Y-%m-%d")

    @staticmethod
    def get_yesterday_str():
        """return a string with yesterday's date formatted in yyyy-mm-dd"""
        return datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")

    @staticmethod
    def get_headers(access_token: str) -> Dict[str, str]:
        """return a headers dict with access token for Zemanta api call"""
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        }

    def build_payload(self, advertiser_id: str) -> Dict[str, Any]:
        """build a json payload using values from params object"""
        print("Building payload")
        payload = {}
        field_list = [{"field": field} for field in self.params.Fields.value]
        payload["fields"] = field_list
        payload["options"] = self.params.Options.value
        payload["filters"] = self.params.Filters.value
        payload["filters"][0]["from"] = self.start_date
        payload["filters"][0]["to"] = self.end_date
        payload["filters"][1]["value"] = advertiser_id

        return payload

    @staticmethod
    @retry(wait=wait_exponential(multiplier=1, min=2, max=10))
    def call_api(
        url: str,
        method: str,
        headers: Dict[str, str],
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[Any, Any]:
        """
        Send GET request
        :param url: url formatted with args
        :param method: "GET"
        :param headers: dict with access token
        :param payload: json formatted dict
        :return: Response in JSON format
        """
        print(f"Calling API: {url}; {method}; {headers}; {payload}")
        # pylint: disable=W3101
        rsp = r.request(
            method=method, url=url, headers=headers, data=json.dumps(payload)
        )
        assert rsp.status_code not in [400, 401]
        return rsp.json()

    def wait_for_report(self, url: str, headers: Dict[str, str]) -> str:
        """a wrapper around a while loop to retrieve report status"""
        download_link = None
        start = time.perf_counter()
        while download_link is None:
            status = self.call_api(url, "GET", headers)
            status_resp = status["data"]["status"]
            if status_resp == "IN_PROGRESS":
                time.sleep(30)
            elif status_resp == "DONE":
                download_link = status["data"]["result"]
                stop = time.perf_counter()
                print(f"Report fetching took {stop-start}s")
            else:
                raise ApiCallerError(f"Status returned: {status_resp}")
        print("Download link received")
        return download_link

    @staticmethod
    def get_dataframe(file: Response) -> pd.DataFrame:
        """Return a pandas dataframe from the downloaded file"""
        print("Building dataframe")
        return pd.read_csv(StringIO(file.content.decode("utf-8")))

    @staticmethod
    def format_df(df: pd.DataFrame) -> pd.DataFrame:
        """clean up column names and cast as strings"""
        print("formatting dataframe")
        df.columns = (
            df.columns.str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace("%", "pct", regex=False)
            .str.replace("(", "_", regex=False)
            .str.replace(")", "_", regex=False)
            .str.replace("-", "_", regex=False)
            .str.replace("+", "_", regex=False)
            .str.replace("/", "_", regex=False)
            .str.replace("(_)+", "_", regex=True)
            .str.replace("#", "_num", regex=True)
            .str.strip("_")
        )
        df = df.astype(str)
        df = df.replace({"nan": None}).replace({"None": None})
        return df

    def go(self, advertiser_id) -> pd.DataFrame:
        """Main execution to be run once per advertiser id"""
        print(
            f"Running API caller go method; advertiser_id:{advertiser_id}; "
            f"start_date: {self.start_date}; "
            f"end: {self.end_date}"
        )
        headers = self.get_headers(self.access_token)
        payload = self.build_payload(advertiser_id)

        # create report job
        report_job = self.call_api(URL, "POST", headers, payload)
        job_id = report_job["data"]["id"]
        print(f"{advertiser_id} Job id {job_id}")

        # get report job status
        status_url = URL + job_id
        download_link = self.wait_for_report(status_url, headers)

        # download
        file = r.request("GET", download_link, timeout=200)
        if file.status_code != 200:
            raise ApiCallerError(f"Could not download file from link: {file.json()}")

        # parse data in dataframe
        df = self.get_dataframe(file)
        print(f"{advertiser_id} Length of df: {len(df)}")
        # format
        df = self.format_df(df)

        return df

    def main(self) -> pd.DataFrame:
        """use multiprocessing to parallelize advertiser ids"""
        print(f"CPU count: {mp.cpu_count()}")
        start = time.perf_counter()
        ids = self.parse_ad_id(self.advertiser_id)
        pool = mp.Pool(2)
        results = pool.map(self.go, ids)
        print("Concatenating dataframe")
        final = pd.concat(list(results), axis=0, ignore_index=False)
        stop = time.perf_counter()
        print(f"Total time: {stop-start}s")
        return final
