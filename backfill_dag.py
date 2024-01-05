from datetime import datetime, timedelta
from enum import Enum
from io import StringIO, BytesIO
import requests
import logging
import pandas as pd
from airflow.hooks.S3_hook import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.models.param import Param

logger = logging.getLogger(__name__)
today_str = datetime.today().strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')
yesterday_str = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


class Advertisers(Enum):
    vwb = ['9mz2lqk', 'm3v1m1d', '0xo85cp', 'qp97a1p', 'a7gdydh', 'qgsw0rg', 'wluqpxd', 'xttinzm', '6548z1o', '3bd2hxn']
    audi = ["fyhx3ac", "pocrc8a", "z6gc5jq", "9mz2lqk", "s5v90p3", "rv68qz9", "blhi8ak", "xwjhpjp", "6548z1o",
            "3rbs0ud"]


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True
}


class TtdReportSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, headers, i, advertisers, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = headers
        self.i = i
        self.advertisers = advertisers

    def poke(self, context) -> bool:
        ti = context['ti']
        execution_id = ti.xcom_pull(task_ids=f'ttd_backfill_loop.inner_task_{self.i}.report_triggering_{self.i}',
                                    key='return_value')
        assert execution_id is not None
        logger.info(f'execution_id: {execution_id}')
        payload = {
            "PageSize": 1,
            "PageStartIndex": 0,
            "ReportTemplateIds": [1352769],
            "ReportScheduleIds": [execution_id],
            "AdvertiserIds": self.advertisers
        }
        response = requests.request('post',
                                    "https://api.thetradedesk.com/v3/myreports/reportexecution/query/advertisers",
                                    json=payload, headers=self.headers)
        logger.info(f'payload: {payload}')
        logger.info(response)
        logger.info(response.json())
        if response.json()['Result'][0]['ReportExecutionState'] == 'Complete':
            dl_url = response.json()['Result'][0]['ReportDeliveries'][0]['DownloadURL']
            ti.xcom_push(key='download_link', value=dl_url)
            return True
        return False


def get_payload(start, end, advertisers):
    return {
        'ReportScheduleName': f'{start}_{end}_backfill',
        'RequestedByUserName': 'backfill',
        'CreationDateUtc': today_str,
        'ReportTemplateId': 1352769,
        'ReportScheduleStatus': 'Active',
        'ReportFileFormat': 'CSV',
        'AdvertiserFilters': advertisers,
        'ReportDateRange': 'Custom',
        'TimeZone': 'UTC',
        'ReportDateFormat': 'International',
        'ReportNumericFormat': 'International',
        'IncludeHeaders': True,
        'ReportFrequency': 'Once',
        'ScheduleStartDate': yesterday_str,
        'ReportStartDateInclusive': start,
        'ReportEndDateExclusive': end,
    }


def generate_report(payload):
    url = 'https://api.thetradedesk.com/v3/myreports/reportschedule'
    headers = {'TTD-Auth': '',
               'Content-Type': 'application/json'}
    return requests.request('post', url, json=payload, headers=headers)


def fmt_ymd(date):
    return date.strftime('%Y-%m-%dT00:00:00.000000+00:00')


def generate_date_chunks(start_date, end_date, chunk_in_n_days):
    chunks = []
    if end_date is None:
        end_date = start_date
    current_start = datetime.strptime(start_date, '%Y-%m-%d')
    current_end = datetime.strptime(end_date, '%Y-%m-%d')
    while current_start <= current_end:
        next_end = current_start + timedelta(days=chunk_in_n_days)
        chunks.append((fmt_ymd(current_start), fmt_ymd(min(next_end, current_end))))
        current_start = next_end
    logger.info(chunks)
    return chunks


def trigger(chunk, advertisers):
    logger.info(chunk)
    logger.info(advertisers)
    payload = get_payload(chunk[0], chunk[1], advertisers)
    response = generate_report(payload)
    logger.info(response)
    report_schedule_id = response.json()['ReportScheduleId']
    logger.info(report_schedule_id)
    return report_schedule_id


def download_file(i, **context):
    download_link = context['ti'].xcom_pull(task_ids=f'ttd_backfill_loop.inner_task_{i}.check_report_status_{i}',
                                            key='download_link')
    assert download_link is not None
    response = requests.request("GET", download_link, timeout=200)
    return pd.read_csv(StringIO(response.content.decode("utf-8")))


def write_to_s3(i, brand, **context):
    temp_df = context['ti'].xcom_pull(task_ids=f'ttd_backfill_loop.inner_task_{i}.download_file_{i}',
                                      key='return_value')
    report_no = context['ti'].xcom_pull(task_ids=f'ttd_backfill_loop.inner_task_{i}.check_report_status_{i}',
                                        key='download_link').split("/")[-2]
    buffer = BytesIO()
    temp_df.astype(str).to_parquet(buffer, index=False)
    buffer.seek(0)
    tdy = datetime.strftime(datetime.today().date(), '%Y-%m-%d')
    s3_bucket = "ann40mmm-aew1-volkswagen-airbyte-ingestion"
    s3_prefix = f"raw-airbyte/vwg/{brand}/ttd"
    name = f'{s3_prefix}/{tdy}_report_{report_no}_{i}.parquet'
    s3_path = name
    s3_hook = S3Hook()
    s3_hook.load_bytes(
        bytes_data=buffer.getvalue(),
        bucket_name=s3_bucket,
        key=s3_path,
        replace=True,
    )


def backfill_loop(**context):
    with TaskGroup('ttd_backfill_loop') as t0:
        chunks = generate_date_chunks("2020-01-01", "2023-12-31", 2)
        tasks = []
        brand = "audi"
        advertisers = Advertisers[brand].value
        for i, chunk in enumerate(chunks):
            with TaskGroup(f'inner_task_{i}') as t_in:
                t2 = PythonOperator(
                    task_id=f"report_triggering_{i}",
                    python_callable=trigger,
                    op_kwargs={"chunk": chunk,
                               "advertisers": advertisers}
                )
                t3 = TtdReportSensor(
                    task_id=f"check_report_status_{i}",
                    headers={'TTD-Auth': '',
                             'Content-Type': 'application/json'},
                    i=i,
                    advertisers=advertisers,
                    poke_interval=300,
                    mode="reschedule"
                )
                t4 = PythonOperator(
                    task_id=f"download_file_{i}",
                    python_callable=download_file,
                    op_kwargs={"i": i}
                )
                t5 = PythonOperator(
                    task_id=f'write_to_s3_{i}',
                    python_callable=write_to_s3,
                    op_kwargs={"i": i,
                               "brand": brand}
                )
                t2 >> t3 >> t4 >> t5
                tasks.append([t_in])
        chain(*tasks)
    return t0


with DAG('ttd-backfill-simple', schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         params={"start_date": Param(
             "2020-01-01",
             type="string",
             format="date",
             title="Start Date",
             description="Please select a date, use the button on the left for a pop-up calendar. ",
         ),
             "end_date": Param(
                 "2020-02-01",
                 type="string",
                 format="date",
                 title="End Date",
                 description="Please select a date, use the button on the left for a pop-up calendar. ",
             ),
             "chunk_in_n_days": Param(
                 7,
                 type="integer",
                 title="chunk in n days",
                 description="Enter the interval of days to chunk in",
             ),

             "brand": Param(
                 "vwb",
                 title="Brand",
                 description="Choose the brand to backfill",
                 enum=["vwb", "audi"],
             )
         }) as dag:
    params = {"start_date": "2020-01-01",
              "end_date": "2021-01-01",
              "chunk_in_n_days": 7,
              "brand": "vwb"
              }
    start = EmptyOperator(task_id="start")
    t1 = PythonOperator(
        task_id="generate_chunks",
        python_callable=generate_date_chunks,
        op_kwargs=params
    )
    end = EmptyOperator(task_id="end")
    start >> t1 >> backfill_loop() >> end
