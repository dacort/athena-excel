from typing import List
import io
import os

from athena.federation.athena_data_source import AthenaDataSource
import boto3
import openpyxl
import pandas as pd
import pyarrow as pa

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX").strip(
    "/"
)  # Ensure that the prefix does *not* have a slash at the end or beginning :D


class ExcelDataSource(AthenaDataSource):
    """
    An Athena Data Source for Excel docs
    """

    def __init__(self):
        super().__init__()
        self._client = boto3.client("s3")

    def databases(self) -> List[str]:
        return self._list_excel_files_without_extension()

    def tables(self, database_name: str) -> List[str]:
        return self._get_sheet_names(database_name)

    def columns(self, database_name: str, table_name: str) -> List[str]:
        return self._get_column_names(database_name, table_name)

    def schema(self, database_name: str, table_name: str) -> pa.Schema:
        return super().schema(database_name, table_name)

    def records(
        self, database_name: str, table_name: str, split: Mapping[str, str]
    ) -> Dict[str, List[Any]]:
        """
        Retrieve records from an Excel sheet
        """
        # We unfortunately need to transpose the data - we should add a helper for this
        df = pd.read_excel(
            self._get_filelike_object(database_name), sheet_name=table_name
        )
        return dict(zip(self.columns(database_name, table_name), list(df.to_records())))

    def _list_excel_files_without_extension(self):
        excel_db_names = []
        for object_data in self._client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=S3_PREFIX
        ).get("Contents"):
            excel_db_names.append(
                object_data.get("Key").strip(f"{S3_PREFIX}/").rstrip(".xls")
            )

        return excel_db_names

    def _get_sheet_names(self, database_name: str) -> List[str]:
        workbook = openpyxl.reader.excel.load_workbook(
            self._get_filelike_object(database_name)
        )
        return workbook.sheetnames

    def _get_column_names(self, database_name: str, table_name: str) -> List[str]:
        df = pd.read_excel(
            self._get_filelike_object(database_name), sheet_name=table_name
        )
        return list(df.columns)

    def _get_filelike_object(self, database_name: str):
        resp = self._client.get_object(
            Bucket=S3_BUCKET, Key=f"{S3_PREFIX}/{database_name}.xlsx"
        )
        return io.BytesIO(resp["Body"].read())
