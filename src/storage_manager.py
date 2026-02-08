"""
Storage layer for Yahoo Fantasy Baseball analyzer.

DynamoDB-only implementation using 5 tables:
- LiveData: Current/latest data (overwrite pattern)
- WeeklyTimeSeries: Week-by-week historical data
- MatchupResults: Weekly matchup outcomes
- Schedule: League schedule
- AllTimeHistory: All-time rankings

Usage:
    storage = DynamoStorageManager(region='us-west-2')
    storage.write_live_data('live_standings', df)
"""

from typing import Dict, List, Optional
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal


# DynamoDB metadata columns added during writes - stripped from reads
_METADATA_COLUMNS = {'DataType', 'TeamNumber', 'Week#DataType'}


def _strip_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Remove DynamoDB metadata columns from a DataFrame."""
    cols_to_drop = [c for c in _METADATA_COLUMNS if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
    return df


class DynamoStorageManager:
    """
    DynamoDB storage manager for Yahoo Fantasy Baseball.

    Uses 5 DynamoDB tables for all data storage.
    """

    LIVE_DATA_TYPES = {
        'live_standings', 'playoff_status', 'power_ranks', 'Power_Ranks',
        'normalized_ranks', 'power_ranks_lite', 'team_dict', 'remaining_sos',
        'weekly_luck_analysis', 'Coefficient_Last_Four', 'Coefficient_Last_Two',
        'minimum_innings_check', 'seasons_best_long', 'seasons_best_regular'
    }

    WEEKLY_DATA_TYPES = {
        'running_normalized_ranks', 'power_ranks_season_trend',
        'standings_season_trend', 'weekly_stats', 'coefficient',
        'Running_ELO', 'week_stats', 'weekly_results'
    }

    def __init__(self, region: str = 'us-west-2', table_prefix: str = 'FantasyBaseball'):
        self.region = region
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table_prefix = table_prefix

        self.TABLE_LIVE_DATA = f'{table_prefix}-LiveData'
        self.TABLE_WEEKLY_TIME_SERIES = f'{table_prefix}-WeeklyTimeSeries'
        self.TABLE_MATCHUP_RESULTS = f'{table_prefix}-MatchupResults'
        self.TABLE_SCHEDULE = f'{table_prefix}-Schedule'
        self.TABLE_ALL_TIME = f'{table_prefix}-AllTimeHistory'

    # ========================================================================
    # Type conversion helpers
    # ========================================================================

    def _convert_floats_to_decimal(self, obj):
        """Convert floats to Decimal for DynamoDB compatibility."""
        if isinstance(obj, dict):
            return {k: self._convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_floats_to_decimal(item) for item in obj]
        elif isinstance(obj, float):
            return Decimal(str(obj))
        return obj

    def _convert_decimals_to_float(self, obj):
        """Convert Decimals back to float for pandas compatibility."""
        if isinstance(obj, dict):
            return {k: self._convert_decimals_to_float(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimals_to_float(item) for item in obj]
        elif isinstance(obj, Decimal):
            return float(obj)
        return obj

    def _batch_write_items(self, table_name: str, items: List[Dict]) -> None:
        """Write items in batches of 25 (DynamoDB limit)."""
        table = self.dynamodb.Table(table_name)
        items = [self._convert_floats_to_decimal(item) for item in items]

        for i in range(0, len(items), 25):
            batch = items[i:i + 25]
            with table.batch_writer() as writer:
                for item in batch:
                    writer.put_item(Item=item)

        print(f"Wrote {len(items)} items to {table_name}")

    # ========================================================================
    # LiveData table operations
    # ========================================================================

    def write_live_data(self, data_type: str, df: pd.DataFrame) -> None:
        """Write data that gets fully refreshed (overwrite pattern)."""
        if data_type not in self.LIVE_DATA_TYPES:
            print(f"Warning: {data_type} not in LIVE_DATA_TYPES, writing to LiveData table anyway")

        self._clear_live_data_type(data_type)

        items = []
        for idx, row in df.iterrows():
            item = row.to_dict()
            item['DataType'] = data_type
            if 'Team_Number' in item:
                item['TeamNumber'] = str(item['Team_Number'])
            elif 'TeamNumber' not in item:
                item['TeamNumber'] = str(idx)
            items.append(item)

        if items:
            self._batch_write_items(self.TABLE_LIVE_DATA, items)

    def _clear_live_data_type(self, data_type: str) -> None:
        """Clear all items for a specific DataType in LiveData table."""
        table = self.dynamodb.Table(self.TABLE_LIVE_DATA)
        try:
            response = table.query(
                KeyConditionExpression='DataType = :dt',
                ExpressionAttributeValues={':dt': data_type}
            )
            items_to_delete = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    KeyConditionExpression='DataType = :dt',
                    ExpressionAttributeValues={':dt': data_type},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items_to_delete.extend(response.get('Items', []))

            if items_to_delete:
                with table.batch_writer() as writer:
                    for item in items_to_delete:
                        writer.delete_item(Key={
                            'DataType': item['DataType'],
                            'TeamNumber': item['TeamNumber']
                        })
                print(f"Deleted {len(items_to_delete)} existing items for {data_type}")
        except ClientError as e:
            print(f"Error clearing {data_type}: {e}")

    def get_live_data(self, data_type: str, filters: Optional[Dict] = None) -> pd.DataFrame:
        """Retrieve current live data."""
        table = self.dynamodb.Table(self.TABLE_LIVE_DATA)
        try:
            response = table.query(
                KeyConditionExpression='DataType = :dt',
                ExpressionAttributeValues={':dt': data_type}
            )
            items = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    KeyConditionExpression='DataType = :dt',
                    ExpressionAttributeValues={':dt': data_type},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response.get('Items', []))

            if not items:
                return pd.DataFrame()

            items = [self._convert_decimals_to_float(item) for item in items]
            df = _strip_metadata(pd.DataFrame(items))

            if filters:
                for key, value in filters.items():
                    if key in df.columns:
                        df = df[df[key] == value]
            return df
        except ClientError as e:
            print(f"Error fetching live data: {e}")
            return pd.DataFrame()

    # ========================================================================
    # WeeklyTimeSeries table operations
    # ========================================================================

    def append_weekly_data(self, data_type: str, week: int, df: pd.DataFrame) -> None:
        """Append data for a specific week (clears existing week data first)."""
        if data_type not in self.WEEKLY_DATA_TYPES:
            print(f"Warning: {data_type} not in WEEKLY_DATA_TYPES, writing to WeeklyTimeSeries anyway")

        self._clear_weekly_data(data_type, week)

        items = []
        for idx, row in df.iterrows():
            item = row.to_dict()
            item['Week'] = week
            item['DataType'] = data_type
            if 'Team_Number' in item:
                item['TeamNumber'] = str(item['Team_Number'])
            elif 'TeamNumber' not in item:
                item['TeamNumber'] = str(idx)
            item['Week#DataType'] = f"{week:02d}#{data_type}"
            items.append(item)

        if items:
            self._batch_write_items(self.TABLE_WEEKLY_TIME_SERIES, items)

    def _clear_weekly_data(self, data_type: str, week: int) -> None:
        """Clear data for a specific week and data type."""
        table = self.dynamodb.Table(self.TABLE_WEEKLY_TIME_SERIES)
        try:
            response = table.query(
                IndexName='DataTypeWeekIndex',
                KeyConditionExpression='DataType = :dt AND #w = :week',
                ExpressionAttributeNames={'#w': 'Week'},
                ExpressionAttributeValues={':dt': data_type, ':week': week}
            )
            items_to_delete = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    IndexName='DataTypeWeekIndex',
                    KeyConditionExpression='DataType = :dt AND #w = :week',
                    ExpressionAttributeNames={'#w': 'Week'},
                    ExpressionAttributeValues={':dt': data_type, ':week': week},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items_to_delete.extend(response.get('Items', []))

            if items_to_delete:
                with table.batch_writer() as writer:
                    for item in items_to_delete:
                        writer.delete_item(Key={
                            'TeamNumber': item['TeamNumber'],
                            'Week#DataType': item['Week#DataType']
                        })
                print(f"Deleted {len(items_to_delete)} existing items for {data_type} week {week}")
        except ClientError as e:
            print(f"Error clearing week data: {e}")

    def get_weekly_data(self, data_type: str, week: int) -> pd.DataFrame:
        """Retrieve data for a specific week."""
        table = self.dynamodb.Table(self.TABLE_WEEKLY_TIME_SERIES)
        try:
            response = table.query(
                IndexName='DataTypeWeekIndex',
                KeyConditionExpression='DataType = :dt AND #w = :week',
                ExpressionAttributeNames={'#w': 'Week'},
                ExpressionAttributeValues={':dt': data_type, ':week': week}
            )
            items = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    IndexName='DataTypeWeekIndex',
                    KeyConditionExpression='DataType = :dt AND #w = :week',
                    ExpressionAttributeNames={'#w': 'Week'},
                    ExpressionAttributeValues={':dt': data_type, ':week': week},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response.get('Items', []))

            if not items:
                return pd.DataFrame()

            items = [self._convert_decimals_to_float(item) for item in items]
            return _strip_metadata(pd.DataFrame(items))
        except ClientError as e:
            print(f"Error fetching weekly data: {e}")
            return pd.DataFrame()

    def get_historical_data(self, data_type: str, weeks: Optional[List[int]] = None) -> pd.DataFrame:
        """Retrieve historical data across multiple weeks."""
        if weeks:
            dfs = [self.get_weekly_data(data_type, week) for week in weeks]
            dfs = [df for df in dfs if not df.empty]
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        table = self.dynamodb.Table(self.TABLE_WEEKLY_TIME_SERIES)
        try:
            response = table.scan(
                FilterExpression='DataType = :dt',
                ExpressionAttributeValues={':dt': data_type}
            )
            items = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = table.scan(
                    FilterExpression='DataType = :dt',
                    ExpressionAttributeValues={':dt': data_type},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response.get('Items', []))

            if not items:
                return pd.DataFrame()

            items = [self._convert_decimals_to_float(item) for item in items]
            return _strip_metadata(pd.DataFrame(items))
        except ClientError as e:
            print(f"Error fetching historical data: {e}")
            return pd.DataFrame()

    # ========================================================================
    # Schedule table operations
    # ========================================================================

    def write_schedule_data(self, week: int, df: pd.DataFrame) -> None:
        """Write schedule data for a specific week (clears week first)."""
        self._clear_schedule_week(week)

        items = []
        for idx, row in df.iterrows():
            item = row.to_dict()
            item['Week'] = week
            if 'Team_Number' in item:
                item['TeamNumber'] = str(item['Team_Number'])
            elif 'TeamNumber' not in item:
                item['TeamNumber'] = str(idx)
            items.append(item)

        if items:
            self._batch_write_items(self.TABLE_SCHEDULE, items)

    def get_schedule_data(self, week: int = None) -> pd.DataFrame:
        """Get schedule data, optionally for a specific week."""
        table = self.dynamodb.Table(self.TABLE_SCHEDULE)
        try:
            if week is not None:
                response = table.query(
                    KeyConditionExpression='#w = :week',
                    ExpressionAttributeNames={'#w': 'Week'},
                    ExpressionAttributeValues={':week': week}
                )
                items = response.get('Items', [])
                while 'LastEvaluatedKey' in response:
                    response = table.query(
                        KeyConditionExpression='#w = :week',
                        ExpressionAttributeNames={'#w': 'Week'},
                        ExpressionAttributeValues={':week': week},
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                    items.extend(response.get('Items', []))
            else:
                response = table.scan()
                items = response.get('Items', [])
                while 'LastEvaluatedKey' in response:
                    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                    items.extend(response.get('Items', []))

            if not items:
                return pd.DataFrame()

            items = [self._convert_decimals_to_float(item) for item in items]
            df = pd.DataFrame(items)
            # Strip TeamNumber metadata but keep Week (it's real data for schedule)
            if 'TeamNumber' in df.columns:
                df = df.drop(columns=['TeamNumber'])
            return df
        except ClientError as e:
            print(f"Error fetching schedule data: {e}")
            return pd.DataFrame()

    def clear_schedule(self) -> None:
        """Clear all schedule data."""
        table = self.dynamodb.Table(self.TABLE_SCHEDULE)
        try:
            response = table.scan()
            items = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response.get('Items', []))

            if items:
                with table.batch_writer() as writer:
                    for item in items:
                        writer.delete_item(Key={
                            'Week': item['Week'],
                            'TeamNumber': item['TeamNumber']
                        })
                print(f"Deleted {len(items)} schedule items")
        except ClientError as e:
            print(f"Error clearing schedule: {e}")

    def _clear_schedule_week(self, week: int) -> None:
        """Clear schedule for a specific week."""
        table = self.dynamodb.Table(self.TABLE_SCHEDULE)
        try:
            response = table.query(
                KeyConditionExpression='#w = :week',
                ExpressionAttributeNames={'#w': 'Week'},
                ExpressionAttributeValues={':week': week}
            )
            items = response.get('Items', [])
            if items:
                with table.batch_writer() as writer:
                    for item in items:
                        writer.delete_item(Key={
                            'Week': item['Week'],
                            'TeamNumber': item['TeamNumber']
                        })
                print(f"Deleted {len(items)} schedule items for week {week}")
        except ClientError as e:
            print(f"Error clearing schedule week {week}: {e}")

    # ========================================================================
    # AllTimeHistory table operations
    # ========================================================================

    def write_all_time_data(self, year: int, df: pd.DataFrame) -> None:
        """Write all-time data for a specific year (clears year first)."""
        self.clear_all_time_year(year)

        items = []
        for idx, row in df.iterrows():
            item = row.to_dict()
            item['Year'] = str(year)
            if 'Team_Number' in item:
                item['TeamNumber'] = str(item['Team_Number'])
            elif 'TeamNumber' not in item:
                item['TeamNumber'] = str(idx)
            items.append(item)

        if items:
            self._batch_write_items(self.TABLE_ALL_TIME, items)

    def get_all_time_data(self, year: int = None) -> pd.DataFrame:
        """Get all-time data, optionally filtered by year."""
        table = self.dynamodb.Table(self.TABLE_ALL_TIME)
        try:
            if year is not None:
                response = table.query(
                    IndexName='YearIndex',
                    KeyConditionExpression='#y = :year',
                    ExpressionAttributeNames={'#y': 'Year'},
                    ExpressionAttributeValues={':year': str(year)}
                )
                items = response.get('Items', [])
                while 'LastEvaluatedKey' in response:
                    response = table.query(
                        IndexName='YearIndex',
                        KeyConditionExpression='#y = :year',
                        ExpressionAttributeNames={'#y': 'Year'},
                        ExpressionAttributeValues={':year': str(year)},
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                    items.extend(response.get('Items', []))
            else:
                response = table.scan()
                items = response.get('Items', [])
                while 'LastEvaluatedKey' in response:
                    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                    items.extend(response.get('Items', []))

            if not items:
                return pd.DataFrame()

            items = [self._convert_decimals_to_float(item) for item in items]
            df = pd.DataFrame(items)
            if 'TeamNumber' in df.columns:
                df = df.drop(columns=['TeamNumber'])
            return df
        except ClientError as e:
            print(f"Error fetching all-time data: {e}")
            return pd.DataFrame()

    def clear_all_time_year(self, year: int) -> None:
        """Clear all-time data for a specific year."""
        table = self.dynamodb.Table(self.TABLE_ALL_TIME)
        try:
            response = table.query(
                IndexName='YearIndex',
                KeyConditionExpression='#y = :year',
                ExpressionAttributeNames={'#y': 'Year'},
                ExpressionAttributeValues={':year': str(year)}
            )
            items_to_delete = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    IndexName='YearIndex',
                    KeyConditionExpression='#y = :year',
                    ExpressionAttributeNames={'#y': 'Year'},
                    ExpressionAttributeValues={':year': str(year)},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items_to_delete.extend(response.get('Items', []))

            if items_to_delete:
                with table.batch_writer() as writer:
                    for item in items_to_delete:
                        writer.delete_item(Key={
                            'TeamNumber': item['TeamNumber'],
                            'Year': item['Year']
                        })
                print(f"Deleted {len(items_to_delete)} all-time items for year {year}")
        except ClientError as e:
            print(f"Error clearing all-time year {year}: {e}")

    # ========================================================================
    # Generic helpers
    # ========================================================================

    def clear_collection(self, collection_name: str) -> None:
        """Clear all data from a collection."""
        if collection_name in self.LIVE_DATA_TYPES:
            self._clear_live_data_type(collection_name)
        elif collection_name in self.WEEKLY_DATA_TYPES:
            print(f"Cannot clear all weekly data for {collection_name} without specifying weeks")
        else:
            print(f"Unknown collection type: {collection_name}")

    def get_all_data(self, collection_name: str) -> pd.DataFrame:
        """Get all data from a collection."""
        if collection_name in self.LIVE_DATA_TYPES:
            return self.get_live_data(collection_name)
        elif collection_name in self.WEEKLY_DATA_TYPES:
            return self.get_historical_data(collection_name)
        else:
            print(f"Unknown collection type: {collection_name}")
            return pd.DataFrame()

    def write_team_dict(self, df_standings: pd.DataFrame) -> None:
        """Write team dictionary from standings DataFrame."""
        df_teamIDs = df_standings[['Team', 'Team_Number']].copy()
        df_teamIDs.reset_index(inplace=True)
        self.write_live_data('team_dict', df_teamIDs)
