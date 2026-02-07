import csv, os, time
from datetime import datetime
import zipfile
from dotenv import load_dotenv

# Local Modules
from email_utils import *
from storage_manager import DynamoStorageManager

load_dotenv()

storage = DynamoStorageManager(region='us-west-2')

# All known data types to export
ALL_DATA_TYPES = list(storage.LIVE_DATA_TYPES) + list(storage.WEEKLY_DATA_TYPES)

def main():
    exported_files = []

    for data_type in ALL_DATA_TYPES:
        try:
            df = storage.get_all_data(data_type)
            if df is not None and not df.empty:
                csv_file_path = f'{data_type}.csv'
                df.to_csv(csv_file_path, index=False)
                exported_files.append(data_type)
                print(f"Export of {data_type} completed successfully.")
            else:
                print(f"Skipping {data_type} - no data found.")
        except Exception as e:
            print(f"Error exporting {data_type}: {e}")

    # Also export schedule and all-time data
    for extra_type, fetch_fn in [('schedule', storage.get_schedule_data), ('all_time_history', storage.get_all_time_data)]:
        try:
            df = fetch_fn()
            if df is not None and not df.empty:
                csv_file_path = f'{extra_type}.csv'
                df.to_csv(csv_file_path, index=False)
                exported_files.append(extra_type)
                print(f"Export of {extra_type} completed successfully.")
        except Exception as e:
            print(f"Error exporting {extra_type}: {e}")

    def zip_csv_files(file_list, output_path):
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_name in file_list:
                file_path = file_name + '.csv'
                if os.path.isfile(file_path):
                    zipf.write(file_path, os.path.basename(file_path))
                    os.remove(file_path)

        print(f'Successfully zipped {len(file_list)} CSV files to {output_path}.')

    output_zip = 'Summertime_Sadness.zip'
    zip_csv_files(exported_files, output_zip)

    send_csvs(output_zip)
    time.sleep(5)
    os.remove('Summertime_Sadness.zip')

if __name__ == '__main__':
    main()
