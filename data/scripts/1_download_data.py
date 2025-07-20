from data_downloader import Downloader
from datetime import datetime, timedelta

def main():
    start_date = '2019-03-30'
    end_date = '2020-07-19'
    
    # Download all BTC options in one call
    print(f"\n=== Downloading BTC options from {start_date} to {end_date} ===")
    options_downloader = Downloader(
        base_path="./data/raw/options/",
        max_concurrent_requests=10,
        max_retries=3,
        log_level='INFO'
    )
    options_downloader.download(
        exchange='deribit',
        symbol='BTC',
        start_date=start_date,
        end_date=end_date,
        format='feather',
        kind='option'
    )
    
    # Download all BTC futures in one call
    #print(f"\n=== Downloading BTC futures from {start_date} to {end_date} ===")
    #futures_downloader = Downloader(
    #    base_path="./downloads/futures/",
    #    max_concurrent_requests=10,
    #    max_retries=3,
    #    log_level='INFO'
    #)
    #futures_downloader.download(
    #    exchange='deribit',
    #    symbol='BTC',
    #    start_date=start_date,
    #    end_date=end_date,
    #    format='feather',
    #    kind='future'
    #)
    
    print("\n=== Download complete! ===")


if __name__ == '__main__':
    main()