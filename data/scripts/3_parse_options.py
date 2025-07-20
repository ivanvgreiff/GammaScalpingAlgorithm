#!/usr/bin/env python3
"""
Options Data Parser for Deribit BTC Data

Parses daily trade files and separates data by individual options.
Creates a feather file and JSON metadata for each unique option.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import re
import sys

def parse_instrument_name(instrument):
    """Parse Deribit instrument name into components.
    
    Format: BTC-19JUL19-10000-P
    Returns: dict with asset, expiry_date, strike_price, option_type
    """
    try:
        parts = instrument.split('-')
        if len(parts) != 4:
            return None
        
        asset = parts[0]
        expiry_str = parts[1]
        strike_price = int(parts[2])
        option_type = 'CALL' if parts[3] == 'C' else 'PUT'
        
        # Parse expiry date (format: 19JUL19)
        expiry_date = datetime.strptime(expiry_str, '%d%b%y').date()
        
        return {
            'asset': asset,
            'expiry_date': expiry_date.isoformat(),
            'strike_price': strike_price,
            'option_type': option_type
        }
    except Exception:
        return None

def generate_metadata(instrument, trades_df, files_count):
    """Generate comprehensive metadata for an option."""
    
    # Parse instrument details
    parsed = parse_instrument_name(instrument)
    if not parsed:
        return None
    
    # Convert timestamps for analysis
    trades_df['datetime'] = pd.to_datetime(trades_df['timestamp'], unit='us', utc=True)
    
    # Calculate statistics
    metadata = {
        'instrument': instrument,
        'asset': parsed['asset'],
        'expiry_date': parsed['expiry_date'],
        'strike_price': parsed['strike_price'],
        'option_type': parsed['option_type'],
        'total_trades': len(trades_df),
        'total_volume': float(trades_df['quantity'].sum()),
        'first_trade': trades_df['datetime'].min().isoformat(),
        'last_trade': trades_df['datetime'].max().isoformat(),
        'price_range': {
            'min': float(trades_df['price'].min()),
            'max': float(trades_df['price'].max())
        },
        'iv_range': {
            'min': float(trades_df['iv'].min()),
            'max': float(trades_df['iv'].max())
        },
        'data_files_processed': files_count
    }
    
    return metadata

def save_option_data(instrument, trades_df, files_count, output_dir):
    """Save trades and metadata for a single option."""
    
    # Create option directory
    option_dir = output_dir / instrument
    option_dir.mkdir(parents=True, exist_ok=True)
    
    # Sort trades chronologically
    trades_df = trades_df.sort_values('timestamp').reset_index(drop=True)
    
    # Save trades as feather
    trades_path = option_dir / 'trades.feather'
    trades_df.to_feather(trades_path)
    
    # Generate and save metadata
    metadata = generate_metadata(instrument, trades_df, files_count)
    if metadata:
        metadata_path = option_dir / 'metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    return len(trades_df)

def process_all_files(input_dir, output_dir):
    """Main processing function to parse all daily files."""
    
    print("=== DERIBIT OPTIONS PARSER ===\n")
    
    # Get all feather files
    files = sorted(list(input_dir.glob('*.feather')))
    if not files:
        print(f"  No feather files found in {input_dir}")
        return
    
    print(f"  Found {len(files)} files to process")
    print(f"  Date range: {files[0].stem} to {files[-1].stem}")
    print(f"  Output directory: {output_dir}")
    
    # Dictionary to accumulate trades by instrument
    instruments_data = defaultdict(list)
    files_per_instrument = defaultdict(int)
    
    # Process each file
    processed_files = 0
    total_trades = 0
    
    for i, file_path in enumerate(files):
        try:
            # Load daily file
            df = pd.read_feather(file_path)
            
            if df.empty:
                continue
            
            # Group trades by instrument
            for instrument in df['instrument'].unique():
                instrument_trades = df[df['instrument'] == instrument].copy()
                instruments_data[instrument].append(instrument_trades)
                files_per_instrument[instrument] += 1
            
            total_trades += len(df)
            processed_files += 1
            
            # Progress update
            if (i + 1) % 50 == 0 or i == len(files) - 1:
                print(f"  Processed {i + 1}/{len(files)} files... ({total_trades:,} trades)")
        
        except Exception as e:
            print(f"   Error processing {file_path.name}: {e}")
            continue
    
    print(f"\nProcessing complete:")
    print(f"  Files processed: {processed_files}")
    print(f"  Total trades: {total_trades:,}")
    print(f"  Unique instruments: {len(instruments_data)}")
    
    # Save data for each instrument
    print(f"\nSaving individual option files...")
    saved_options = 0
    saved_trades = 0
    
    for instrument, trade_dfs in instruments_data.items():
        try:
            # Combine all trades for this instrument
            combined_df = pd.concat(trade_dfs, ignore_index=True)
            
            # Remove duplicates by trade_id
            combined_df = combined_df.drop_duplicates(subset=['id'], keep='first')
            
            # Save option data
            trades_count = save_option_data(
                instrument, 
                combined_df, 
                files_per_instrument[instrument], 
                output_dir
            )
            
            saved_options += 1
            saved_trades += trades_count
            
            # Progress update
            if saved_options % 100 == 0:
                print(f"  Saved {saved_options}/{len(instruments_data)} options...")
        
        except Exception as e:
            print(f"⚠️  Error saving {instrument}: {e}")
            continue
    
    print(f"\nPARSING COMPLETE!")
    print(f"  Options saved: {saved_options}")
    print(f"  Total trades saved: {saved_trades:,}")
    print(f"  Output location: {output_dir}")
    
    return saved_options, saved_trades

def main():
    # Define paths
    input_dir = Path('./data/raw/options/feather/deribit/BTC')
    output_dir = Path('./data/parsed/options/')
    
    # Verify input directory exists
    if not input_dir.exists():
        print(f"Input directory not found: {input_dir}")
        print("Please run the downloader first to create the data files.")
        sys.exit(1)
    
    # Create output directory
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # Process all files
    try:
        saved_options, saved_trades = process_all_files(input_dir, output_dir)
        
        # Final summary
        print(f"\nSUMMARY:")
        print(f"  Individual option directories created: {saved_options}")
        print(f"  Each directory contains: trades.feather + metadata.json")
        print(f"  Ready for per-option analysis!")
        
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()