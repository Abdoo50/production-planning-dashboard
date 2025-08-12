import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import warnings

warnings.filterwarnings("ignore")

class SITDateClassifier:
    """
    A class to handle SIT (Shipment In Transit) date classification and filtering.
    Supports filtering by month and extracting date information from various formats.
    """
    
    def __init__(self):
        self.month_mapping = {
            'january': 1, 'jan': 1,
            'february': 2, 'feb': 2,
            'march': 3, 'mar': 3,
            'april': 4, 'apr': 4,
            'may': 5,
            'june': 6, 'jun': 6,
            'july': 7, 'jul': 7,
            'august': 8, 'aug': 8,
            'september': 9, 'sep': 9, 'sept': 9,
            'october': 10, 'oct': 10,
            'november': 11, 'nov': 11,
            'december': 12, 'dec': 12
        }
    
    def extract_date_from_filename(self, filename):
        """
        Extract date information from filename.
        Supports formats like: SIT_Week1_July_2025, SIT_B1_ETA, etc.
        """
        filename_lower = filename.lower()
        
        # Pattern for month and year extraction
        month_year_pattern = r'(\w+)_(\d{4})'
        match = re.search(month_year_pattern, filename_lower)
        
        if match:
            month_str = match.group(1)
            year = int(match.group(2))
            
            # Check if month string contains a known month
            for month_name, month_num in self.month_mapping.items():
                if month_name in month_str:
                    return {'month': month_num, 'year': year, 'month_name': month_name.title()}
        
        # Pattern for week extraction
        week_pattern = r'week(\d+)'
        week_match = re.search(week_pattern, filename_lower)
        week_num = int(week_match.group(1)) if week_match else None
        
        return {'month': None, 'year': None, 'month_name': None, 'week': week_num}
    
    def classify_sit_date_columns(self, df):
        """
        Classify and standardize date columns in SIT data.
        Common columns: 'Updated ETA port', 'ETA port', 'ETA  port'
        """
        date_columns = []
        
        # Common date column patterns
        date_patterns = [
            'eta', 'updated eta', 'eta port', 'updated eta port', 
            'arrival', 'delivery', 'shipment date', 'ship date'
        ]
        
        for col in df.columns:
            col_lower = col.lower().strip()
            if any(pattern in col_lower for pattern in date_patterns):
                date_columns.append(col)
        
        return date_columns
    
    def parse_date_column(self, series, date_format=None):
        """
        Parse a date column with multiple possible formats.
        """
        parsed_dates = []
        
        for value in series:
            if pd.isna(value):
                parsed_dates.append(None)
                continue
            
            # If it's already a datetime
            if isinstance(value, datetime):
                parsed_dates.append(value)
                continue
            
            # Convert to string and try parsing
            value_str = str(value).strip()
            
            # Common date formats to try
            date_formats = [
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%d-%m-%Y',
                '%Y/%m/%d',
                '%d.%m.%Y',
                '%Y.%m.%d'
            ]
            
            parsed_date = None
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(value_str, fmt)
                    break
                except ValueError:
                    continue
            
            # Try pandas to_datetime as fallback
            if parsed_date is None:
                try:
                    parsed_date = pd.to_datetime(value_str, errors='coerce')
                    if pd.isna(parsed_date):
                        parsed_date = None
                except:
                    parsed_date = None
            
            parsed_dates.append(parsed_date)
        
        return pd.Series(parsed_dates)
    
    def filter_by_month(self, df, target_month, target_year=2025):
        """
        Filter DataFrame by target month and year.
        target_month can be month number (1-12), month name, or "All" for no filtering.
        target_year can be a year number or "All" for no filtering.
        """
        # Handle "All" cases - return original dataframe without filtering
        if target_month == "All" and target_year == "All":
            return df
        
        # If only month is "All", filter by year only
        if target_month == "All" and target_year != "All":
            date_columns = self.classify_sit_date_columns(df)
            if not date_columns:
                print("Warning: No date columns found for year filtering")
                return df
            
            filtered_df = df.copy()
            filter_masks = []
            
            for col in date_columns:
                parsed_dates = self.parse_date_column(df[col])
                filtered_df[f'{col}_parsed'] = parsed_dates
                year_mask = (parsed_dates.dt.year == target_year)
                filter_masks.append(year_mask)
            
            if filter_masks:
                combined_mask = filter_masks[0]
                for mask in filter_masks[1:]:
                    combined_mask = combined_mask | mask
                filtered_df = filtered_df[combined_mask.fillna(False)]
            
            return filtered_df
        
        # If only year is "All", filter by month only
        if target_month != "All" and target_year == "All":
            if isinstance(target_month, str):
                target_month_lower = target_month.lower()
                target_month_num = self.month_mapping.get(target_month_lower)
                if target_month_num is None:
                    raise ValueError(f"Unknown month: {target_month}")
            else:
                target_month_num = target_month
            
            date_columns = self.classify_sit_date_columns(df)
            if not date_columns:
                print("Warning: No date columns found for month filtering")
                return df
            
            filtered_df = df.copy()
            filter_masks = []
            
            for col in date_columns:
                parsed_dates = self.parse_date_column(df[col])
                filtered_df[f'{col}_parsed'] = parsed_dates
                month_mask = (parsed_dates.dt.month == target_month_num)
                filter_masks.append(month_mask)
            
            if filter_masks:
                combined_mask = filter_masks[0]
                for mask in filter_masks[1:]:
                    combined_mask = combined_mask | mask
                filtered_df = filtered_df[combined_mask.fillna(False)]
            
            return filtered_df
        
        # Standard filtering by both month and year
        if isinstance(target_month, str):
            target_month_lower = target_month.lower()
            target_month_num = self.month_mapping.get(target_month_lower)
            if target_month_num is None:
                raise ValueError(f"Unknown month: {target_month}")
        else:
            target_month_num = target_month
        
        # Find date columns
        date_columns = self.classify_sit_date_columns(df)
        
        if not date_columns:
            print("Warning: No date columns found for filtering")
            return df
        
        # Create a copy of the dataframe
        filtered_df = df.copy()
        
        # Parse date columns and create filter masks
        filter_masks = []
        
        for col in date_columns:
            parsed_dates = self.parse_date_column(df[col])
            filtered_df[f'{col}_parsed'] = parsed_dates
            
            # Create mask for target month and year
            month_mask = (parsed_dates.dt.month == target_month_num) & (parsed_dates.dt.year == target_year)
            filter_masks.append(month_mask)
        
        # Combine masks (OR operation - if any date column matches)
        if filter_masks:
            combined_mask = filter_masks[0]
            for mask in filter_masks[1:]:
                combined_mask = combined_mask | mask
            
            # Apply filter
            filtered_df = filtered_df[combined_mask.fillna(False)]
        
        return filtered_df
    
    def process_sit_files(self, file_paths_or_dataframes, target_month="July", target_year=2025):
        """
        Process multiple SIT files and filter by date.
        
        Args:
            file_paths_or_dataframes: List of file paths or DataFrames
            target_month: Target month name, number, or "All"
            target_year: Target year or "All"
        
        Returns:
            Dictionary with processed data and metadata
        """
        processed_files = []
        combined_data = []
        
        for i, file_input in enumerate(file_paths_or_dataframes):
            try:
                # Handle both file paths and DataFrames
                if isinstance(file_input, str):
                    df = pd.read_excel(file_input)
                    filename = file_input.split('/')[-1]
                elif isinstance(file_input, pd.DataFrame):
                    df = file_input.copy()
                    filename = f"dataframe_{i}"
                else:
                    print(f"Warning: Unsupported input type for item {i}")
                    continue
                
                # Extract date info from filename
                file_date_info = self.extract_date_from_filename(filename)
                
                # Filter by target month and year
                filtered_df = self.filter_by_month(df, target_month, target_year)
                
                # Add metadata
                filtered_df['source_file'] = filename
                filtered_df['file_month'] = file_date_info.get('month')
                filtered_df['file_year'] = file_date_info.get('year')
                filtered_df['file_week'] = file_date_info.get('week')
                
                processed_files.append({
                    'filename': filename,
                    'original_rows': len(df),
                    'filtered_rows': len(filtered_df),
                    'date_info': file_date_info,
                    'data': filtered_df
                })
                
                if not filtered_df.empty:
                    combined_data.append(filtered_df)
                
            except Exception as e:
                print(f"Error processing file {file_input}: {e}")
                continue
        
        # Combine all filtered data
        if combined_data:
            final_combined_df = pd.concat(combined_data, ignore_index=True)
        else:
            final_combined_df = pd.DataFrame()
        
        return {
            'combined_data': final_combined_df,
            'processed_files': processed_files,
            'target_month': target_month,
            'target_year': target_year,
            'total_files_processed': len(processed_files),
            'total_rows_after_filter': len(final_combined_df)
        }
    
    def generate_summary_report(self, processing_result):
        """
        Generate a summary report of the SIT processing.
        """
        result = processing_result
        
        filter_description = f"{result['target_month']} {result['target_year']}"
        if result['target_month'] == "All" and result['target_year'] == "All":
            filter_description = "All months and years (no filtering)"
        elif result['target_month'] == "All":
            filter_description = f"All months in {result['target_year']}"
        elif result['target_year'] == "All":
            filter_description = f"{result['target_month']} (all years)"
        
        report = f"""
SIT Data Processing Summary
==========================

Target Filter: {filter_description}
Total Files Processed: {result['total_files_processed']}
Total Rows After Filtering: {result['total_rows_after_filter']}

File Details:
"""
        
        for file_info in result['processed_files']:
            report += f"""
- {file_info['filename']}:
  Original Rows: {file_info['original_rows']}
  Filtered Rows: {file_info['filtered_rows']}
  Date Info: {file_info['date_info']}
"""
        
        return report

# Example usage and testing
if __name__ == "__main__":
    # Create classifier instance
    classifier = SITDateClassifier()
    
    # Test filename parsing
    test_filenames = [
        "SIT_Week1_July_2025.xlsx",
        "SIT_Week2_August_2025.xlsx",
        "SIT_B1_ETA.xlsx",
        "Updated_ETA_September_2025.xlsx"
    ]
    
    print("Testing filename parsing:")
    for filename in test_filenames:
        date_info = classifier.extract_date_from_filename(filename)
        print(f"{filename}: {date_info}")
    
    # Test with sample data
    sample_data = pd.DataFrame({
        'Item No.': ['A001', 'A002', 'A003'],
        'Quantity': [100, 200, 150],
        'Updated ETA port': ['2025-07-15', '2025-08-20', '2025-07-25'],
        'ETA  port': ['15/07/2025', '20/08/2025', '25/07/2025']
    })
    
    print("\nTesting date filtering:")
    
    # Test normal filtering
    filtered_data = classifier.filter_by_month(sample_data, "July", 2025)
    print(f"Original rows: {len(sample_data)}")
    print(f"Filtered rows (July 2025): {len(filtered_data)}")
    
    # Test "All" filtering
    all_data = classifier.filter_by_month(sample_data, "All", "All")
    print(f"Filtered rows (All months, All years): {len(all_data)}")
    
    # Test month "All" filtering
    all_months_data = classifier.filter_by_month(sample_data, "All", 2025)
    print(f"Filtered rows (All months, 2025): {len(all_months_data)}")
    
    # Test year "All" filtering
    all_years_data = classifier.filter_by_month(sample_data, "July", "All")
    print(f"Filtered rows (July, All years): {len(all_years_data)}")

