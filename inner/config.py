import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re

class ExcelProcessor:
    def __init__(self):
        self.date_formats = [
            '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%m-%d-%Y',
            '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S',
            '%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M', '%m/%d/%Y %H:%M',
            '%B %d, %Y', '%b %d, %Y', '%d %B %Y', '%d %b %Y'
        ]
    
    def detect_column_types(self, df):
        """Detect the data type of each column for better filtering"""
        column_types = {}
        
        for col in df.columns:
            # Skip if column is empty
            if df[col].dropna().empty:
                column_types[col] = 'text'
                continue
            
            # Check for datetime
            if self._is_datetime_column(df[col]):
                column_types[col] = 'datetime'
            # Check for numeric
            elif self._is_numeric_column(df[col]):
                column_types[col] = 'numeric'
            # Check for boolean
            elif self._is_boolean_column(df[col]):
                column_types[col] = 'boolean'
            else:
                column_types[col] = 'text'
        
        return column_types
    
    def _is_datetime_column(self, series):
        """Check if a pandas Series contains datetime data"""
        # First check if it's already datetime
        if pd.api.types.is_datetime64_any_dtype(series):
            return True
        
        # Check if string values can be parsed as dates
        non_null_series = series.dropna().astype(str)
        if len(non_null_series) == 0:
            return False
        
        # Sample a few values to test
        sample_size = min(10, len(non_null_series))
        sample_values = non_null_series.head(sample_size)
        
        date_count = 0
        for value in sample_values:
            if self._try_parse_date(value):
                date_count += 1
        
        # If more than 70% of sampled values are dates, consider it a datetime column
        return (date_count / sample_size) > 0.7
    
    def _is_numeric_column(self, series):
        """Check if a pandas Series contains numeric data"""
        # Check if it's already numeric
        if pd.api.types.is_numeric_dtype(series):
            return True
        
        # Try to convert to numeric
        try:
            pd.to_numeric(series.dropna(), errors='raise')
            return True
        except:
            return False
    
    def _is_boolean_column(self, series):
        """Check if a pandas Series contains boolean data"""
        if pd.api.types.is_bool_dtype(series):
            return True
        
        # Check string boolean values
        unique_values = set(str(val).lower() for val in series.dropna().unique())
        boolean_values = {'true', 'false', 'yes', 'no', '1', '0', 'y', 'n'}
        
        return len(unique_values) <= 2 and unique_values.issubset(boolean_values)
    
    def _try_parse_date(self, date_string):
        """Try to parse a string as a date using various formats"""
        date_string = str(date_string).strip()
        
        # Check for Excel date numbers
        try:
            if date_string.replace('.', '').isdigit():
                excel_date = float(date_string)
                if 1 <= excel_date <= 100000:  # Reasonable Excel date range
                    return True
        except:
            pass
        
        # Try different date formats
        for fmt in self.date_formats:
            try:
                datetime.strptime(date_string, fmt)
                return True
            except ValueError:
                continue
        
        # Try pandas date parsing
        try:
            pd.to_datetime(date_string, errors='raise')
            return True
        except:
            pass
        
        return False
    
    def get_column_filter_options(self, df, column):
        """Get filter options for a specific column based on its data type"""
        column_type = self.detect_column_types(df)[column]
        non_null_data = df[column].dropna()
        
        if len(non_null_data) == 0:
            return {'type': 'text', 'options': []}
        
        if column_type == 'datetime':
            return self._get_datetime_filter_options(non_null_data)
        elif column_type == 'numeric':
            return self._get_numeric_filter_options(non_null_data)
        elif column_type == 'boolean':
            return self._get_boolean_filter_options(non_null_data)
        else:
            return self._get_text_filter_options(non_null_data)
    
    def _get_datetime_filter_options(self, series):
        """Get filter options for datetime columns"""
        try:
            # Convert to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(series):
                datetime_series = pd.to_datetime(series, errors='coerce')
            else:
                datetime_series = series
            
            datetime_series = datetime_series.dropna()
            
            if len(datetime_series) == 0:
                return {'type': 'text', 'options': []}
            
            min_date = datetime_series.min()
            max_date = datetime_series.max()
            
            return {
                'type': 'datetime',
                'min_date': min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else None,
                'max_date': max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else None,
                'sample_values': [
                    date.strftime('%Y-%m-%d') if pd.notna(date) else str(date)
                    for date in datetime_series.head(5)
                ]
            }
        except Exception as e:
            # Fallback to text if datetime parsing fails
            return self._get_text_filter_options(series)
    
    def _get_numeric_filter_options(self, series):
        """Get filter options for numeric columns"""
        try:
            numeric_series = pd.to_numeric(series, errors='coerce').dropna()
            
            if len(numeric_series) == 0:
                return {'type': 'text', 'options': []}
            
            return {
                'type': 'numeric',
                'min_value': float(numeric_series.min()),
                'max_value': float(numeric_series.max()),
                'sample_values': [float(val) for val in numeric_series.head(5)]
            }
        except Exception as e:
            return self._get_text_filter_options(series)
    
    def _get_boolean_filter_options(self, series):
        """Get filter options for boolean columns"""
        unique_values = series.unique()
        return {
            'type': 'boolean',
            'options': [str(val) for val in unique_values if pd.notna(val)]
        }
    
    def _get_text_filter_options(self, series):
        """Get filter options for text columns"""
        unique_values = series.unique()
        
        # If too many unique values, just return the type
        if len(unique_values) > 50:
            return {
                'type': 'text',
                'options': []
            }
        
        return {
            'type': 'text',
            'options': [str(val) for val in unique_values if pd.notna(val)]
        }
    
    def apply_filters(self, df, filters):
        """Apply filters to the dataframe"""
        filtered_df = df.copy()
        
        for column, filter_config in filters.items():
            if column not in df.columns:
                continue
            
            filter_type = filter_config.get('type')
            
            if filter_type == 'datetime':
                filtered_df = self._apply_datetime_filter(filtered_df, column, filter_config)
            elif filter_type == 'numeric':
                filtered_df = self._apply_numeric_filter(filtered_df, column, filter_config)
            elif filter_type == 'boolean':
                filtered_df = self._apply_boolean_filter(filtered_df, column, filter_config)
            elif filter_type == 'text':
                filtered_df = self._apply_text_filter(filtered_df, column, filter_config)
        
        return filtered_df
    
    def _apply_datetime_filter(self, df, column, filter_config):
        """Apply datetime range filter"""
        start_date = filter_config.get('start_date')
        end_date = filter_config.get('end_date')
        
        if not start_date and not end_date:
            return df
        
        try:
            # Convert column to datetime
            datetime_series = pd.to_datetime(df[column], errors='coerce')
            mask = pd.Series([True] * len(df))
            
            if start_date:
                start_dt = pd.to_datetime(start_date)
                mask &= (datetime_series >= start_dt)
            
            if end_date:
                end_dt = pd.to_datetime(end_date)
                mask &= (datetime_series <= end_dt)
            
            return df[mask]
        except Exception as e:
            return df
    
    def _apply_numeric_filter(self, df, column, filter_config):
        """Apply numeric range filter"""
        min_value = filter_config.get('min_value')
        max_value = filter_config.get('max_value')
        
        if min_value is None and max_value is None:
            return df
        
        try:
            numeric_series = pd.to_numeric(df[column], errors='coerce')
            mask = pd.Series([True] * len(df))
            
            if min_value is not None:
                mask &= (numeric_series >= min_value)
            
            if max_value is not None:
                mask &= (numeric_series <= max_value)
            
            return df[mask]
        except Exception as e:
            return df
    
    def _apply_boolean_filter(self, df, column, filter_config):
        """Apply boolean filter"""
        selected_values = filter_config.get('selected_values', [])
        
        if not selected_values:
            return df
        
        mask = df[column].astype(str).isin([str(val) for val in selected_values])
        return df[mask]
    
    def _apply_text_filter(self, df, column, filter_config):
        """Apply text filter"""
        selected_values = filter_config.get('selected_values', [])
        search_text = filter_config.get('search_text', '')
        
        mask = pd.Series([True] * len(df))
        
        if selected_values:
            mask &= df[column].astype(str).isin([str(val) for val in selected_values])
        
        if search_text:
            mask &= df[column].astype(str).str.contains(search_text, case=False, na=False)
        
        return df[mask]