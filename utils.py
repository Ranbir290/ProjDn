import pandas as pd
import numpy as np
from typing import Dict, List, Any
import json
from datetime import datetime

class ExcelProcessor:
    """Class to handle Excel file processing with advanced features"""
    
    @staticmethod
    def analyze_data(df: pd.DataFrame) -> Dict[str, Any]:
        """Perform detailed analysis of the DataFrame"""
        analysis = {
            'basic_info': {
                'shape': df.shape,
                'memory_usage': df.memory_usage(deep=True).sum(),
                'dtypes': df.dtypes.to_dict()
            },
            'missing_data': {
                'total_missing': df.isnull().sum().sum(),
                'missing_by_column': df.isnull().sum().to_dict(),
                'missing_percentage': (df.isnull().sum() / len(df) * 100).to_dict()
            },
            'numerical_summary': {},
            'categorical_summary': {},
            'date_columns': []
        }
        
        # Detect date columns
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                analysis['date_columns'].append({
                    'name': col,
                    'min_date': df[col].min().isoformat() if pd.notna(df[col].min()) else None,
                    'max_date': df[col].max().isoformat() if pd.notna(df[col].max()) else None
                })
            elif df[col].dtype == 'object':
                # Try to detect date-like strings
                sample_values = df[col].dropna().head(10).tolist()
                date_like_count = 0
                for val in sample_values:
                    try:
                        pd.to_datetime(str(val))
                        date_like_count += 1
                    except:
                        pass
                
                if date_like_count >= len(sample_values) * 0.7:  # 70% threshold
                    try:
                        converted_dates = pd.to_datetime(df[col], errors='coerce')
                        min_date = converted_dates.min()
                        max_date = converted_dates.max()
                        analysis['date_columns'].append({
                            'name': col,
                            'min_date': min_date.isoformat() if pd.notna(min_date) else None,
                            'max_date': max_date.isoformat() if pd.notna(max_date) else None,
                            'is_string_date': True
                        })
                    except:
                        pass
        
        # Numerical columns analysis
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        if len(numerical_cols) > 0:
            analysis['numerical_summary'] = df[numerical_cols].describe().to_dict()
        
        # Categorical columns analysis
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            analysis['categorical_summary'][col] = {
                'unique_count': df[col].nunique(),
                'top_values': df[col].value_counts().head(5).to_dict()
            }
        
        return analysis
    
    @staticmethod
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare data for display"""
        df_clean = df.copy()
        
        # Store original data types for filtering
        original_dtypes = {}
        
        # Handle different data types
        for col in df_clean.columns:
            original_dtypes[col] = str(df_clean[col].dtype)
            
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].astype(str)
            elif pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                # Keep datetime for filtering, but also store formatted version
                df_clean[f'{col}_formatted'] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            elif pd.api.types.is_numeric_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].round(2)
        
        # Replace NaN values
        df_clean = df_clean.fillna('')
        
        return df_clean
    
    @staticmethod
    def apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """Apply various filters to the DataFrame"""
        filtered_df = df.copy()
        
        # Text search filter
        if filters.get('search_term'):
            search_term = filters['search_term'].lower()
            mask = filtered_df.astype(str).apply(
                lambda x: x.str.lower().str.contains(search_term, na=False)
            ).any(axis=1)
            filtered_df = filtered_df[mask]
        
        # Date range filters
        if filters.get('date_filters'):
            for date_filter in filters['date_filters']:
                col_name = date_filter['column']
                start_date = date_filter.get('start_date')
                end_date = date_filter.get('end_date')
                
                if col_name in filtered_df.columns:
                    # Convert column to datetime if it's not already
                    if not pd.api.types.is_datetime64_any_dtype(filtered_df[col_name]):
                        try:
                            filtered_df[col_name] = pd.to_datetime(filtered_df[col_name], errors='coerce')
                        except:
                            continue
                    
                    # Apply date range filter
                    if start_date:
                        start_dt = pd.to_datetime(start_date)
                        filtered_df = filtered_df[filtered_df[col_name] >= start_dt]
                    
                    if end_date:
                        end_dt = pd.to_datetime(end_date)
                        filtered_df = filtered_df[filtered_df[col_name] <= end_dt]
        
        # Column value filters
        if filters.get('column_filters'):
            for col_filter in filters['column_filters']:
                col_name = col_filter['column']
                values = col_filter['values']
                
                if col_name in filtered_df.columns and values:
                    filtered_df = filtered_df[filtered_df[col_name].isin(values)]
        
        return filtered_df
