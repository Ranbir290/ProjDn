import pandas as pd
import numpy as np
from typing import Dict, List, Any

class DataAnalyzer:
    @staticmethod
    def get_data_summary(df: pd.DataFrame) -> Dict[str, Any]:
        summary = {
            'shape': df.shape,
            'memory_usage': f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB",
            'missing_data': {
                'total_missing': int(df.isnull().sum().sum()),
                'columns_with_missing': df.columns[df.isnull().any()].tolist()
            }
        }
        
        # Numerical analysis
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            summary['numeric_summary'] = {
                'columns': list(numeric_cols),
                'stats': df[numeric_cols].describe().to_dict()
            }
        
        # Categorical analysis
        cat_cols = df.select_dtypes(include=['object']).columns
        summary['categorical_summary'] = {
            'columns': list(cat_cols),
            'unique_counts': {col: df[col].nunique() for col in cat_cols}
        }
        
        return summary

    @staticmethod
    def clean_for_display(df: pd.DataFrame) -> pd.DataFrame:
        df_clean = df.copy()
        
        # Convert datetime columns to string
        for col in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d')
            elif pd.api.types.is_numeric_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].round(3)
        
        return df_clean.fillna('')