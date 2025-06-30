from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_cors import CORS
import pandas as pd
import numpy as np
import os
from werkzeug.utils import secure_filename
import json
from datetime import datetime, time
import logging
import re
from dateutil import parser as date_parser

# Import the Analysis blueprint
from Analysis import Analysis

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Register the Analysis blueprint
app.register_blueprint(Analysis, url_prefix='')

# Enable CORS for all routes
CORS(app)

# Configure upload settings
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Store processed data in memory (in production, use Redis or database)
processed_data = {}

class ExcelProcessor:
    def detect_column_types(self, df):
        """Detect data types for each column"""
        column_types = {}
        
        for col in df.columns:
            sample_data = df[col].dropna().head(100)
            
            if len(sample_data) == 0:
                column_types[col] = 'text'
                continue
            
            # Check if numeric, date, or time
            numeric_count = 0
            date_count = 0
            time_count = 0
            datetime_count = 0
            
            for value in sample_data:
                value_str = str(value).strip()
                
                # Check if numeric
                try:
                    float(value_str)
                    numeric_count += 1
                except (ValueError, TypeError):
                    pass
                
                # Check if pure time (HH:MM:SS or HH:MM format)
                if self._is_time_only(value_str):
                    time_count += 1
                # Check if datetime (contains both date and time)
                elif self._is_datetime_like(value_str):
                    datetime_count += 1
                # Check if date only
                elif self._is_date_like(value_str):
                    date_count += 1
            
            total = len(sample_data)
            
            # Prioritize time detection first, then datetime, then date
            if time_count > total * 0.6:
                column_types[col] = 'time'
            elif datetime_count > total * 0.6:
                column_types[col] = 'datetime'
            elif date_count > total * 0.6:
                column_types[col] = 'date'
            elif numeric_count > total * 0.7:
                column_types[col] = 'numeric'
            else:
                column_types[col] = 'text'
        
        return column_types
    
    def _is_time_only(self, value):
        """Check if a value is purely time (HH:MM:SS or HH:MM)"""
        if not value or str(value).strip() == '':
            return False
        
        value_str = str(value).strip()
        
        # Time-only patterns
        time_patterns = [
            r'^\d{1,2}:\d{2}:\d{2}$',  # HH:MM:SS
            r'^\d{1,2}:\d{2}$',        # HH:MM
            r'^\d{1,2}:\d{2}:\d{2}\.\d+$',  # HH:MM:SS.microseconds
            r'^\d{1,2}:\d{2}:\d{2}\s?(AM|PM|am|pm)$',  # HH:MM:SS AM/PM
            r'^\d{1,2}:\d{2}\s?(AM|PM|am|pm)$',        # HH:MM AM/PM
        ]
        
        # Check if it matches time patterns and doesn't contain date info
        for pattern in time_patterns:
            if re.match(pattern, value_str):
                # Make sure it doesn't contain date information
                if not re.search(r'\d{4}|/|-|\.|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec', value_str, re.IGNORECASE):
                    try:
                        # Try to parse as time
                        if ':' in value_str:
                            parts = value_str.replace('AM', '').replace('PM', '').replace('am', '').replace('pm', '').strip().split(':')
                            if len(parts) >= 2:
                                hour = int(parts[0])
                                minute = int(parts[1])
                                second = int(parts[2]) if len(parts) > 2 else 0
                                if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                                    return True
                    except (ValueError, IndexError):
                        pass
        
        return False
    
    def _is_datetime_like(self, value):
        """Check if a value contains both date and time information"""
        if not value or str(value).strip() == '':
            return False
        
        value_str = str(value).strip()
        
        # Must contain both date and time indicators
        has_date = bool(re.search(r'\d{4}|/|-|\.|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec', value_str, re.IGNORECASE))
        has_time = bool(re.search(r'\d{1,2}:\d{2}', value_str))
        
        if has_date and has_time:
            try:
                parsed_date = date_parser.parse(value_str, fuzzy=False)
                if 1900 <= parsed_date.year <= 2100:
                    return True
            except (ValueError, TypeError, OverflowError):
                pass
        
        return False
    
    def _is_date_like(self, value):
        """Check if a value looks like a date (without time)"""
        if not value or str(value).strip() == '':
            return False
        
        value_str = str(value).strip()
        
        # Skip if it's already identified as time-only
        if self._is_time_only(value_str):
            return False
        
        # Common date patterns (without time)
        date_patterns = [
            r'^\d{4}-\d{1,2}-\d{1,2}$',  # YYYY-MM-DD
            r'^\d{1,2}/\d{1,2}/\d{4}$',  # MM/DD/YYYY
            r'^\d{1,2}-\d{1,2}-\d{4}$',  # MM-DD-YYYY
            r'^\d{1,2}/\d{1,2}/\d{2}$',  # MM/DD/YY
            r'^\d{4}/\d{1,2}/\d{1,2}$',  # YYYY/MM/DD
            r'^\d{1,2}\.\d{1,2}\.\d{4}$',  # DD.MM.YYYY
            r'^\d{4}\.\d{1,2}\.\d{1,2}$',  # YYYY.MM.DD
        ]
        
        # Check for date patterns without time
        for pattern in date_patterns:
            if re.match(pattern, value_str):
                try:
                    parsed_date = date_parser.parse(value_str, fuzzy=False)
                    if 1900 <= parsed_date.year <= 2100:
                        return True
                except (ValueError, TypeError, OverflowError):
                    pass
        
        return False
    
    def get_column_filter_options(self, df, column):
        """Get filter options for a column based on its type"""
        column_data = df[column].dropna()
        
        if len(column_data) == 0:
            return {'type': 'text', 'options': []}
        
        # Detect column type
        column_types = self.detect_column_types(df)
        col_type = column_types.get(column, 'text')
        
        if col_type == 'time':
            # For time columns, provide min/max times
            times = []
            for value in column_data:
                try:
                    time_str = str(value).strip()
                    # Parse time string
                    if ':' in time_str:
                        # Remove AM/PM and clean up
                        clean_time = re.sub(r'\s?(AM|PM|am|pm)', '', time_str).strip()
                        parts = clean_time.split(':')
                        if len(parts) >= 2:
                            hour = int(parts[0])
                            minute = int(parts[1])
                            second = int(parts[2]) if len(parts) > 2 else 0
                            
                            # Handle 12-hour format
                            if 'PM' in time_str.upper() and hour != 12:
                                hour += 12
                            elif 'AM' in time_str.upper() and hour == 12:
                                hour = 0
                            
                            if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                                times.append(time(hour, minute, second))
                except (ValueError, IndexError):
                    continue
            
            if times:
                min_time = min(times)
                max_time = max(times)
                return {
                    'type': 'time',
                    'min_time': min_time.strftime('%H:%M:%S'),
                    'max_time': max_time.strftime('%H:%M:%S')
                }
        
        elif col_type == 'datetime':
            # For datetime columns, provide min/max dates
            dates = []
            for value in column_data:
                try:
                    parsed_date = date_parser.parse(str(value))
                    dates.append(parsed_date)
                except:
                    continue
            
            if dates:
                return {
                    'type': 'datetime',
                    'min_date': min(dates).strftime('%Y-%m-%d'),
                    'max_date': max(dates).strftime('%Y-%m-%d')
                }
        
        elif col_type == 'date':
            # For date-only columns
            dates = []
            for value in column_data:
                try:
                    parsed_date = date_parser.parse(str(value))
                    dates.append(parsed_date)
                except:
                    continue
            
            if dates:
                return {
                    'type': 'date',
                    'min_date': min(dates).strftime('%Y-%m-%d'),
                    'max_date': max(dates).strftime('%Y-%m-%d')
                }
        
        elif col_type == 'numeric':
            # For numeric columns, provide min/max values
            numeric_values = []
            for value in column_data:
                try:
                    numeric_values.append(float(str(value)))
                except:
                    continue
            
            if numeric_values:
                return {
                    'type': 'numeric',
                    'min_value': min(numeric_values),
                    'max_value': max(numeric_values)
                }
        
        else:
            # For text columns, provide unique values (limited to 50)
            unique_values = column_data.unique()[:50]
            return {
                'type': 'text',
                'options': [str(val) for val in unique_values if pd.notna(val)]
            }
        
        return {'type': 'text', 'options': []}

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def process_excel_file(filepath, file_id):
    """Process Excel file and return data with metadata"""
    try:
        # Read the Excel file
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
        
        # Handle NaN values
        df = df.fillna('')
        
        # Store the data
        processed_data[file_id] = df
        
        # Get basic statistics
        total_rows, total_columns = df.shape
        
        # Get column information with filter options
        processor = ExcelProcessor()
        columns_info = []
        column_types = processor.detect_column_types(df)
        
        for col in df.columns:
            filter_options = processor.get_column_filter_options(df, col)
            col_info = {
                'name': str(col),
                'type': str(df[col].dtype),
                'data_type': column_types.get(col, 'text'),
                'non_null_count': int(df[col].count()),
                'unique_count': int(df[col].nunique()),
                'filter_options': filter_options
            }
            columns_info.append(col_info)
        
        # Convert DataFrame to list of dictionaries for JSON serialization
        # Handle datetime and other non-serializable types
        data = []
        for record in df.to_dict('records'):
            serializable_record = {}
            for key, value in record.items():
                if pd.isna(value):
                    serializable_record[key] = None
                elif isinstance(value, (datetime, pd.Timestamp)):
                    serializable_record[key] = value.isoformat()
                elif isinstance(value, time):
                    serializable_record[key] = value.strftime('%H:%M:%S')
                elif isinstance(value, (np.integer, np.floating)):
                    serializable_record[key] = value.item()
                elif isinstance(value, np.ndarray):
                    serializable_record[key] = value.tolist()
                else:
                    serializable_record[key] = str(value)
            data.append(serializable_record)
        
        # Get file size
        file_size = os.path.getsize(filepath)
        
        metadata = {
            'total_rows': total_rows,
            'total_columns': total_columns,
            'columns_info': columns_info,
            'file_size': file_size,
            'processed_at': datetime.now().isoformat(),
            'file_id': file_id
        }
        
        return {
            'success': True,
            'data': data,
            'metadata': metadata,
            'columns': list(df.columns),
            'file_id': file_id
        }
        
    except Exception as e:
        logger.error(f"Error processing file {filepath}: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def apply_filters(df, filters):
    """Apply filters to dataframe"""
    filtered_df = df.copy()
    
    for column, filter_config in filters.items():
        if column not in df.columns:
            continue
        
        filter_type = filter_config.get('type')
        
        if filter_type == 'text':
            values = filter_config.get('values', [])
            if values:
                filtered_df = filtered_df[filtered_df[column].astype(str).isin(values)]
        
        elif filter_type == 'numeric':
            min_val = filter_config.get('min')
            max_val = filter_config.get('max')
            
            # Convert column to numeric
            numeric_col = pd.to_numeric(filtered_df[column], errors='coerce')
            
            if min_val is not None:
                filtered_df = filtered_df[numeric_col >= min_val]
            if max_val is not None:
                filtered_df = filtered_df[numeric_col <= max_val]
        
        elif filter_type == 'time':
            start_time = filter_config.get('start_time')
            end_time = filter_config.get('end_time')
            
            if start_time or end_time:
                # Convert column values to time objects for comparison
                def parse_time_value(val):
                    try:
                        val_str = str(val).strip()
                        if ':' in val_str:
                            # Remove AM/PM and clean up
                            clean_time = re.sub(r'\s?(AM|PM|am|pm)', '', val_str).strip()
                            parts = clean_time.split(':')
                            if len(parts) >= 2:
                                hour = int(parts[0])
                                minute = int(parts[1])
                                second = int(parts[2]) if len(parts) > 2 else 0
                                
                                # Handle 12-hour format
                                if 'PM' in val_str.upper() and hour != 12:
                                    hour += 12
                                elif 'AM' in val_str.upper() and hour == 12:
                                    hour = 0
                                
                                if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                                    return time(hour, minute, second)
                    except (ValueError, IndexError):
                        pass
                    return None
                
                # Parse filter times
                start_time_obj = None
                end_time_obj = None
                
                if start_time:
                    try:
                        parts = start_time.split(':')
                        start_time_obj = time(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)
                    except (ValueError, IndexError):
                        pass
                
                if end_time:
                    try:
                        parts = end_time.split(':')
                        end_time_obj = time(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)
                    except (ValueError, IndexError):
                        pass
                
                # Apply time filters
                if start_time_obj or end_time_obj:
                    mask = pd.Series([True] * len(filtered_df), index=filtered_df.index)
                    
                    for idx, val in filtered_df[column].items():
                        time_val = parse_time_value(val)
                        if time_val is not None:
                            if start_time_obj and time_val < start_time_obj:
                                mask[idx] = False
                            if end_time_obj and time_val > end_time_obj:
                                mask[idx] = False
                        else:
                            mask[idx] = False
                    
                    filtered_df = filtered_df[mask]
        
        elif filter_type == 'datetime':
            start_date = filter_config.get('start_date')
            end_date = filter_config.get('end_date')
            
            # Convert column to datetime
            try:
                date_col = pd.to_datetime(filtered_df[column], errors='coerce')
                
                if start_date:
                    start_dt = pd.to_datetime(start_date)
                    filtered_df = filtered_df[date_col >= start_dt]
                
                if end_date:
                    end_dt = pd.to_datetime(end_date)
                    filtered_df = filtered_df[date_col <= end_dt]
            except:
                continue
        
        elif filter_type == 'date':
            start_date = filter_config.get('start_date')
            end_date = filter_config.get('end_date')
            
            # Convert column to date
            try:
                date_col = pd.to_datetime(filtered_df[column], errors='coerce').dt.date
                
                if start_date:
                    start_dt = pd.to_datetime(start_date).date()
                    filtered_df = filtered_df[date_col >= start_dt]
                
                if end_date:
                    end_dt = pd.to_datetime(end_date).date()
                    filtered_df = filtered_df[date_col <= end_dt]
            except:
                continue
    
    return filtered_df

# Custom JSON encoder to handle datetime and other non-serializable types
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif isinstance(obj, time):
            return obj.strftime('%H:%M:%S')
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        return super().default(obj)

# Set the custom JSON encoder for the Flask app
app.json_encoder = CustomJSONEncoder

@app.route('/')
def index():
    """Serve the main HTML page"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and processing"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file provided'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({
                'success': False, 
                'error': 'Invalid file type. Please upload .xlsx, .xls, or .csv files'
            }), 400
        
        # Secure the filename and save
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_')
        filename = timestamp + filename
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        file.save(filepath)
        logger.info(f"File saved: {filepath}")
        
        # Generate unique file ID
        file_id = timestamp + secure_filename(file.filename)
        
        # Process the file
        result = process_excel_file(filepath, file_id)
        
        # Clean up the uploaded file after processing
        try:
            os.remove(filepath)
        except:
            pass
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 500
            
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/filter', methods=['POST'])
def filter_data():
    """Apply filters to the data"""
    try:
        request_data = request.get_json()
        file_id = request_data.get('file_id')
        filters = request_data.get('filters', {})
        search_term = request_data.get('search', '')
        page = request_data.get('page', 1)
        per_page = request_data.get('per_page', 50)
        
        if not file_id or file_id not in processed_data:
            return jsonify({'success': False, 'error': 'File not found'}), 404
        
        df = processed_data[file_id]
        
        # Apply filters
        if filters:
            df = apply_filters(df, filters)
        
        # Apply search
        if search_term:
            mask = df.astype(str).apply(
                lambda x: x.str.contains(search_term, case=False, na=False)
            ).any(axis=1)
            df = df[mask]
        
        # Calculate pagination
        total_rows = len(df)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        
        paginated_df = df.iloc[start_idx:end_idx]
        
        # Convert to records with proper serialization
        data = []
        for record in paginated_df.to_dict('records'):
            serializable_record = {}
            for key, value in record.items():
                if pd.isna(value):
                    serializable_record[key] = None
                elif isinstance(value, (datetime, pd.Timestamp)):
                    serializable_record[key] = value.isoformat()
                elif isinstance(value, time):
                    serializable_record[key] = value.strftime('%H:%M:%S')
                elif isinstance(value, (np.integer, np.floating)):
                    serializable_record[key] = value.item()
                elif isinstance(value, np.ndarray):
                    serializable_record[key] = value.tolist()
                else:
                    serializable_record[key] = str(value)
            data.append(serializable_record)
        
        return jsonify({
            'success': True,
            'data': data,
            'total_rows': total_rows,
            'page': page,
            'per_page': per_page,
            'total_pages': (total_rows + per_page - 1) // per_page
        })
        
    except Exception as e:
        logger.error(f"Filter error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/export', methods=['POST'])
def export_data():
    """Export filtered data"""
    try:
        request_data = request.get_json()
        file_id = request_data.get('file_id')
        filters = request_data.get('filters', {})
        search_term = request_data.get('search', '')
        export_format = request_data.get('format', 'csv')
        
        if not file_id or file_id not in processed_data:
            return jsonify({'success': False, 'error': 'File not found'}), 404
        
        df = processed_data[file_id]
        
        # Apply filters
        if filters:
            df = apply_filters(df, filters)
        
        # Apply search
        if search_term:
            mask = df.astype(str).apply(
                lambda x: x.str.contains(search_term, case=False, na=False)
            ).any(axis=1)
            df = df[mask]
        
        # Export data
        if export_format == 'csv':
            csv_data = df.to_csv(index=False)
            return jsonify({
                'success': True,
                'data': csv_data,
                'filename': f'filtered_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            })
        
        return jsonify({'success': False, 'error': 'Unsupported export format'}), 400
        
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })

@app.errorhandler(413)
def too_large(e):
    """Handle file too large error"""
    return jsonify({
        'success': False,
        'error': 'File too large. Maximum size is 16MB.'
    }), 413

@app.errorhandler(500)
def internal_error(e):
    """Handle internal server errors"""
    return jsonify({
        'success': False,
        'error': 'Internal server error occurred.'
    }), 500

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
