from flask import Flask, render_template, request, jsonify
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
app.config['WEBAPI_THROTTLE'] = 1
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Register the Analysis blueprint
app.register_blueprint(Analysis, url_prefix='')

# Enable CORS for all routes
CORS(app)

# Configure upload settings
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv', 'jrn', 'prn'}

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Store processed data in memory
processedData = {}
fileMetadata = {}

def extract_date_from_filename(filename):
    """Extract date from filename using various patterns"""
    name = os.path.splitext(filename)[0]

    # Handle YYYYMMDD format — now using the last occurrence
    yyyymmdd_matches = re.findall(r'(\d{8})', name)
    if yyyymmdd_matches:
        date_str = yyyymmdd_matches[-1]
        try:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            
            if (1900 <= year <= 2100 and 
                1 <= month <= 12 and 
                1 <= day <= 31):
                dt = datetime(year, month, day)
                return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            pass

    # Handle YYYY-MM-DD format
    yyyy_mm_dd_match = re.search(r'(\d{4}-\d{2}-\d{2})', name)
    if yyyy_mm_dd_match:
        date_str = yyyy_mm_dd_match.group(1)
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

    # Handle other formats...
    return 'Unknown'

def extract_json_from_string(text):
    """Extract JSON object from string"""
    if not text:
        return None, -1
    start_idx = text.find('{')
    if start_idx == -1:
        return None, -1
    brace_count = 0
    end_idx = start_idx
    for i in range(start_idx, len(text)):
        if text[i] == '{':
            brace_count += 1
        elif text[i] == '}':
            brace_count -= 1
            if brace_count == 0:
                end_idx = i
                break
    if brace_count != 0:
        return None, -1
    json_str = text[start_idx:end_idx + 1]
    try:
        return json.loads(json_str), end_idx
    except json.JSONDecodeError:
        return json_str, end_idx

def extract_type_from_data(data_text):
    """Extract type from data text"""
    if not data_text:
        return None
    match = re.search(r'\b(display|activated|result)\s*:', data_text, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    for keyword in ['display', 'activated', 'result']:
        if keyword in data_text.lower():
            return keyword
    return None

def parse_system_jrn_line(line, direction_symbols, date_from_filename=None):
    """Parse a single line from system journal file"""
    line = line.strip()
    if not line:
        return None
    match = re.match(r'^(\d{2}:\d{2}:\d{2})\s+(\S+)\s+(.*)$', line)
    if not match:
        return None

    timestamp, moduleid, rest = match.groups()

    for symbol in direction_symbols:
        if f' {symbol} ' in rest:
            parts = rest.rsplit(f' {symbol} ', 1)
            if len(parts) == 2:
                modulename = parts[0].strip()
                restdata = parts[1].strip()
                data_type = extract_type_from_data(restdata)
                param_json, end_idx = extract_json_from_string(restdata)
                truncated_restdata = restdata[:end_idx+1] if end_idx != -1 else restdata
                combined_data = f"{symbol} {truncated_restdata}"
                param_string = json.dumps(param_json) if isinstance(param_json, dict) else param_json

                view_id = view_key = route_name = url = None
                if isinstance(param_json, dict):
                    view_id = param_json.get('viewId')
                    view_key = param_json.get('viewKey')
                    route_name = param_json.get('routeName')
                    url = param_json.get('url')

                return {
                    'Date': date_from_filename or 'Unknown',
                    'Timestamp': timestamp,
                    'Module id': moduleid,
                    'Module name': modulename,
                    'Data': combined_data,
                    'Type': data_type,
                    'Param string': param_string,
                    'View id': view_id,
                    'View key': view_key,
                    'Route name': route_name,
                    'Url': url
                }
    return None

def parse_customer_jrn_blocks(lines, filename):
    """Parse customer transaction journal blocks"""
    date = extract_date_from_filename(filename)
    blocks, current_block = [], []

    for line in lines:
        if line.strip().startswith('*'):
            if current_block:
                blocks.append(current_block)
                current_block = []
        else:
            current_block.append(line.rstrip())

    if current_block:
        blocks.append(current_block)

    parsed_rows = []

    for block in blocks:
        if not block:
            continue

        # Initialize metadata
        transaction_number = function = state = end_state = pan = ''
        chaining = 'N'

        # First pass: extract shared metadata
        for line in block:
            if 'Transaction no' in line and not transaction_number:
                match = re.search(r"Transaction no\. *'([^']+)'", line)
                if match:
                    transaction_number = match.group(1)
            
            if 'Function' in line and not function:
                match = re.search(r"Function\s+'([^']+)'", line)
                if match:
                    function = match.group(1)
            
            if 'state' in line.lower() and not state:
                match = re.search(r"state\s+'([^']+)'", line, re.IGNORECASE)
                if match:
                    state = match.group(1)
            
            if 'end-state' in line.lower() and not end_state:
                match = re.search(r"end-state'?\s*'([^']+)'", line, re.IGNORECASE)
                if match:
                    end_state = match.group(1)
            
            if 'PAN' in line and not pan:
                match = re.search(r"PAN\s+'([^']+)'", line)
                if match:
                    pan = match.group(1)
            
            if 'chaining' in line.lower():
                chaining = 'Y'

        # Second pass: build rows
        for line in block:
            timestamp = moduleid = ''
            raw_line = line.strip()

            match = re.match(r"^(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(.*)", raw_line)
            if match:
                timestamp, moduleid, _ = match.groups()

            row = {
                'Date': date,
                'Timestamp': timestamp,
                'Module id': moduleid,
                'Transaction': transaction_number,
                'Function': function,
                'Pan': pan,
                'State': state,
                'End state': end_state,
                'Chaining': chaining
            }
            parsed_rows.append(row)

    return parsed_rows

def detect_jrn_format(filepath):
    """Detect if JRN file is customer format or system format"""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            sample_lines = [f.readline().strip() for _ in range(10)]
        
        # Check for customer format indicators
        for line in sample_lines:
            if any(keyword in line for keyword in ['Transaction no', 'Function', 'PAN', 'state']):
                return 'customer'
        
        # Check for system format indicators
        for line in sample_lines:
            if any(symbol in line for symbol in ['<', '>', '*']) and re.match(r'^\d{2}:\d{2}:\d{2}', line):
                return 'system'
        
        return 'unknown'
    except Exception:
        return 'unknown'

def reorder_columns(df):
    """Reorder columns to move 'Data' column to the end"""
    columns = list(df.columns)
    end_columns = []
    other_columns = []
    
    for col in columns:
        if col.lower() == 'data':
            end_columns.append(col)
        else:
            other_columns.append(col)
    
    new_order = other_columns + end_columns
    return df[new_order]

def process_jrn_file(filepath, fileId):
    """Process JRN file based on detected format"""
    try:
        format_type = detect_jrn_format(filepath)
        filename = os.path.basename(filepath)
        date_from_filename = extract_date_from_filename(filename)
        
        if format_type == 'customer':
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
            
            parsed_rows = parse_customer_jrn_blocks(lines, filename)
            if not parsed_rows:
                return {'success': False, 'error': 'No valid customer transactions found'}
            
            df = pd.DataFrame(parsed_rows)
            
        elif format_type == 'system':
            direction_symbols = ['<', '>', '*']
            parsed_rows = []
            
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    parsed = parse_system_jrn_line(line, direction_symbols, date_from_filename)
                    if parsed:
                        parsed_rows.append(parsed)
            
            if not parsed_rows:
                return {'success': False, 'error': 'No valid system log entries found'}
            
            df = pd.DataFrame(parsed_rows)
        
        else:
            return {'success': False, 'error': 'Unknown JRN file format'}
        
        # Clean empty values
        df = df.fillna('')
        for col in df.columns:
            if col == 'Date':
                df[col] = df[col].apply(lambda x: x if (x and x != '' and x != 'Unknown') else None)
            else:
                df[col] = df[col].apply(lambda x: None if (pd.isna(x) or str(x).strip() == '' or str(x).lower() == 'null') else x)
        
        # Reorder columns to move 'Data' to the end
        df = reorder_columns(df)
        
        return {'success': True, 'dataframe': df}
        
    except Exception as e:
        logger.error(f"Error processing JRN file {filepath}: {str(e)}")
        return {'success': False, 'error': str(e)}

class ExcelProcessor:
    def detectColumnTypes(self, df):
        """Analyze DataFrame columns to determine data types"""
        columnTypes = {}
        
        for col in df.columns:
            sampleData = df[col].dropna().head(100)
            
            if len(sampleData) == 0:
                columnTypes[col] = 'text'
                continue
            
            numericCount = timeCount = timestampCount = 0
            
            for value in sampleData:
                valueStr = str(value).strip()
                
                try:
                    float(valueStr)
                    numericCount += 1
                except (ValueError, TypeError):
                    pass
                
                if self._isTimeOnly(valueStr):
                    timeCount += 1
                elif self._isTimestamp(valueStr, col):
                    timestampCount += 1
            
            total = len(sampleData)
            
            # Enhanced detection logic for timestamps
            if timestampCount > total * 0.6:
                columnTypes[col] = 'timestamp'
            elif timeCount > total * 0.6:
                columnTypes[col] = 'time'
            elif numericCount > total * 0.7:
                columnTypes[col] = 'numeric'
            else:
                columnTypes[col] = 'text'
        
        return columnTypes
    
    def _isTimeOnly(self, value):
        """Check if value is time-only format"""
        if not value or str(value).strip() == '':
            return False
        
        valueStr = str(value).strip()
        timePatterns = [r'^\d{1,2}:\d{2}:\d{2}$', r'^\d{1,2}:\d{2}$']
        
        for pattern in timePatterns:
            if re.match(pattern, valueStr):
                if not re.search(r'\d{4}|/|-|\.|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec', valueStr, re.IGNORECASE):
                    try:
                        parts = valueStr.split(':')
                        hour, minute = int(parts[0]), int(parts[1])
                        second = int(parts[2]) if len(parts) > 2 else 0
                        if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                            return True
                    except (ValueError, IndexError):
                        pass
        return False
    
    def _isTimestamp(self, value, column_name):
        """Check if value is a timestamp (combines date and time)"""
        if not value or str(value).strip() == '':
            return False
        
        valueStr = str(value).strip()
        
        # Skip if it's just a time (handled by _isTimeOnly)
        if self._isTimeOnly(valueStr):
            return False
            
        # Check for common timestamp patterns
        timestamp_patterns = [
            r'\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
            r'\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}',  # MM/DD/YYYY HH:MM:SS
            r'\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}',  # M/D/YYYY HH:MM:SS
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',      # ISO format
        ]
        
        for pattern in timestamp_patterns:
            if re.match(pattern, valueStr):
                try:
                    # Try to parse as datetime
                    pd.to_datetime(valueStr)
                    return True
                except:
                    continue
        
        # Additional check for values that contain both date and time components
        if any(sep in valueStr for sep in ['-', '/', '.']) and ':' in valueStr:
            try:
                pd.to_datetime(valueStr)
                return True
            except:
                pass
                
        return False
    
    def getColumnFilterOptions(self, df, column):
        """Generate filter options based on column type"""
        columnData = df[column].dropna()
        
        # For Date columns in JRN files, filter out empty/invalid values
        if column == 'Date':
            columnData = columnData[columnData != '']
            columnData = columnData[columnData != 'Unknown']
        
        if len(columnData) == 0:
            return {'type': 'text', 'options': []}
        
        columnTypes = self.detectColumnTypes(df)
        colType = columnTypes.get(column, 'text')
        
        if colType == 'timestamp':
            # Handle timestamp columns
            timestamps = []
            for value in columnData:
                try:
                    ts = pd.to_datetime(str(value))
                    timestamps.append(ts)
                except:
                    continue
            
            if timestamps:
                min_timestamp = min(timestamps)
                max_timestamp = max(timestamps)
                return {
                    'type': 'timestamp',
                    'min_timestamp': min_timestamp.isoformat(),
                    'max_timestamp': max_timestamp.isoformat(),
                    'min_date': min_timestamp.date().isoformat(),
                    'max_date': max_timestamp.date().isoformat(),
                    'min_time': min_timestamp.time().strftime('%H:%M:%S'),
                    'max_time': max_timestamp.time().strftime('%H:%M:%S')
                }
        
        elif colType == 'numeric':
            numericValues = []
            for value in columnData:
                try:
                    numericValues.append(float(str(value)))
                except:
                    continue
            
            if numericValues:
                return {'type': 'numeric', 'min_value': min(numericValues), 'max_value': max(numericValues)}
        
        elif colType == 'time':
            times = []
            for value in columnData:
                try:
                    if ':' in str(value) and len(str(value).split()) == 1:
                        time_obj = pd.to_datetime(str(value), format='%H:%M:%S').time()
                    else:
                        time_obj = pd.to_datetime(str(value)).time()
                    times.append(time_obj)
                except:
                    continue
            
            if times:
                min_time = min(times).strftime('%H:%M:%S')
                max_time = max(times).strftime('%H:%M:%S')
                return {'type': 'time', 'min_time': min_time, 'max_time': max_time}
            elif column.lower() == 'date' or colType == 'date':
                dates = []
                for value in columnData:
                    try:
                         if value and value != 'Unknown' and value != '':
                            date_obj = pd.to_datetime(str(value)).date()
                            dates.append(date_obj)
                    except:
                        continue
        
                if dates:
                 min_date = min(dates).isoformat()
                 max_date = max(dates).isoformat()
                 return {'type': 'date', 'min_date': min_date, 'max_date': max_date}
        
        # Default to text with unique values
        uniqueValues = columnData.unique()[:50]
        return {'type': 'text', 'options': [str(val) for val in uniqueValues if pd.notna(val)]}

def allowedFile(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def processExcelFile(filepath, fileId):
    """Process uploaded file and extract data"""
    try:
        # Handle different file types
        if filepath.endswith('.jrn'):
            result = process_jrn_file(filepath, fileId)
            if not result['success']:
                return result
            df = result['dataframe']
        elif filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        elif filepath.endswith('.prn'):
            df = pd.read_csv(filepath, delimiter=None, engine='python', header=None)
        else:
            df = pd.read_excel(filepath)
        
        # Handle NaN values
        df = df.fillna('')
        
        # Reorder columns for non-JRN files
        if not filepath.endswith('.jrn'):
            df = reorder_columns(df)
        
        # Get basic statistics
        totalRows, totalColumns = df.shape
        
        # Store the data
        processedData[fileId] = df
        fileMetadata[fileId] = {
            'filename': os.path.basename(filepath),
            'upload_time': datetime.now().isoformat(),
            'total_rows': totalRows,
            'total_columns': totalColumns
        }
        
        # Get column information
        processor = ExcelProcessor()
        columnsInfo = []
        columnTypes = processor.detectColumnTypes(df)
        
        for col in df.columns:
            filterOptions = processor.getColumnFilterOptions(df, col)
            colInfo = {
                'name': str(col),
                'type': str(df[col].dtype),
                'data_type': columnTypes.get(col, 'text'),
                'non_null_count': int(df[col].count()),
                'unique_count': int(df[col].nunique()),
                'filter_options': filterOptions
            }
            columnsInfo.append(colInfo)
        
        # Convert DataFrame to serializable format
        data = []
        for record in df.to_dict('records'):
            serializableRecord = {}
            for key, value in record.items():
                if pd.isna(value):
                    serializableRecord[key] = None
                elif isinstance(value, (datetime, pd.Timestamp)):
                    serializableRecord[key] = value.isoformat()
                elif isinstance(value, time):
                    serializableRecord[key] = value.strftime('%H:%M:%S')
                elif isinstance(value, (np.integer, np.floating)):
                    serializableRecord[key] = value.item()
                else:
                    serializableRecord[key] = str(value)
            data.append(serializableRecord)
        
        return {
            'success': True,
            'data': data,
            'metadata': {
                'total_rows': totalRows,
                'total_columns': totalColumns,
                'columns_info': columnsInfo,
                'file_size': os.path.getsize(filepath),
                'processed_at': datetime.now().isoformat(),
                'file_id': fileId,
                'filename': os.path.basename(filepath)
            },
            'columns': list(df.columns),
            'file_id': fileId
        }
        
    except Exception as e:
        logger.error(f"Error processing file {filepath}: {str(e)}")
        return {'success': False, 'error': str(e)}

def applyFilters(df, filters):
    """Apply filters to DataFrame including separate date and time filtering"""
    filteredDf = df.copy()
    
    # Check if we have both Date and Timestamp filters (combined datetime filtering)
    date_filter = filters.get('Date', {})
    timestamp_filter = filters.get('Timestamp', {})
    
    has_date_filter = date_filter.get('type') == 'date' and (date_filter.get('start_date') or date_filter.get('end_date'))
    has_time_filter = timestamp_filter.get('type') == 'time' and (timestamp_filter.get('start_time') or timestamp_filter.get('end_time'))
    
    # If we have both date and time filters, combine them into datetime filtering
    if has_date_filter and has_time_filter:
        try:
            start_date = date_filter.get('start_date')
            end_date = date_filter.get('end_date')
            start_time = timestamp_filter.get('start_time', '00:00:00')
            end_time = timestamp_filter.get('end_time', '23:59:59')
            
            # Add seconds if not provided
            if len(start_time.split(':')) == 2:
                start_time += ':00'
            if len(end_time.split(':')) == 2:
                end_time += ':00'
            
            # Create datetime objects for filtering
            if start_date and end_date:
                start_datetime = pd.to_datetime(f"{start_date} {start_time}")
                end_datetime = pd.to_datetime(f"{end_date} {end_time}")
                
                # Combine Date and Timestamp columns into a single datetime
                date_col = pd.to_datetime(filteredDf['Date'], errors='coerce')
                timestamp_col = pd.to_datetime(filteredDf['Timestamp'], format='%H:%M:%S', errors='coerce')
                
                # Create combined datetime
                combined_datetime = pd.to_datetime(
                    date_col.dt.strftime('%Y-%m-%d') + ' ' + timestamp_col.dt.strftime('%H:%M:%S'),
                    errors='coerce'
                )
                
                # Apply datetime range filter
                valid_datetimes = ~combined_datetime.isna()
                datetime_mask = (combined_datetime >= start_datetime) & (combined_datetime <= end_datetime) & valid_datetimes
                filteredDf = filteredDf[datetime_mask]
                
                # Remove Date and Timestamp from further individual processing
                remaining_filters = {k: v for k, v in filters.items() if k not in ['Date', 'Timestamp']}
            else:
                remaining_filters = filters
                
        except Exception as e:
            logger.error(f"Error in combined datetime filtering: {str(e)}")
            remaining_filters = filters
    else:
        remaining_filters = filters
    
    # Process remaining filters individually
    for column, filterConfig in remaining_filters.items():
        if column not in df.columns:
            continue
        
        filterType = filterConfig.get('type')
        
        if filterType == 'text':
            values = filterConfig.get('values', [])
            if values:
                filteredDf = filteredDf[filteredDf[column].astype(str).isin(values)]
        
        elif filterType == 'numeric':
            minVal = filterConfig.get('min')
            maxVal = filterConfig.get('max')
            
            numericCol = pd.to_numeric(filteredDf[column], errors='coerce')
            
            if minVal is not None:
                filteredDf = filteredDf[numericCol >= minVal]
            if maxVal is not None:
                filteredDf = filteredDf[numericCol <= maxVal]
        
        elif filterType == 'date':
            # Handle date-only filtering (when not combined with time)
            startDate = filterConfig.get('start_date')
            endDate = filterConfig.get('end_date')
            
            try:
                if startDate or endDate:
                    # Convert column to datetime, then extract date
                    dateCol = pd.to_datetime(filteredDf[column], errors='coerce')
                    valid_dates = ~dateCol.isna()
                    
                    if startDate:
                        startD = pd.to_datetime(startDate).date()
                        date_mask = (dateCol.dt.date >= startD) & valid_dates
                        filteredDf = filteredDf[date_mask]
                    
                    if endDate:
                        endD = pd.to_datetime(endDate).date()
                        date_mask = (dateCol.dt.date <= endD) & valid_dates
                        filteredDf = filteredDf[date_mask]
                        
            except Exception as e:
                logger.error(f"Error filtering date column {column}: {str(e)}")
                continue
        
        elif filterType == 'time':
            # Handle time-only filtering (when not combined with date)
            startTime = filterConfig.get('start_time')
            endTime = filterConfig.get('end_time')
            
            try:
                if startTime or endTime:
                    # For timestamp columns, extract time component
                    if column.lower() == 'timestamp':
                        # Try to parse as full datetime first, then extract time
                        try:
                            timestampCol = pd.to_datetime(filteredDf[column], errors='coerce')
                            timeCol = timestampCol.dt.time
                            valid_times = ~timestampCol.isna()
                        except:
                            # Fallback to direct time parsing
                            timeCol = pd.to_datetime(filteredDf[column], format='%H:%M:%S', errors='coerce').dt.time
                            valid_times = ~pd.isna(timeCol)
                    else:
                        # For pure time columns
                        timeCol = pd.to_datetime(filteredDf[column], format='%H:%M:%S', errors='coerce').dt.time
                        valid_times = ~pd.isna(timeCol)
                    
                    if startTime:
                        # Parse HH:MM:SS format
                        if len(startTime.split(':')) == 2:
                            startTime += ':00'  # Add seconds if not provided
                        startT = pd.to_datetime(startTime, format='%H:%M:%S').time()
                        time_mask = (timeCol >= startT) & valid_times
                        filteredDf = filteredDf[time_mask]
                    
                    if endTime:
                        # Parse HH:MM:SS format
                        if len(endTime.split(':')) == 2:
                            endTime += ':00'  # Add seconds if not provided
                        endT = pd.to_datetime(endTime, format='%H:%M:%S').time()
                        time_mask = (timeCol <= endT) & valid_times
                        filteredDf = filteredDf[time_mask]
                        
            except Exception as e:
                logger.error(f"Error filtering time column {column}: {str(e)}")
                continue
        
        elif filterType == 'timestamp':
            # Handle full timestamp filtering
            startTimestamp = filterConfig.get('start_timestamp')
            endTimestamp = filterConfig.get('end_timestamp')
            startDate = filterConfig.get('start_date')
            endDate = filterConfig.get('end_date')
            
            try:
                # Convert column to datetime
                timestampCol = pd.to_datetime(filteredDf[column], errors='coerce')
                valid_timestamps = ~timestampCol.isna()
                
                # Apply timestamp range filter (full datetime)
                if startTimestamp:
                    startTs = pd.to_datetime(startTimestamp)
                    timestamp_mask = (timestampCol >= startTs) & valid_timestamps
                    filteredDf = filteredDf[timestamp_mask]
                
                if endTimestamp:
                    endTs = pd.to_datetime(endTimestamp)
                    timestamp_mask = (timestampCol <= endTs) & valid_timestamps
                    filteredDf = filteredDf[timestamp_mask]
                
                # Apply date-only filter (if no full timestamp provided)
                if not startTimestamp and not endTimestamp:
                    if startDate:
                        startD = pd.to_datetime(startDate).date()
                        date_mask = (timestampCol.dt.date >= startD) & valid_timestamps
                        filteredDf = filteredDf[date_mask]
                    
                    if endDate:
                        endD = pd.to_datetime(endDate).date()
                        date_mask = (timestampCol.dt.date <= endD) & valid_timestamps
                        filteredDf = filteredDf[date_mask]
                        
            except Exception as e:
                logger.error(f"Error filtering timestamp column {column}: {str(e)}")
                continue
    
    return filteredDf


# Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def uploadFile():
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file provided'}), 400
        
        file = request.files['file']
        
        if file.filename == '' or not allowedFile(file.filename):
            return jsonify({'success': False, 'error': 'Invalid file type'}), 400
        
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_')
        filename = timestamp + filename
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        file.save(filepath)
        logger.info(f"File saved: {filepath}")
        
        fileId = timestamp + secure_filename(file.filename)
        result = processExcelFile(filepath, fileId)
        
        try:
            os.remove(filepath)
        except:
            pass
        
        return jsonify(result) if result['success'] else (jsonify(result), 500)
            
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/filter', methods=['POST'])
def filterData():
    try:
        requestData = request.get_json()
        fileId = requestData.get('file_id')
        filters = requestData.get('filters', {})
        searchTerm = requestData.get('search', '')
        page = requestData.get('page', 1)
        perPage = requestData.get('per_page', 50)
        
        if not fileId or fileId not in processedData:
            return jsonify({'success': False, 'error': 'File not found'}), 404
        
        df = processedData[fileId]
        
        if filters:
            df = applyFilters(df, filters)
        
        if searchTerm:
            mask = df.astype(str).apply(lambda x: x.str.contains(searchTerm, case=False, na=False)).any(axis=1)
            df = df[mask]
        
        totalRows = len(df)
        startIdx = (page - 1) * perPage
        endIdx = startIdx + perPage
        paginatedDf = df.iloc[startIdx:endIdx]
        
        data = [
            {key: (None if pd.isna(value) else str(value)) for key, value in record.items()}
            for record in paginatedDf.to_dict('records')
        ]
        
        return jsonify({
            'success': True,
            'data': data,
            'total_rows': totalRows,
            'page': page,
            'per_page': perPage,
            'total_pages': (totalRows + perPage - 1) // perPage
        })
        
    except Exception as e:
        logger.error(f"Filter error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/upload-multiple', methods=['POST'])
def upload_multiple_files():
    try:
        files = request.files.getlist('files')
        if not files:
            return jsonify({'success': False, 'error': 'No files provided'})
        
        # Process and combine all files
        combined_df = None
        total_file_size = 0
        file_names = []
        
        for file in files:
            if not file.filename or not allowedFile(file.filename):
                continue
                
            file_names.append(file.filename)
            
            # Get file size
            file.seek(0, 2)  # Seek to end
            file_size = file.tell()
            file.seek(0)  # Seek back to beginning
            total_file_size += file_size
            
            # Save file temporarily for processing
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_')
            temp_filename = timestamp + filename
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)
            
            file.save(filepath)
            
            try:
                # Process file using existing logic
                if filepath.endswith('.jrn'):
                    result = process_jrn_file(filepath, temp_filename)
                    if not result['success']:
                        logger.error(f"Failed to process JRN file {filename}: {result['error']}")
                        continue
                    df = result['dataframe']
                elif filepath.endswith('.csv'):
                    df = pd.read_csv(filepath)
                elif filepath.endswith('.prn'):
                    df = pd.read_csv(filepath, delimiter=None, engine='python', header=None)
                else:
                    df = pd.read_excel(filepath)
                
                # Handle NaN values
                df = df.fillna('')
                
                # Reorder columns (move Data column to end)
                df = reorder_columns(df)
                
                # Append to combined dataframe
                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                
            except Exception as e:
                logger.error(f"Error processing file {filename}: {str(e)}")
                continue
            finally:
                # Clean up temporary file
                try:
                    os.remove(filepath)
                except:
                    pass
        
        if combined_df is None:
            return jsonify({'success': False, 'error': 'No valid files processed'})
        
        # Generate unique file ID and store
        file_id = datetime.now().strftime('%Y%m%d_%H%M%S_') + 'combined'
        
        # Store the combined dataframe
        processedData[file_id] = combined_df
        
        # Generate metadata using existing ExcelProcessor
        processor = ExcelProcessor()
        columnsInfo = []
        columnTypes = processor.detectColumnTypes(combined_df)
        
        for col in combined_df.columns:
            filterOptions = processor.getColumnFilterOptions(combined_df, col)
            colInfo = {
                'name': str(col),
                'type': str(combined_df[col].dtype),
                'data_type': columnTypes.get(col, 'text'),
                'non_null_count': int(combined_df[col].count()),
                'unique_count': int(combined_df[col].nunique()),
                'filter_options': filterOptions
            }
            columnsInfo.append(colInfo)
        
        # Store file metadata
        fileMetadata[file_id] = {
            'filename': f"Combined_{len(file_names)}_files",
            'upload_time': datetime.now().isoformat(),
            'total_rows': len(combined_df),
            'total_columns': len(combined_df.columns),
            'source_files': file_names
        }
        
        # Convert DataFrame to serializable format
        data = []
        for record in combined_df.to_dict('records'):
            serializableRecord = {}
            for key, value in record.items():
                if pd.isna(value):
                    serializableRecord[key] = None
                elif isinstance(value, (datetime, pd.Timestamp)):
                    serializableRecord[key] = value.isoformat()
                elif isinstance(value, time):
                    serializableRecord[key] = value.strftime('%H:%M:%S')
                elif isinstance(value, (np.integer, np.floating)):
                    serializableRecord[key] = value.item()
                else:
                    serializableRecord[key] = str(value)
            data.append(serializableRecord)
        
        metadata = {
            'total_rows': len(combined_df),
            'total_columns': len(combined_df.columns),
            'columns_info': columnsInfo,
            'file_size': total_file_size,
            'processed_at': datetime.now().isoformat(),
            'file_id': file_id,
            'filename': f"Combined_{len(file_names)}_files",
            'source_files': file_names
        }
        
        return jsonify({
            'success': True,
            'data': data,
            'metadata': metadata,
            'columns': list(combined_df.columns),
            'file_id': file_id,
            'file_names': file_names
        })
        
    except Exception as e:
        logger.error(f"Multiple upload error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/export', methods=['POST'])
def exportData():
    try:
        requestData = request.get_json()
        fileId = requestData.get('file_id')
        filters = requestData.get('filters', {})
        searchTerm = requestData.get('search', '')
        
        if not fileId or fileId not in processedData:
            return jsonify({'success': False, 'error': 'File not found'}), 404
        
        df = processedData[fileId]
        
        if filters:
            df = applyFilters(df, filters)
        
        if searchTerm:
            mask = df.astype(str).apply(lambda x: x.str.contains(searchTerm, case=False, na=False)).any(axis=1)
            df = df[mask]
        
        csvData = df.to_csv(index=False)
        return jsonify({
            'success': True,
            'data': csvData,
            'filename': f'filtered_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        })
        
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/health')
def healthCheck():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
