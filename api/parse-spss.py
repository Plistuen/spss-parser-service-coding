from http.server import BaseHTTPRequestHandler
import json
import pyreadstat
import pandas as pd
import numpy as np
import tempfile
import os
import gc
import traceback

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            # Get file data
            content_length = int(self.headers['Content-Length'])
            file_data = self.rfile.read(content_length)
            
            # Save to temp file
            with tempfile.NamedTemporaryFile(suffix='.sav', delete=False) as tmp:
                tmp.write(file_data)
                tmp_path = tmp.name
            
            # Parse SPSS file with optimizations for large files
            df, meta = pyreadstat.read_sav(
                tmp_path, 
                apply_value_formats=False,
                usecols=None  # Read all columns but more efficiently
            )
            
            # Memory optimization for large datasets
            if len(df) > 10000:
                gc.collect()
            
            # Build response (same format as your local parsers)
            result = {
                'success': True,
                'metadata': {
                    'caseCount': len(df),
                    'variableCount': len(df.columns),
                    'variables': [],
                    'openTextQuestions': [],
                    'timingVariables': 0,
                    'hasDateVars': False
                },
                'data': []
            }
            
            # Process variables
            for col in df.columns:
                is_string = str(df[col].dtype) == 'object'
                var_info = {
                    'name': col,
                    'label': meta.column_names_to_labels.get(col, col),
                    'type': 'string' if is_string else 'numeric',
                    'valueLabels': meta.variable_value_labels.get(col, {})
                }
                result['metadata']['variables'].append(var_info)
                
                # Check for timing variables
                if 'qtime' in col.lower():
                    result['metadata']['timingVariables'] += 1
                
                # Check for date variables
                if any(t in col.lower() for t in ['date', 'dato', 'tid', 'time']):
                    result['metadata']['hasDateVars'] = True
                
                # Find open text questions
                if is_string and len(var_info['valueLabels']) == 0:
                    technical = ['id', 'respondent', 'starttid', 'sluttid', 'respid', 'panel', 'weight']
                    if not any(t in col.lower() for t in technical):
                        samples = df[col].dropna().head(10).tolist()
                        result['metadata']['openTextQuestions'].append({
                            'variableName': col,
                            'questionText': var_info['label'],
                            'sampleResponses': samples,
                            'uniqueResponses': int(df[col].nunique()),
                            'totalResponses': int(df[col].notna().sum())
                        })
            
            # Add sample data (first 1000 rows for Pro version)
            sample_size = min(1000, len(df))
            for idx in range(sample_size):
                row = df.iloc[idx]
                record = {}
                for col in df.columns:
                    val = row[col]
                    if pd.isna(val):
                        record[col] = None
                    else:
                        record[col] = val if isinstance(val, str) else float(val)
                result['data'].append(record)
            
            # Clean up
            os.unlink(tmp_path)
            
            # Send response
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())
            
        except Exception as e:
            # Enhanced error reporting for debugging
            error_details = {
                'success': False,
                'error': str(e),
                'type': type(e).__name__
            }
            
            # Add stack trace in development
            if os.environ.get('DEBUG'):
                error_details['traceback'] = traceback.format_exc()
            
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(error_details).encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()