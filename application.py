from flask import Flask, render_template, Response, jsonify
import boto3
import json
import time
from datetime import datetime
from decimal import Decimal
from collections import defaultdict

application = Flask(__name__)

app = application

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('dynamodb-cold-storage-x23337818')

# Convert Decimal → float/int
def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    try:
        response = table.scan()
        data = response.get('Items', [])
        if not data:
            return jsonify({'error': 'No data available', 'data': []})
        data = sorted(data, key=lambda x: x.get('timestamp', ''), reverse=True)
        return jsonify(convert_decimals(data[:50]))  # Return last 50 records
    except Exception as e:
        return jsonify({'error': str(e), 'data': []})

@app.route('/api/latest')
def get_latest():
    """Get latest reading for each device"""
    try:
        response = table.scan()
        data = response.get('Items', [])
        
        if not data:
            return jsonify({'error': 'No data available', 'devices': []})
        
        # Group by device and get latest
        devices = {}
        for item in data:
            device_id = item.get('device_id')
            timestamp = item.get('timestamp', '')
            if device_id not in devices or timestamp > devices[device_id].get('timestamp', ''):
                devices[device_id] = item
        
        return jsonify(convert_decimals(list(devices.values())))
    except Exception as e:
        return jsonify({'error': str(e), 'devices': []})

@app.route('/api/timeseries')
def get_timeseries():
    """Get time series data for graphs"""
    try:
        response = table.scan()
        data = response.get('Items', [])
        
        if not data:
            return jsonify({'error': 'No data available', 'timestamps': [], 'temperatures': [], 
                          'humidities': [], 'co2_levels': [], 'power_usages': []})
        
        # Sort by timestamp
        data = sorted(data, key=lambda x: x.get('timestamp', ''))
        
        # Get last 100 records for graphs
        data = data[-100:]
        
        timeseries = {
            'timestamps': [],
            'temperatures': [],
            'humidities': [],
            'co2_levels': [],
            'power_usages': []
        }
        
        for item in data:
            if 'timestamp' in item:
                # Format timestamp for display
                ts = item['timestamp']
                if isinstance(ts, str):
                    try:
                        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                        timeseries['timestamps'].append(dt.strftime('%H:%M:%S'))
                    except:
                        timeseries['timestamps'].append(ts[-8:])  # Last 8 chars as time
                else:
                    timeseries['timestamps'].append(str(ts))
            
            timeseries['temperatures'].append(float(item.get('temperature', 0)))
            timeseries['humidities'].append(float(item.get('humidity', 0)))
            timeseries['co2_levels'].append(float(item.get('co2_level', 0)))
            timeseries['power_usages'].append(float(item.get('power_usage', 0)))
        
        return jsonify(convert_decimals(timeseries))
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/stream')
def stream():
    def event_stream():
        last_hash = None
        while True:
            try:
                response = table.scan()
                data = response.get('Items', [])
                
                if not data:
                    # Send empty data indicator
                    payload = {
                        'data': [],
                        'stats': {
                            'avg_temperature': 0,
                            'max_temperature': 0,
                            'min_temperature': 0,
                            'avg_humidity': 0,
                            'avg_co2': 0,
                            'avg_power': 0,
                            'unique_devices': 0,
                            'alerts': 0,
                            'no_data': True
                        },
                        'timeseries': [],
                        'no_data': True
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                    time.sleep(2)
                    continue
                
                data = sorted(data, key=lambda x: x.get('timestamp', ''), reverse=True)[:50]
                data = convert_decimals(data)
                
                # Get latest stats
                temps = [float(item.get('temperature', 0)) for item in data if 'temperature' in item]
                hums = [float(item.get('humidity', 0)) for item in data if 'humidity' in item]
                co2s = [float(item.get('co2_level', 0)) for item in data if 'co2_level' in item]
                powers = [float(item.get('power_usage', 0)) for item in data if 'power_usage' in item]
                
                stats = {
                    'avg_temperature': round(sum(temps)/len(temps),1) if temps else 0,
                    'max_temperature': max(temps) if temps else 0,
                    'min_temperature': min(temps) if temps else 0,
                    'avg_humidity': round(sum(hums)/len(hums),1) if hums else 0,
                    'avg_co2': round(sum(co2s)/len(co2s),1) if co2s else 0,
                    'avg_power': round(sum(powers)/len(powers),1) if powers else 0,
                    'unique_devices': len(set(item.get('device_id') for item in data)),
                    'alerts': sum(1 for item in data if item.get('temp_status') == 'HIGH' or 
                                 item.get('co2_status') == 'HIGH' or 
                                 item.get('power_status') == 'HIGH')
                }
                
                # Get timeseries data for graphs
                timeseries_data = []
                sorted_data = sorted(data, key=lambda x: x.get('timestamp', ''))
                for item in sorted_data[-30:]:  # Last 30 points for smooth graph
                    timeseries_data.append({
                        'timestamp': item.get('timestamp', ''),
                        'temperature': float(item.get('temperature', 0)),
                        'humidity': float(item.get('humidity', 0)),
                        'co2_level': float(item.get('co2_level', 0)),
                        'power_usage': float(item.get('power_usage', 0))
                    })
                
                payload = {
                    'data': data[:20],  # Latest 20 records for table
                    'stats': stats,
                    'timeseries': timeseries_data,
                    'no_data': False
                }
                
                current_hash = hash(json.dumps(payload, sort_keys=True))
                if current_hash != last_hash:
                    last_hash = current_hash
                    yield f"data: {json.dumps(payload)}\n\n"
                
                time.sleep(2)
                
            except Exception as e:
                print(f"Stream error: {e}")
                error_payload = {
                    'error': str(e),
                    'no_data': True
                }
                yield f"data: {json.dumps(error_payload)}\n\n"
                time.sleep(5)
    
    return Response(event_stream(), mimetype="text/event-stream")

@app.route('/api/stats')
def get_stats():
    try:
        response = table.scan()
        data = response.get('Items', [])
        
        if not data:
            return jsonify({
                'error': 'No data available',
                'avg_temperature': 0,
                'max_temperature': 0,
                'min_temperature': 0,
                'avg_humidity': 0,
                'avg_co2': 0,
                'avg_power': 0,
                'unique_devices': 0,
                'total_readings': 0,
                'no_data': True
            })
        
        temps, hums, co2s, powers = [], [], [], []
        
        for item in data:
            if 'temperature' in item:
                temps.append(float(item['temperature']))
            if 'humidity' in item:
                hums.append(float(item['humidity']))
            if 'co2_level' in item:
                co2s.append(float(item['co2_level']))
            if 'power_usage' in item:
                powers.append(float(item['power_usage']))
        
        stats = {
            'avg_temperature': round(sum(temps)/len(temps),1) if temps else 0,
            'max_temperature': max(temps) if temps else 0,
            'min_temperature': min(temps) if temps else 0,
            'avg_humidity': round(sum(hums)/len(hums),1) if hums else 0,
            'avg_co2': round(sum(co2s)/len(co2s),1) if co2s else 0,
            'avg_power': round(sum(powers)/len(powers),1) if powers else 0,
            'unique_devices': len(set(item.get('device_id') for item in data)),
            'total_readings': len(data),
            'no_data': False
        }
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e), 'no_data': True})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True, threaded=True)