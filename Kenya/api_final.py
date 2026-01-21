from flask import Flask, jsonify, render_template, Response
import asyncio
import threading
import json
import os
import time
import queue
from datetime import datetime
from kenya_final import js_interaction # This is the key line

app = Flask(__name__)

# Global queue for log messages
log_queue = queue.Queue()
scraping_active = False
scraping_lock = threading.Lock()

def log_message(message, level="info"):
    """Add a log message to the queue with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = {
        "timestamp": timestamp,
        "message": message,
        "level": level
    }
    log_queue.put(log_entry)

# Route to serve the HTML file
@app.route('/')
def index():
    return render_template('index.html')

def run_in_new_loop(loop, coro):
    """
    Function to run an asyncio coroutine in a separate thread's event loop.
    """
    global scraping_active
    try:
        log_message("Starting scraper initialization...", "info")
        asyncio.set_event_loop(loop)
        
        with scraping_lock:
            scraping_active = True

        log_message("Scraper is now running...", "info")
        scraping_active = True
        
        
        # Run the actual scraping
        loop.run_until_complete(coro)
        
        log_message("Scraping completed successfully!", "success")
        log_message("Data has been saved to knbs_files.json", "info")
        
    except Exception as e:
        log_message(f"Scraping failed: {str(e)}", "error")
    finally:
        with scraping_lock:
            scraping_active = False
        log_message("Scraper process ended.", "info")

@app.route('/run-crawl', methods=['GET'])
def run_crawl():
    """
    API endpoint to trigger the web crawling script.
    """
    global scraping_active
    
    with scraping_lock:
        if scraping_active:
            return jsonify({"error": "Scraper is already running. Please wait for it to complete."}), 409
    
    try:
        # Clear previous logs
        while not log_queue.empty():
            log_queue.get()
            
        log_message("Scraper request received", "info")
        
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=run_in_new_loop, args=(loop, js_interaction()))
        thread.daemon = True  # Dies when main thread dies
        thread.start()
        
        return jsonify({"message": "Web crawling script has been started in the background."}), 202
    except Exception as e:
        log_message(f"Failed to start scraper: {str(e)}", "error")
        return jsonify({"error": str(e)}), 500

@app.route('/get-data', methods=['GET'])
def get_data():
    """
    API endpoint to retrieve the latest crawled data from the knbs_files.json file.
    """
    file_path = 'knbs_files.json'
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            log_message(f"Data retrieved successfully: {len(data)} records", "success")
            return jsonify(data), 200
        except Exception as e:
            log_message(f"Error reading data file: {str(e)}", "error")
            return jsonify({"error": f"Error reading data file: {str(e)}"}), 500
    else:
        log_message("Data file not found", "warning")
        return jsonify({"error": "Data file not found. Please run the script first."}), 404

@app.route('/logs')
def stream_logs():
    """
    Server-Sent Events endpoint for streaming logs in real-time
    """
    def generate():
        # Send initial connection message
        yield f"data: {json.dumps({'timestamp': datetime.now().strftime('%H:%M:%S'), 'message': 'Connected to log stream', 'level': 'info'})}\n\n"
        
        while True:
            try:
                # Get log message from queue (blocks until available)
                log_entry = log_queue.get(timeout=10)
                yield f"data: {json.dumps(log_entry)}\n\n"
            except queue.Empty:
                # Send heartbeat to keep connection alive
                yield f"data: {json.dumps({'heartbeat': True})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'timestamp': datetime.now().strftime('%H:%M:%S'), 'message': f'Log stream error: {str(e)}', 'level': 'error'})}\n\n"
                break
    
    return Response(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*'
    })

@app.route('/scraper-status')
def scraper_status():
    """
    Get current scraper status
    """
    with scraping_lock:
        return jsonify({"active": scraping_active})

if __name__ == '__main__':
    app.run(debug=True, threaded=True)
