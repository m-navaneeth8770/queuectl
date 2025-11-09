import sqlite3
from flask import Flask, render_template, render_template_string
from db import (
    get_status_summary, 
    get_metrics, 
    list_jobs_by_state, 
    get_db_connection
)
from models import JobState


app = Flask(__name__)


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>QueueCTL Dashboard</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 40px; background: #f9f9f9; }
        .container { max-width: 1200px; margin: 0 auto; background: #fff; border: 1px solid #ddd; border-radius: 8px; padding: 20px; }
        h1, h2 { border-bottom: 2px solid #eee; padding-bottom: 10px; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .card { background: #fafafa; border: 1px solid #eee; border-radius: 5px; padding: 15px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }
        th { background: #f4f4f4; }
        .state { padding: 3px 8px; border-radius: 12px; font-weight: bold; }
        .state-pending { background: #e0e0e0; }
        .state-completed { background: #d4edda; color: #155724; }
        .state-dead { background: #f8d7da; color: #721c24; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 QueueCTL Dashboard</h1>
        
        <div class="grid">
            <div class="card">
                <h2>📊 Job Status</h2>
                <table>
                {% for state in summary %}
                    <tr>
                        <th>{{ state.upper() }}</th>
                        <td>{{ summary[state] }}</td>
                    </tr>
                {% else %}
                    <tr><td>No jobs found.</td></tr>
                {% endfor %}
                </table>
            </div>
            <div class="card">
                <h2>📈 Execution Metrics</h2>
                <table>
                {% for key, value in metrics.items() %}
                    <tr>
                        <th>{{ key.replace('_', ' ')|title }}</th>
                        <td>{{ value }}</td>
                    </tr>
                {% else %}
                    <tr><td>No metrics found.</td></tr>
                {% endfor %}
                </table>
            </div>
        </div>

        <h2>Pending Jobs</h2>
        <div class="card">
            <table>
                <tr><th>ID</th><th>Command</th><th>Priority</th><th>Run At</th></tr>
                {% for job in pending_jobs %}
                <tr>
                    <td>{{ job['id'] }}</td>
                    <td><code>{{ job['command'] }}</code></td>
                    <td>{{ job['priority'] }}</td>
                    <td>{{ job['run_at'] }}</td>
                </tr>
                {% else %}
                <tr><td colspan="4">No pending jobs.</td></tr>
                {% endfor %}
            </table>
        </div>

        <h2>Dead Letter Queue (DLQ)</h2>
        <div class="card">
            <table>
                <tr><th>ID</th><th>Command</th><th>Error</th><th>Attempts</th></tr>
                {% for job in dead_jobs %}
                <tr>
                    <td>{{ job['id'] }}</td>
                    <td><code>{{ job['command'] }}</code></td>
                    <td><code>{{ job['error'] }}</code></td>
                    <td>{{ job['attempts'] }}</td>
                </tr>
                {% else %}
                <tr><td colspan="4">No jobs in DLQ.</td></tr>
                {% endfor %}
            </table>
        </div>
    </div>
</body>
</html>
"""

@app.route('/')
def dashboard():
    """Main dashboard page."""
    try:
        summary = get_status_summary()
        metrics = get_metrics()
        pending_jobs = list_jobs_by_state(JobState.PENDING)
        dead_jobs = list_jobs_by_state(JobState.DEAD)
        
        # Ensure summary displays all states
        full_summary = {s.value: 0 for s in JobState}
        if summary:
            full_summary.update(summary)
        
        return render_template_string(
            HTML_TEMPLATE,
            summary=full_summary,
            metrics=metrics or {},
            pending_jobs=pending_jobs or [],
            dead_jobs=dead_jobs or []
        )
    except sqlite3.Error as e:
        return f"Database error: {e}. <br/>Have you run 'python queuectl.py initdb'?", 500

def run_dashboard():
    """Starts the Flask web server."""
    print("Starting QueueCTL Dashboard...")
    print("View at: http://127.0.0.1:5000")
    app.run(debug=True, host='127.0.0.1', port=5000)
    
if __name__ == "__main__":
    run_dashboard()
