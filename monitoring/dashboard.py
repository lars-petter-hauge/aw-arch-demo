"""Web Dashboard for AW Arch Demo

Streamlit-based real-time monitoring dashboard.
Separated from main application for clean architecture.
Mobile-friendly responsive design.
"""

import asyncio
import os
import logging
from typing import Dict, Any

import streamlit as st
import requests
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration - Mobile friendly
st.set_page_config(
    page_title="AW Arch Demo - Monitoring",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# API URL
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Custom CSS for mobile responsiveness
st.markdown(
    """
    <style>
    /* Mobile-first responsive design */
    @media (max-width: 768px) {
        .metric-card {
            margin-bottom: 1rem;
        }
        .plotly-container {
            margin: 0 auto;
        }
        button {
            width: 100%;
        }
    }
    
    /* Improve touch targets on mobile */
    @media (max-width: 600px) {
        button {
            padding: 1rem;
            font-size: 1.1rem;
            min-height: 44px;
        }
        input, select, textarea {
            font-size: 16px;
            min-height: 44px;
        }
    }
    
    /* Dashboard styling */
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 0.5rem;
    }
    
    .queue-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 0.75rem;
        text-align: center;
        margin-bottom: 1rem;
    }
    
    .queue-card h3 {
        margin: 0;
        font-size: 0.9rem;
        opacity: 0.9;
    }
    
    .queue-card .value {
        font-size: 2rem;
        font-weight: bold;
        margin: 0.5rem 0;
    }
    
    /* Status indicator */
    .status-ready {
        color: #28a745;
        font-weight: bold;
    }
    
    .status-running {
        color: #ffc107;
        font-weight: bold;
    }
    
    .status-error {
        color: #dc3545;
        font-weight: bold;
    }
    
    /* Progress bar styling */
    .progress-section {
        margin: 2rem 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def get_metrics() -> Dict[str, Any]:
    """Fetch metrics from API."""
    try:
        response = requests.get(f"{API_URL}/metrics", timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch metrics: {e}")
    return None


def get_pipeline_status(pipeline_id: str) -> Dict[str, Any]:
    """Fetch pipeline status from API."""
    try:
        response = requests.get(f"{API_URL}/pipeline/{pipeline_id}", timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch pipeline status: {e}")
    return None


def render_header():
    """Render dashboard header - Mobile friendly."""
    st.title("📊 AW Arch Demo - Monitoring")
    
    # Responsive header info
    col1, col2 = st.columns([2, 1])
    
    with col1:
        with st.expander("ℹ️ API Status"):
            st.code(API_URL, language="text")
    
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()
    
    st.divider()


def render_queue_metrics():
    """Render queue depth metrics - Mobile optimized."""
    st.subheader("📬 Message Queues")

    metrics = get_metrics()

    if metrics and "queues" in metrics:
        queues = metrics["queues"]

        # Mobile-friendly queue cards (stacked on mobile)
        col1, col2, col3 = st.columns(3)
        
        queue_items = list(queues.items())
        
        # Queue A
        if len(queue_items) > 0:
            with col1:
                queue_name, depth = queue_items[0]
                st.metric(
                    label=queue_name.replace("jobs.", "").upper(),
                    value=f"{depth}",
                    label_visibility="visible"
                )
        
        # Queue B
        if len(queue_items) > 1:
            with col2:
                queue_name, depth = queue_items[1]
                st.metric(
                    label=queue_name.replace("jobs.", "").upper(),
                    value=f"{depth}",
                    label_visibility="visible"
                )
        
        # Queue C
        if len(queue_items) > 2:
            with col3:
                queue_name, depth = queue_items[2]
                st.metric(
                    label=queue_name.replace("jobs.", "").upper(),
                    value=f"{depth}",
                    label_visibility="visible"
                )

        # Total jobs
        total_jobs = metrics.get("total_jobs_waiting", 0)
        st.metric("Total Jobs Waiting", f"{total_jobs}", label_visibility="visible")

        # Chart - Responsive
        if queues:
            chart_data = pd.DataFrame(
                {
                    "Queue": [q.replace("jobs.", "").upper() for q in queues.keys()],
                    "Jobs Waiting": list(queues.values()),
                }
            )

            fig = px.bar(
                chart_data,
                x="Queue",
                y="Jobs Waiting",
                title="Queue Depth by Worker",
                color="Queue",
                text="Jobs Waiting",
            )
            fig.update_layout(
                height=400,
                margin=dict(l=10, r=10, t=40, b=10),
                hovermode="x unified"
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ Unable to fetch metrics from API")


def render_pipeline_section():
    """Render pipeline submission and tracking - Mobile optimized."""
    st.subheader("🚀 Submit Pipeline")

    # Mobile-friendly form layout
    with st.form("pipeline_form"):
        models = st.multiselect(
            "Select Models",
            ["worker_a", "worker_b", "worker_c"],
            default=["worker_a", "worker_b"],
        )

        simulations = st.number_input(
            "Number of Simulations",
            min_value=1,
            max_value=1000,
            value=10,
            step=1,
        )
        
        submit_button = st.form_submit_button("▶️ Submit Pipeline", use_container_width=True)
        
        if submit_button:
            if not models:
                st.error("Please select at least one model")
            else:
                try:
                    payload = {"models": models, "simulations": simulations}
                    response = requests.post(
                        f"{API_URL}/pipeline", json=payload, timeout=10
                    )
                    if response.status_code == 200:
                        result = response.json()
                        pipeline_id = result.get("pipeline_id")
                        st.success(f"✅ Pipeline submitted!")
                        st.info(f"**Pipeline ID:** `{pipeline_id}`")
                        # Store in session state for easy access
                        st.session_state.last_pipeline_id = pipeline_id
                    else:
                        st.error(f"Failed to submit pipeline: {response.status_code}")
                except Exception as e:
                    st.error(f"Error submitting pipeline: {e}")

    st.divider()

    st.subheader("📋 Track Pipeline")
    
    # Use last pipeline ID if available
    default_id = st.session_state.get("last_pipeline_id", "")
    
    pipeline_id = st.text_input(
        "Enter Pipeline ID",
        value=default_id,
        placeholder="Enter pipeline ID to track progress"
    )

    if pipeline_id:
        status = get_pipeline_status(pipeline_id)
        if status and status.get("status") != "error":
            # Status cards - Mobile responsive
            st.markdown("### Status Overview")
            
            col1, col2, col3 = st.columns(3)

            with col1:
                status_text = status.get("status", "Unknown").upper()
                status_class = f"status-{status.get('status', 'unknown').lower()}"
                st.metric("Status", status_text)

            with col2:
                st.metric(
                    "Total Simulations",
                    status.get("total_simulations", 0),
                )

            with col3:
                completed = status.get("completed_simulations", 0)
                total = status.get("total_simulations", 1)
                progress = (completed / total * 100) if total > 0 else 0
                st.metric("Progress", f"{progress:.1f}%")

            # Progress bar
            st.markdown("### Progress")
            completed = status.get("completed_simulations", 0)
            total = status.get("total_simulations", 1)
            if total > 0:
                st.progress(completed / total)
                st.caption(f"{completed}/{total} simulations completed")

            # Models
            if "models" in status:
                st.markdown("### Configuration")
                st.write(f"**Models:** {', '.join(status['models'])}")

            # Results (if any)
            if "results" in status and status["results"]:
                st.markdown("### Results")
                results = status["results"]
                
                # Show summary
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Results Available", len(results))
                
                # Expandable results section
                with st.expander(f"📊 View {len(results)} simulation results"):
                    # Limit display on mobile
                    display_count = min(5, len(results))
                    st.caption(f"Showing first {display_count} of {len(results)} results")
                    
                    for sim_key, sim_result in list(results.items())[:display_count]:
                        with st.expander(f"ℹ️ {sim_key}", expanded=False):
                            if isinstance(sim_result, dict):
                                st.json(sim_result)
                            else:
                                st.write(sim_result)

        else:
            st.warning("⚠️ Pipeline not found or has an error")


def render_info_section():
    """Render information section - Mobile friendly."""
    st.divider()
    st.subheader("ℹ️ Information")

    with st.expander("🔗 API & Service Links", expanded=False):
        st.write("**API Endpoint:**")
        st.code(API_URL, language="text")
        
        st.write("**RabbitMQ Management:**")
        st.code("http://localhost:15672\nUsername: guest\nPassword: guest", language="text")

    with st.expander("💻 Example Commands", expanded=False):
        st.code(
            """# Health check
curl http://localhost:8000/health

# Submit pipeline (1 simulation)
curl -X POST http://localhost:8000/pipeline \\
  -H 'Content-Type: application/json' \\
  -d '{"models": ["worker_a", "worker_b"], "simulations": 1}'

# Submit pipeline (100 simulations)
curl -X POST http://localhost:8000/pipeline \\
  -H 'Content-Type: application/json' \\
  -d '{"models": ["worker_a", "worker_b", "worker_c"], "simulations": 100}'

# Get metrics
curl http://localhost:8000/metrics
""",
            language="bash",
        )
    
    with st.expander("📱 Mobile Tips", expanded=False):
        st.write("""
        - **Landscape mode** works best for charts
        - **Tap on metrics** to see detailed information
        - Use **expandable sections** to manage screen space
        - Results are **limited to 5** on mobile for better performance
        """)


def main():
    """Main dashboard function."""
    # Initialize session state
    if "last_pipeline_id" not in st.session_state:
        st.session_state.last_pipeline_id = ""
    
    render_header()
    render_queue_metrics()
    render_pipeline_section()
    render_info_section()

    # Footer
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Auto-refresh every 5 seconds (optional - comment out if too aggressive)
    # st.markdown("""
    # <script>
    # setTimeout(function() {
    #     window.location.reload();
    # }, 5000);
    # </script>
    # """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
