"""Web Dashboard for AW Arch Demo

Streamlit-based real-time monitoring dashboard.
Separated from main application for clean architecture.
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

# Page configuration
st.set_page_config(
    page_title="AW Arch Demo - Monitoring",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# API URL
API_URL = os.getenv("API_URL", "http://localhost:8000")


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
    """Render dashboard header."""
    col1, col2, col3 = st.columns([2, 2, 1])

    with col1:
        st.title("📊 AW Arch Demo - Monitoring")

    with col2:
        st.metric("API Endpoint", API_URL)

    with col3:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()

    st.divider()


def render_queue_metrics():
    """Render queue depth metrics."""
    st.subheader("📨 Message Queues")

    metrics = get_metrics()

    if metrics and "queues" in metrics:
        queues = metrics["queues"]

        # Create columns for queue cards
        cols = st.columns(3)
        for idx, (queue_name, depth) in enumerate(queues.items()):
            with cols[idx]:
                st.metric(
                    label=queue_name.replace("jobs.", "").upper(),
                    value=f"{depth} jobs",
                    delta=None,
                )

        # Total jobs
        total_jobs = metrics.get("total_jobs_waiting", 0)
        st.metric("Total Jobs Waiting", f"{total_jobs} jobs")

        # Chart
        if queues:
            chart_data = pd.DataFrame(
                {
                    "Queue": [q.replace("jobs.", "") for q in queues.keys()],
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
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Unable to fetch metrics from API")


def render_pipeline_section():
    """Render pipeline submission and tracking section."""
    st.subheader("🚀 Submit Pipeline")

    col1, col2 = st.columns(2)

    with col1:
        models = st.multiselect(
            "Select Models",
            ["worker_a", "worker_b", "worker_c"],
            default=["worker_a", "worker_b"],
        )

    with col2:
        simulations = st.number_input(
            "Number of Simulations",
            min_value=1,
            max_value=1000,
            value=10,
            step=1,
        )

    if st.button("▶️ Submit Pipeline", use_container_width=True):
        try:
            payload = {"models": models, "simulations": simulations}
            response = requests.post(
                f"{API_URL}/pipeline", json=payload, timeout=10
            )
            if response.status_code == 200:
                result = response.json()
                pipeline_id = result.get("pipeline_id")
                st.success(f"✅ Pipeline submitted: `{pipeline_id}`")
                st.info(f"Use this ID to track progress: `{pipeline_id}`")
            else:
                st.error(f"Failed to submit pipeline: {response.status_code}")
        except Exception as e:
            st.error(f"Error submitting pipeline: {e}")

    st.divider()

    st.subheader("📋 Track Pipeline")
    pipeline_id = st.text_input(
        "Enter Pipeline ID", placeholder="Enter pipeline ID to track progress"
    )

    if pipeline_id:
        status = get_pipeline_status(pipeline_id)
        if status and status.get("status") != "error":
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Status", status.get("status", "Unknown").upper())

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
            completed = status.get("completed_simulations", 0)
            total = status.get("total_simulations", 1)
            if total > 0:
                st.progress(completed / total)

            # Models
            if "models" in status:
                st.write(f"**Models:** {', '.join(status['models'])}")

            # Results (if any)
            if "results" in status and status["results"]:
                st.subheader("Results")
                results = status["results"]
                with st.expander(f"Show {len(results)} simulation results"):
                    for sim_key, sim_result in list(results.items())[:5]:
                        st.write(f"**{sim_key}:**")
                        if isinstance(sim_result, dict):
                            st.json(sim_result)
                        else:
                            st.write(sim_result)

        else:
            st.warning("Pipeline not found or has an error")


def render_info_section():
    """Render information section."""
    st.divider()
    st.subheader("ℹ️ Information")

    col1, col2 = st.columns(2)

    with col1:
        st.write("**API Endpoint:**")
        st.code(API_URL)

    with col2:
        st.write("**RabbitMQ Management:**")
        st.code("http://localhost:15672 (guest/guest)")

    st.write("\n**Example cURL Commands:**")
    with st.expander("View cURL examples"):
        st.code(
            """# Health check
curl http://localhost:8000/health

# Submit pipeline
curl -X POST http://localhost:8000/pipeline \\
  -H 'Content-Type: application/json' \\
  -d '{"models": ["worker_a", "worker_b"], "simulations": 10}'

# Get metrics
curl http://localhost:8000/metrics
""",
            language="bash",
        )


def main():
    """Main dashboard function."""
    render_header()
    render_queue_metrics()
    render_pipeline_section()
    render_info_section()

    # Auto-refresh every 5 seconds
    st.markdown(
        """
        <script>
        setTimeout(function() {
            window.location.reload();
        }, 5000);
        </script>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
