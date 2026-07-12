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
from streamlit_autorefresh import st_autorefresh

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration - Mobile friendly
st.set_page_config(
    page_title="AW Arch Demo - Monitoring",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# API URL
API_URL = os.getenv("API_URL", "http://localhost:8000")
POLL_INTERVAL_MS = 500

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
    .block-container {
        max-width: 1100px;
        padding-top: 1rem;
        padding-bottom: 2rem;
    }

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


def submit_pipeline(models, simulations: int) -> str:
    """Submit a pipeline and return pipeline ID on success."""
    payload = {"models": models, "simulations": int(simulations)}
    response = requests.post(f"{API_URL}/pipeline", json=payload, timeout=10)
    if response.status_code == 200:
        return response.json().get("pipeline_id", "")
    raise RuntimeError(f"Failed to submit pipeline: HTTP {response.status_code}")


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
    st.caption("Shows both waiting jobs and in-progress jobs currently being processed by workers.")

    tracked_pipeline_id = st.session_state.get(
        "tracked_pipeline_id", st.session_state.get("last_pipeline_id", "")
    )
    if tracked_pipeline_id:
        tracked_status = get_pipeline_status(tracked_pipeline_id)
        if tracked_status and tracked_status.get("status") not in ("error", "not_found"):
            total_simulations = tracked_status.get("total_simulations", 0)
            completed_simulations = tracked_status.get("completed_simulations", 0)
            in_progress_simulations = max(total_simulations - completed_simulations, 0)

            a_col, b_col, c_col = st.columns(3)
            with a_col:
                st.metric("Tracked Pipeline", tracked_pipeline_id[:8])
            with b_col:
                st.metric("In Progress", in_progress_simulations)
            with c_col:
                st.metric("Completed", completed_simulations)

    metrics = get_metrics()

    if metrics and "queues" in metrics:
        queues = metrics["queues"]
        queue_stats = metrics.get("queue_stats", {})

        # Mobile-friendly queue cards (stacked on mobile)
        col1, col2, col3 = st.columns(3)
        
        queue_items = list(queues.items())
        
        def _queue_card(col, queue_name, queue_stats):
            stats = queue_stats.get(queue_name, {})
            ready = stats.get("ready", queues.get(queue_name, 0))
            unacked = stats.get("unacked", 0)
            with col:
                st.metric(
                    label=f"{queue_name.replace('jobs.', '').upper()} — waiting",
                    value=ready,
                )
                st.metric(
                    label=f"{queue_name.replace('jobs.', '').upper()} — in flight",
                    value=unacked,
                )

        # Queue A
        if len(queue_items) > 0:
            _queue_card(col1, queue_items[0][0], queue_stats)

        # Queue B
        if len(queue_items) > 1:
            _queue_card(col2, queue_items[1][0], queue_stats)

        # Queue C
        if len(queue_items) > 2:
            _queue_card(col3, queue_items[2][0], queue_stats)

        # Total jobs
        total_waiting = metrics.get("total_jobs_waiting", 0)
        total_processing = metrics.get("total_jobs_processing", 0)
        total_in_system = metrics.get("total_jobs_in_system", total_waiting + total_processing)

        t1, t2, t3 = st.columns(3)
        with t1:
            st.metric("Total Waiting", f"{total_waiting}", label_visibility="visible")
        with t2:
            st.metric("Total Processing", f"{total_processing}", label_visibility="visible")
        with t3:
            st.metric("Total In System", f"{total_in_system}", label_visibility="visible")

        # Chart - Responsive stacked bar showing waiting + in-flight
        if queues or queue_stats:
            queue_names = [q.replace("jobs.", "").upper() for q in (queue_stats or queues).keys()]
            if queue_stats:
                ready_values = [queue_stats[q].get("ready", 0) for q in queue_stats]
                unacked_values = [queue_stats[q].get("unacked", 0) for q in queue_stats]
            else:
                ready_values = list(queues.values())
                unacked_values = [0] * len(ready_values)

            fig = go.Figure(data=[
                go.Bar(
                    name="Waiting (ready)",
                    x=queue_names,
                    y=ready_values,
                    text=ready_values,
                    textposition="inside",
                    marker_color="#4C78A8",
                ),
                go.Bar(
                    name="In Flight (unacked)",
                    x=queue_names,
                    y=unacked_values,
                    text=unacked_values,
                    textposition="inside",
                    marker_color="#F58518",
                ),
            ])
            fig.update_layout(
                barmode="stack",
                title="Queue Depth by Worker (Waiting + In Flight)",
                height=400,
                margin=dict(l=10, r=10, t=40, b=10),
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig, use_container_width=True)

        if queue_stats:
            per_queue_rows = []
            for queue_name, stats in queue_stats.items():
                per_queue_rows.append(
                    {
                        "Queue": queue_name.replace("jobs.", "").upper(),
                        "Waiting": stats.get("ready", 0),
                        "Processing": stats.get("unacked", 0),
                        "Total": stats.get("total", 0),
                        "Consumers": stats.get("consumers", 0),
                    }
                )
            st.markdown("#### Per-Queue Runtime Stats")
            st.dataframe(per_queue_rows, use_container_width=True, hide_index=True)
    else:
        st.warning("⚠️ Unable to fetch metrics from API")


def render_compact_queue_metrics(
    metrics: Dict[str, Any], tracked_pipeline_id: str = "", tracked_status: Dict[str, Any] = None
):
    """Render condensed queue metrics for compact mode."""
    st.subheader("📬 Queue Snapshot")
    st.caption("Waiting and processing jobs across all worker queues.")

    if tracked_pipeline_id and tracked_status and tracked_status.get("status") != "error":
        total_simulations = tracked_status.get("total_simulations", 0)
        completed_simulations = tracked_status.get("completed_simulations", 0)
        in_progress_simulations = max(total_simulations - completed_simulations, 0)

        p_col1, p_col2 = st.columns(2)
        with p_col1:
            st.metric("In Progress", in_progress_simulations)
        with p_col2:
            st.metric("Completed", completed_simulations)

    if not metrics or "queues" not in metrics:
        st.warning("⚠️ Unable to fetch metrics from API")
        return

    queues = metrics.get("queues", {})
    total_waiting = metrics.get("total_jobs_waiting", 0)
    total_processing = metrics.get("total_jobs_processing", 0)
    total_in_system = metrics.get("total_jobs_in_system", total_waiting + total_processing)
    queue_stats = metrics.get("queue_stats", {})

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Waiting", total_waiting)
    with col2:
        st.metric("Processing", total_processing)
    with col3:
        st.metric("In System", total_in_system)

    col4, _ = st.columns(2)
    with col4:
        st.metric("Active Queues", len([d for d in queues.values() if d > 0]))

    if queue_stats:
        queue_rows = [
            {
                "Queue": q.replace("jobs.", "").upper(),
                "Waiting": stats.get("ready", 0),
                "Processing": stats.get("unacked", 0),
            }
            for q, stats in queue_stats.items()
        ]
        st.dataframe(queue_rows, use_container_width=True, hide_index=True)
    elif queues:
        queue_rows = [
            {"Queue": q.replace("jobs.", "").upper(), "Waiting": d}
            for q, d in queues.items()
        ]
        st.dataframe(queue_rows, use_container_width=True, hide_index=True)


def render_pipeline_section(show_tracking: bool = True):
    """Render pipeline submission and tracking - Mobile optimized."""
    st.subheader("🚀 Submit Pipeline")

    st.caption("Use quick launch for one-tap runs, or configure workers and simulations manually.")

    quick_col1, quick_col2, quick_col3 = st.columns(3)
    with quick_col1:
        run_quick_1 = st.button("1 Sim A->B", use_container_width=True)
    with quick_col2:
        run_quick_2 = st.button("10 Sims A->B", use_container_width=True)
    with quick_col3:
        run_quick_3 = st.button("25 Sims A->B->C", use_container_width=True)

    quick_request = None
    if run_quick_1:
        quick_request = (["worker_a", "worker_b"], 1)
    elif run_quick_2:
        quick_request = (["worker_a", "worker_b"], 10)
    elif run_quick_3:
        quick_request = (["worker_a", "worker_b", "worker_c"], 25)

    if quick_request:
        try:
            models, simulations = quick_request
            pipeline_id = submit_pipeline(models=models, simulations=simulations)
            if pipeline_id:
                st.success("✅ Quick pipeline submitted")
                st.info(f"**Pipeline ID:** `{pipeline_id}`")
                st.session_state.last_pipeline_id = pipeline_id
                st.session_state.tracked_pipeline_id = pipeline_id
        except Exception as e:
            st.error(f"Error submitting quick pipeline: {e}")

    st.markdown("#### Advanced Configuration")

    # Mobile-friendly form layout
    with st.form("pipeline_form"):
        models = st.multiselect(
            "Select Models",
            ["worker_a", "worker_b", "worker_c"],
            default=["worker_a", "worker_b"],
            help="Tap one or more workers to build the execution pipeline order.",
        )

        simulations = st.slider(
            "Number of Simulations",
            min_value=1,
            max_value=200,
            value=10,
            step=1,
            help="How many independent simulations to run through the selected workers.",
        )
        
        submit_button = st.form_submit_button("▶️ Submit Pipeline", use_container_width=True)
        
        if submit_button:
            if not models:
                st.error("Please select at least one model")
            else:
                try:
                    pipeline_id = submit_pipeline(models=models, simulations=simulations)
                    if pipeline_id:
                        st.success(f"✅ Pipeline submitted!")
                        st.info(f"**Pipeline ID:** `{pipeline_id}`")
                        # Store in session state for easy access
                        st.session_state.last_pipeline_id = pipeline_id
                        st.session_state.tracked_pipeline_id = pipeline_id
                except Exception as e:
                    st.error(f"Error submitting pipeline: {e}")

    if show_tracking:
        st.divider()

        st.subheader("📋 Track Pipeline")

        default_id = st.session_state.get(
            "tracked_pipeline_id", st.session_state.get("last_pipeline_id", "")
        )

        pipeline_id = st.text_input(
            "Enter Pipeline ID",
            value=default_id,
            key="pipeline_id_input_full",
            placeholder="Enter pipeline ID to track progress",
        )
        st.session_state.tracked_pipeline_id = pipeline_id

        if pipeline_id:
            status = get_pipeline_status(pipeline_id)
            if status and status.get("status") != "error":
                # Status cards - Mobile responsive
                st.markdown("### Status Overview")

                completed = status.get("completed_simulations", 0)
                total = status.get("total_simulations", 1)
                progress = (completed / total * 100) if total > 0 else 0

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    status_text = status.get("status", "Unknown").upper()
                    st.metric("Status", status_text)

                with col2:
                    st.metric(
                        "Total Simulations",
                        total,
                    )

                with col3:
                    st.metric("Completed", completed)

                with col4:
                    st.metric("Progress", f"{progress:.1f}%")

                # Progress bar
                st.markdown("### Progress")
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

                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Results Available", len(results))

                    with st.expander(f"📊 View {len(results)} simulation results"):
                        display_count = min(5, len(results))
                        st.caption(f"Showing first {display_count} of {len(results)} results")

                        for sim_key, sim_result in list(results.items())[:display_count]:
                            st.markdown(f"**{sim_key}**")
                            if isinstance(sim_result, dict):
                                st.json(sim_result)
                            else:
                                st.write(sim_result)
                            st.divider()

            else:
                st.warning("⚠️ Pipeline not found or has an error")


def render_pipeline_track_only_section():
    """Render only pipeline tracking controls for compact mode."""
    st.subheader("📋 Track Pipeline")
    default_id = st.session_state.get(
        "tracked_pipeline_id", st.session_state.get("last_pipeline_id", "")
    )

    pipeline_id = st.text_input(
        "Pipeline ID",
        value=default_id,
        key="pipeline_id_input_compact",
        placeholder="Paste pipeline ID",
        label_visibility="visible",
    )
    st.session_state.tracked_pipeline_id = pipeline_id

    status = None
    if pipeline_id:
        status = get_pipeline_status(pipeline_id)
        if status and status.get("status") != "error":
            completed = status.get("completed_simulations", 0)
            total = status.get("total_simulations", 1)
            progress = (completed / total * 100) if total > 0 else 0

            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Status", status.get("status", "unknown").upper())
            with c2:
                st.metric("Done", completed)
            with c3:
                st.metric("Progress", f"{progress:.1f}%")

            if total > 0:
                st.progress(completed / total)
                st.caption(f"{completed}/{total} simulations completed")
        else:
            st.warning("⚠️ Pipeline not found or has an error")

    return pipeline_id, status


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

    if "compact_mode" not in st.session_state:
        st.session_state.compact_mode = False

    if "tracked_pipeline_id" not in st.session_state:
        st.session_state.tracked_pipeline_id = st.session_state.last_pipeline_id

    with st.sidebar:
        st.markdown("### Display")
        st.session_state.compact_mode = st.toggle(
            "Compact mode",
            value=st.session_state.compact_mode,
            help="Prioritize launch + tracking controls and reduce heavy visuals for phone screens.",
        )

        auto_refresh_enabled = st.toggle(
            "Auto-refresh (0.5s)",
            value=True,
            help="Continuously poll queue and pipeline status every 0.5 seconds.",
        )

    if auto_refresh_enabled:
        st_autorefresh(interval=POLL_INTERVAL_MS, key="dashboard_auto_refresh")
    
    render_header()

    if st.session_state.compact_mode:
        metrics = get_metrics()
        render_pipeline_section(show_tracking=False)
        tracked_pipeline_id, tracked_status = render_pipeline_track_only_section()
        st.divider()
        render_compact_queue_metrics(metrics, tracked_pipeline_id, tracked_status)
        with st.expander("ℹ️ Links", expanded=False):
            st.code(API_URL, language="text")
            st.code("http://localhost:15672", language="text")
    else:
        render_queue_metrics()
        render_pipeline_section()
        render_info_section()

    # Footer
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
