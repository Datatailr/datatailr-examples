##########################################################################
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
##########################################################################
import os

import streamlit as st
import requests
import time


def _data_service_url() -> str:
    if os.getenv("DATATAILR_JOB_TYPE") == "workspace":
        return "http://localhost:1024"
    return "http://simple-service"


def _health_check(url: str) -> str:
    try:
        resp = requests.get(f"{url}/__health_check__.html", timeout=2)
        if resp.status_code == 200 and resp.text.strip() == "OK":
            return "Healthy"
        return f"Unhealthy: {resp.text.strip()}"
    except Exception as e:
        return f"Error: {e}"


def main():
    st.title("Datatailr Demo — Streamlit Showcase")

    # Sidebar: quick navigation help
    st.sidebar.success("Use the sidebar to navigate pages.")

    # Data service integration summary card
    data_service_url = _data_service_url()
    health_status = _health_check(data_service_url)
    with st.container(border=True):
        st.subheader("Data Service Status")
        st.write(f"Base URL: {data_service_url}")
        st.info(f"Health: {health_status}")

    # Greeting functionality
    st.subheader("Greeting via Data Service")
    name = st.text_input("Enter your name", placeholder="e.g., Ada")
    if name:
        try:
            r = requests.get(
                f"{data_service_url}/greet", params={"name": name}, timeout=2
            )
            if r.status_code == 200:
                greeting = r.json().get("greeting", "")
                st.success(greeting)
            else:
                st.error("Data service error on /greet.")
        except Exception as e:
            st.error(f"Could not connect to Data service: {e}")

    # Random number functionality
    st.subheader("Random Number via Data Service")
    col1, col2 = st.columns(2)
    with col1:
        min_val = st.number_input("Min value", value=0, step=1)
    with col2:
        max_val = st.number_input("Max value", value=100, step=1)
    get_rand = st.button("Get Random Number")
    if get_rand:
        try:
            r = requests.get(
                f"{data_service_url}/random",
                params={"min": int(min_val), "max": int(max_val)},
                timeout=2,
            )
            if r.status_code == 200:
                rand_num = r.json().get("random_number", None)
                st.write(f"Random number: {rand_num}")
            else:
                st.error("Data service error on /random.")
        except Exception as e:
            st.error(f"Could not connect to Data service: {e}")

    # Showcase: progress + status widgets
    st.subheader("Quick Demo: Progress & Status")
    with st.expander("Simulate a short task"):
        if st.button("Run task"):
            prog = st.progress(0)
            status = st.empty()
            for i in range(1, 6):
                time.sleep(0.2)
                prog.progress(i * 20)
                status.text(f"Step {i}/5")
            status.text("Done!")
            st.success("Task completed.")


if __name__ == "__main__":
    main()
