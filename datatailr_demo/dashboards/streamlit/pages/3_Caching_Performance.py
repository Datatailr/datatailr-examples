##########################################################################
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
##########################################################################
import time
import random

import streamlit as st
import pandas as pd


@st.cache_data(show_spinner=False)
def generate_data(n: int, seed: int) -> pd.DataFrame:
    random.seed(seed)
    data = {
        "a": [random.random() for _ in range(n)],
        "b": [random.random() * 2 for _ in range(n)],
        "c": [random.randint(0, 100) for _ in range(n)],
    }
    # Simulate computation time
    time.sleep(0.2)
    return pd.DataFrame(data)


class ExpensiveClient:
    def __init__(self):
        # Simulate an expensive setup
        time.sleep(0.4)
        self.created_at = time.time()

    def do_work(self) -> float:
        time.sleep(0.1)
        return random.random()


@st.cache_resource(show_spinner=False)
def get_client() -> ExpensiveClient:
    return ExpensiveClient()


def main():
    st.title("Caching & Performance")
    st.caption("Demonstrates st.cache_data, st.cache_resource, and progress bars.")

    n = st.slider("Rows", 1000, 10000, 3000, step=500)
    seed = st.number_input("Seed", 0, value=7, step=1)

    t0 = time.time()
    df = generate_data(n, int(seed))
    elapsed = (time.time() - t0) * 1000
    st.success(f"Data ready in {elapsed:.1f} ms (cached on repeats)")

    tab1, tab2 = st.tabs(["Overview", "Chart"])
    with tab1:
        st.dataframe(df.head(500), use_container_width=True)
    with tab2:
        st.line_chart(df[["a", "b"]], use_container_width=True)

    st.subheader("Progress Demo")
    if st.button("Run 5 steps"):
        prog = st.progress(0)
        status = st.empty()
        for i in range(1, 6):
            time.sleep(0.2)
            prog.progress(i * 20)
            status.text(f"Step {i}/5")
        status.text("Done!")
        st.success("Completed.")

    st.subheader("Cached Resource (Client)")
    client = get_client()
    st.info(f"Client created at {client.created_at:.0f}")
    if st.button("Use client"):
        result = client.do_work()
        st.write(f"Client work result: {result:.3f}")

    st.toast("Tip: Adjust inputs and re-run to see caching effects.")


if __name__ == "__main__":
    main()
