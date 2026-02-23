##########################################################################
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
##########################################################################
import random
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd


def _make_series(n: int, seed: int, as_time: bool) -> pd.DataFrame:
    random.seed(seed)
    values = [0]
    for _ in range(n - 1):
        values.append(values[-1] + random.randint(-5, 8))
    if as_time:
        start = datetime.now() - timedelta(days=n)
        idx = [start + timedelta(days=i) for i in range(n)]
    else:
        idx = list(range(n))
    return pd.DataFrame({"x": idx, "y": values})


def main():
    st.title("Visualization Playground")
    st.caption("Try different chart types and controls.")

    with st.sidebar:
        st.header("Controls")
        points = st.slider("Points", min_value=50, max_value=2000, value=500, step=50)
        seed = st.number_input("Seed", min_value=0, value=42, step=1)
        as_time = st.toggle("Time series", value=True)
        chart = st.radio("Chart", ["Line", "Area", "Bar", "Scatter"], index=0)

    df = _make_series(points, int(seed), as_time)
    st.write(f"Generated {len(df)} points")

    # Chart rendering
    if chart == "Line":
        st.line_chart(df.set_index("x"), use_container_width=True)
    elif chart == "Area":
        st.area_chart(df.set_index("x"), use_container_width=True)
    elif chart == "Bar":
        st.bar_chart(df.set_index("x"), use_container_width=True)
    else:
        st.scatter_chart(df, x="x", y="y", use_container_width=True)

    with st.expander("Show data"):
        st.dataframe(df.head(1000), use_container_width=True)

    st.info("This page highlights interactive charts, sidebar controls, and expanders.")


if __name__ == "__main__":
    main()
