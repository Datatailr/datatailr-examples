##########################################################################
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
##########################################################################
import io
from typing import Optional

import streamlit as st
import pandas as pd


def _read_csv(file) -> Optional["pd.DataFrame"]:
    try:
        import pandas as pd  # local import to keep dependencies scoped

        content = file.read()
        return pd.read_csv(io.BytesIO(content))
    except Exception as e:
        st.error(f"Failed to read CSV: {e}")
        return None


def main():
    st.title("Data Explorer")
    st.caption("Upload a CSV, filter columns, and visualize.")

    uploaded = st.file_uploader("Upload CSV", type=["csv"], accept_multiple_files=False)
    if not uploaded:
        st.info("Upload a .csv file to begin.")
        return

    df = _read_csv(uploaded)
    if df is None:
        return

    st.success(f"Loaded dataset with {df.shape[0]} rows × {df.shape[1]} columns")
    st.dataframe(df.head(100), use_container_width=True)

    # Basic filtering
    import pandas as pd

    num_cols = df.select_dtypes(include=["number"]).columns.tolist()
    cat_cols = df.select_dtypes(exclude=["number"]).columns.tolist()

    with st.expander("Filter data"):
        colA, colB = st.columns(2)
        with colA:
            num_col = st.selectbox("Numeric column", options=["(none)"] + num_cols)
            if num_col and num_col != "(none)":
                min_v = float(pd.to_numeric(df[num_col], errors="coerce").min())
                max_v = float(pd.to_numeric(df[num_col], errors="coerce").max())
                sel_min, sel_max = st.slider(
                    "Range",
                    min_value=min_v,
                    max_value=max_v,
                    value=(min_v, max_v),
                )
                df = df[df[num_col].between(sel_min, sel_max)]

        with colB:
            cat_col = st.selectbox("Categorical column", options=["(none)"] + cat_cols)
            if cat_col and cat_col != "(none)":
                cats = sorted([str(x) for x in df[cat_col].dropna().unique().tolist()])
                picks = st.multiselect(
                    "Keep categories", options=cats, default=cats[: min(5, len(cats))]
                )
                if picks:
                    df = df[df[cat_col].astype(str).isin(picks)]

        st.info(f"Filtered dataset: {df.shape[0]} rows × {df.shape[1]} columns")

    # Quick chart
    st.subheader("Quick Chart")
    chart_cols = st.multiselect(
        "Numeric columns to plot", options=num_cols, max_selections=3
    )
    chart_type = st.radio(
        "Chart type", options=["Line", "Area", "Bar"], horizontal=True
    )

    if chart_cols:
        display_df = df[chart_cols].head(1000)
        if chart_type == "Line":
            st.line_chart(display_df, use_container_width=True)
        elif chart_type == "Area":
            st.area_chart(display_df, use_container_width=True)
        else:
            st.bar_chart(display_df, use_container_width=True)
    else:
        st.info("Select at least one numeric column to plot.")

    # Download button
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download filtered CSV",
        data=csv_bytes,
        file_name="filtered.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
