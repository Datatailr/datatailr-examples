##########################################################################
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
##########################################################################
import streamlit as st


def main():
    st.title("Components & Layout")
    st.caption("Explore tabs, expanders, metrics, forms, and sidebar controls.")

    with st.sidebar:
        st.header("Sidebar Controls")
        theme_color = st.color_picker("Accent color", value="#4B9CD3")
        density = st.selectbox("Density", ["Compact", "Comfortable"], index=1)
        show_tips = st.checkbox("Show tips", value=True)

    # Apply a simple accent style
    st.markdown(
        f"<style>.accent {{ color: {theme_color}; }}</style>",
        unsafe_allow_html=True,
    )

    tab_inputs, tab_metrics, tab_layout = st.tabs(["Inputs", "Metrics", "Layout"])

    with tab_inputs:
        st.subheader("Common Inputs")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.text_input("Text", placeholder="Type here…")
            st.number_input("Number", value=10, step=1)
        with col2:
            st.date_input("Date")
            st.time_input("Time")
        with col3:
            st.selectbox("Select", ["Alpha", "Beta", "Gamma"], index=0)
            st.slider("Slider", 0, 100, 50)

        with st.expander("Form with validation"):
            with st.form("sample_form"):
                name = st.text_input("Name")
                agree = st.checkbox("I agree")
                submitted = st.form_submit_button("Submit")
                if submitted:
                    if not name:
                        st.error("Name is required")
                    elif not agree:
                        st.warning("Please agree to continue")
                    else:
                        st.success(f"Thanks, {name}!")

    with tab_metrics:
        st.subheader("KPIs")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Users", 1248, "+42")
        with c2:
            st.metric("Latency (ms)", 98, "-7")
        with c3:
            st.metric("Errors", 3, "+1")

        st.info("Metrics demonstrate compact KPIs; adjust layout density in sidebar.")

    with tab_layout:
        st.subheader("Containers & Expanders")
        with st.container(border=True):
            st.write("Container with border; useful for grouping content.")
            st.write("Density:", density)
            st.write(
                "Accent:",
                f"<span class='accent'>{theme_color}</span>",
                unsafe_allow_html=True,
            )

        with st.expander("More details"):
            st.write("This expander can hold additional context or settings.")

    if show_tips:
        st.toast("Tip: Use tabs to organize complex UIs.")


if __name__ == "__main__":
    main()
