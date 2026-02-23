# *************************************************************************
#  *
#  * Copyright (c) 2026 - Datatailr Inc.
#  * All Rights Reserved.
#  *
#  * This file is part of Datatailr and subject to the terms and conditions
#  * defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  * of this file, in parts or full, via any medium is strictly prohibited.
#  *************************************************************************

"""Tab 3 — Interactive widgets with reactive pn.bind() bindings."""

import numpy as np
import pandas as pd
import panel as pn

from theme import ACCENT, ACCENT_LIGHT, CARD_STYLE


def create() -> pn.Column:
    slider_rows = pn.widgets.IntSlider(name="Rows", start=5, end=50, value=10)
    slider_cols = pn.widgets.IntSlider(name="Columns", start=2, end=10, value=4)
    select_agg = pn.widgets.Select(
        name="Aggregation", options=["sum", "mean", "std", "min", "max"]
    )
    checkbox_abs = pn.widgets.Checkbox(name="Absolute values", value=False)
    text_prefix = pn.widgets.TextInput(name="Column prefix", value="col")

    def _build_df(rows, cols, agg, absolute, prefix):
        rng = np.random.default_rng()
        raw = rng.standard_normal((rows, cols))
        if absolute:
            raw = np.abs(raw)
        columns = [f"{prefix}_{i}" for i in range(cols)]
        df = pd.DataFrame(raw, columns=columns)
        agg_row = getattr(df, agg)()
        summary = (
            f"<b>{agg.title()}</b> across "
            f"<span style='color:{ACCENT}; font-weight:600'>{rows}</span> rows "
            f"&times; "
            f"<span style='color:{ACCENT}; font-weight:600'>{cols}</span> cols"
        )
        return pn.Column(
            pn.pane.HTML(
                f"<div style='padding:8px 12px; background:{ACCENT_LIGHT}; "
                f"border-radius:6px; margin-bottom:8px'>{summary}</div>"
            ),
            pn.pane.DataFrame(df, sizing_mode="stretch_width", max_height=300),
            pn.pane.HTML(
                "<div style='margin-top:12px; font-weight:600; "
                "border-bottom:2px solid {c}; padding-bottom:4px; "
                "display:inline-block'>Aggregation</div>".format(c=ACCENT)
            ),
            pn.pane.DataFrame(
                agg_row.to_frame(name=agg).T,
                sizing_mode="stretch_width",
            ),
        )

    reactive_output = pn.bind(
        _build_df, slider_rows, slider_cols, select_agg, checkbox_abs, text_prefix
    )

    return pn.Column(
        pn.pane.HTML(
            "<div style='background:{bg}; padding:12px 16px; border-radius:8px; "
            "margin-bottom:12px'>"
            "Every widget is wired to a single function with "
            "<code>pn.bind()</code>. Changing any control re-renders the output "
            "instantly.</div>".format(bg=ACCENT_LIGHT)
        ),
        pn.Row(
            pn.Card(
                slider_rows,
                slider_cols,
                select_agg,
                checkbox_abs,
                text_prefix,
                title="Controls",
                header_background=ACCENT,
                header_color="white",
                styles=CARD_STYLE,
                width=280,
                collapsed=False,
            ),
            pn.Column(reactive_output, sizing_mode="stretch_width"),
        ),
        margin=(5, 0),
    )
