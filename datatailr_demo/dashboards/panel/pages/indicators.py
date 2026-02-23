# *************************************************************************
#  *
#  * Copyright (c) 2026 - Datatailr Inc.
#  * All Rights Reserved.
#  *
#  * This file is part of Datatailr and subject to the terms and conditions
#  * defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  * of this file, in parts or full, via any medium is strictly prohibited.
#  *************************************************************************

"""Tab 5 — Indicators (Gauge, Number, Trend) and FileDownload."""

import io

import numpy as np
import panel as pn

from pages.data_table import df_table
from theme import ACCENT, ACCENT_LIGHT, CARD_STYLE


def create() -> pn.Column:
    gauge = pn.indicators.Gauge(
        name="CPU",
        value=67,
        bounds=(0, 100),
        format="{value}%",
        colors=[(0.33, "green"), (0.66, "gold"), (1, "red")],
        width=250,
        height=250,
    )

    number = pn.indicators.Number(
        name="Revenue",
        value=1_234_567,
        format="${value:,.0f}",
        default_color=ACCENT,
        font_size="28pt",
    )

    trend = pn.indicators.Trend(
        name="Users",
        data={
            "x": list(range(20)),
            "y": np.random.randint(80, 120, 20).cumsum().tolist(),
        },
        width=280,
        height=180,
        plot_color=ACCENT,
    )

    def _csv_callback():
        buf = io.StringIO()
        df_table.head(100).to_csv(buf, index=False)
        buf.seek(0)
        return buf

    download_btn = pn.widgets.FileDownload(
        callback=_csv_callback,
        filename="sample_data.csv",
        button_type="primary",
        label="Download first 100 rows as CSV",
    )

    return pn.Column(
        pn.pane.HTML(
            "<div style='background:{bg}; padding:12px 16px; border-radius:8px; "
            "margin-bottom:12px'>"
            "Built-in <b>indicators</b> (Gauge, Number, Trend) and a "
            "<b>FileDownload</b> widget that generates a CSV on the fly."
            "</div>".format(bg=ACCENT_LIGHT)
        ),
        pn.Row(
            pn.Card(
                gauge,
                title="CPU usage",
                header_background=ACCENT,
                header_color="white",
                styles=CARD_STYLE,
            ),
            pn.Card(
                pn.Column(number, pn.Spacer(height=10), trend),
                title="Business metrics",
                header_background=ACCENT,
                header_color="white",
                styles=CARD_STYLE,
            ),
            sizing_mode="stretch_width",
        ),
        pn.Spacer(height=10),
        download_btn,
        margin=(5, 0),
    )
