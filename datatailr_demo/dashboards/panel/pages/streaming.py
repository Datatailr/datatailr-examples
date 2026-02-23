# *************************************************************************
#  *
#  * Copyright (c) 2026 - Datatailr Inc.
#  * All Rights Reserved.
#  *
#  * This file is part of Datatailr and subject to the terms and conditions
#  * defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  * of this file, in parts or full, via any medium is strictly prohibited.
#  *************************************************************************

"""Tab 1 — Real-time Perspective line chart with periodic callback."""

import numpy as np
import pandas as pd
import panel as pn

from theme import ACCENT, ACCENT_LIGHT, CARD_STYLE


def create() -> pn.Column:
    df_stream = pd.DataFrame(np.random.randn(400, 4), columns=list("ABCD")).cumsum()

    perspective = pn.pane.Perspective(
        df_stream,
        plugin="d3_y_line",
        columns=["A", "B", "C", "D"],
        theme="material-dark",
        sizing_mode="stretch_width",
        height=500,
        margin=0,
    )

    rollover = pn.widgets.IntInput(
        name="Rollover", value=500, step=50, start=100, end=5000
    )

    def _tick():
        data = df_stream.iloc[-1] + np.random.randn(4)
        perspective.stream(data, rollover.value)

    cb = pn.state.add_periodic_callback(_tick, 50)

    return pn.Column(
        pn.pane.HTML(
            "<div style='background:{bg}; padding:12px 16px; border-radius:8px; "
            "margin-bottom:12px'>"
            "<b>Real-time Perspective chart</b> updated every <i>N</i>&nbsp;ms via "
            "<code>pn.state.add_periodic_callback</code>. "
            "Adjust period and rollover below.</div>".format(bg=ACCENT_LIGHT)
        ),
        pn.Row(
            pn.Card(
                cb.param.period,
                rollover,
                title="Stream controls",
                header_background=ACCENT,
                header_color="white",
                styles=CARD_STYLE,
                width=260,
                collapsed=False,
            ),
            perspective,
        ),
        margin=(5, 0),
    )
