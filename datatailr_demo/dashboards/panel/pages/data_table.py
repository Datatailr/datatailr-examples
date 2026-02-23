# *************************************************************************
#  *
#  * Copyright (c) 2026 - Datatailr Inc.
#  * All Rights Reserved.
#  *
#  * This file is part of Datatailr and subject to the terms and conditions
#  * defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  * of this file, in parts or full, via any medium is strictly prohibited.
#  *************************************************************************

"""Tab 2 — Perspective data table with 9 000 mixed-type rows."""

import random
from datetime import datetime, timedelta

import pandas as pd
import panel as pn

from theme import ACCENT_LIGHT

N = 9_000

df_table = pd.DataFrame(
    {
        "int": [random.randint(-10, 10) for _ in range(N)],
        "float": [random.uniform(-10, 10) for _ in range(N)],
        "date": [(datetime.now() + timedelta(days=i)).date() for i in range(N)],
        "datetime": [(datetime.now() + timedelta(hours=i)) for i in range(N)],
        "category": ["Category A", "Category B", "Category C"] * 3_000,
        "link": [
            "https://panel.holoviz.org/",
            "https://discourse.holoviz.org/",
            "https://github.com/holoviz/panel",
        ]
        * 3_000,
    }
)


def create() -> pn.Column:
    perspective = pn.pane.Perspective(
        df_table,
        sizing_mode="stretch_width",
        height=600,
        theme="material-dark",
    )

    return pn.Column(
        pn.pane.HTML(
            "<div style='background:{bg}; padding:12px 16px; border-radius:8px; "
            "margin-bottom:12px'>"
            "<b>{n:,}</b> rows with mixed types: int, float, date, datetime, "
            "category, and link. Sort, filter, pivot, and group directly inside "
            "the Perspective viewer.</div>".format(bg=ACCENT_LIGHT, n=N)
        ),
        perspective,
        margin=(5, 0),
    )
