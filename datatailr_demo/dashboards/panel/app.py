# *************************************************************************
#  *
#  * Copyright (c) 2026 - Datatailr Inc.
#  * All Rights Reserved.
#  *
#  * This file is part of Datatailr and subject to the terms and conditions
#  * defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  * of this file, in parts or full, via any medium is strictly prohibited.
#  *************************************************************************

"""
Panel framework showcase deployed via datatailr_run_app.py (`panel serve`).

Each tab lives in its own module under ``pages/``; this file loads
extensions, assembles the template, and calls ``.servable()``.
"""

import panel as pn

from pages import data_table, indicators, layout, streaming, widgets
from theme import ACCENT

pn.extension("perspective", "echarts", sizing_mode="stretch_width")

tabs = pn.Tabs(
    ("Streaming chart", streaming.create()),
    ("Data table", data_table.create()),
    ("Widgets & bindings", widgets.create()),
    ("Layout & panes", layout.create()),
    ("Indicators & download", indicators.create()),
    sizing_mode="stretch_width",
    dynamic=True,
)

sidebar_md = pn.pane.Markdown(
    "### About\n\n"
    "This app demonstrates the **Panel** framework:\n\n"
    "- **Streaming chart** — live Perspective line chart\n"
    "- **Data table** — 9k-row Perspective viewer\n"
    "- **Widgets** — reactive `pn.bind()` bindings\n"
    "- **Layout** — Cards, Accordion, panes\n"
    "- **Indicators** — Gauge, Number, Trend\n\n"
    "---\n\n"
    f"Panel **{pn.__version__}**\n\n"
    "[panel.holoviz.org](https://panel.holoviz.org/)"
)

template = pn.template.FastListTemplate(
    title="Panel Showcase",
    sidebar=[sidebar_md],
    main=[tabs],
    accent_base_color=ACCENT,
    header_background=ACCENT,
)
template.servable()
