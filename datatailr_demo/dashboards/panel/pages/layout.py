# *************************************************************************
#  *
#  * Copyright (c) 2026 - Datatailr Inc.
#  * All Rights Reserved.
#  *
#  * This file is part of Datatailr and subject to the terms and conditions
#  * defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  * of this file, in parts or full, via any medium is strictly prohibited.
#  *************************************************************************

"""Tab 4 — Layout primitives: Card, Accordion, Markdown / HTML / JSON panes."""

import panel as pn

from theme import ACCENT, ACCENT_LIGHT, CARD_STYLE


def create() -> pn.Column:
    card_markdown = pn.Card(
        pn.pane.Markdown(
            "Panel's **Markdown** pane renders GitHub-flavoured Markdown "
            "including tables, code blocks, and math:\n\n"
            "| Feature | Supported |\n"
            "|---|---|\n"
            "| Bold / italic | Yes |\n"
            "| Code blocks | Yes |\n"
            "| Tables | Yes |\n\n"
            "```python\nimport panel as pn\npn.extension()\n```"
        ),
        title="Markdown",
        header_background=ACCENT,
        header_color="white",
        styles=CARD_STYLE,
        collapsed=False,
    )

    card_html = pn.Card(
        pn.pane.HTML(
            "<div style='padding:12px'>"
            "<p>Arbitrary <b>HTML</b>, CSS, and inline SVG.</p>"
            "<svg width='140' height='44'>"
            "<rect width='140' height='44' rx='8' fill='{c}'/>"
            "<text x='70' y='28' text-anchor='middle' fill='white' "
            "font-size='14' font-weight='600'>SVG badge</text>"
            "</svg>"
            "</div>".format(c=ACCENT)
        ),
        title="HTML",
        header_background=ACCENT,
        header_color="white",
        styles=CARD_STYLE,
        collapsed=False,
    )

    card_json = pn.Card(
        pn.pane.JSON(
            {
                "framework": "Panel",
                "version": pn.__version__,
                "features": ["widgets", "layout", "streaming", "templates"],
            },
            depth=2,
            theme="light",
        ),
        title="JSON",
        header_background=ACCENT,
        header_color="white",
        styles=CARD_STYLE,
        collapsed=False,
    )

    accordion = pn.Accordion(
        ("Row", pn.pane.Markdown("`pn.Row(a, b)` — horizontal layout.")),
        ("Column", pn.pane.Markdown("`pn.Column(a, b)` — vertical layout.")),
        ("Card", pn.pane.Markdown("`pn.Card(content, title=...)` — collapsible card.")),
        ("Tabs", pn.pane.Markdown("`pn.Tabs(('Name', widget))` — tabbed navigation.")),
        (
            "Accordion",
            pn.pane.Markdown("`pn.Accordion(...)` — collapsible sections (this one!)."),
        ),
        ("GridSpec", pn.pane.Markdown("`pn.GridSpec(...)` — CSS-grid based layout.")),
        active=[0],
    )

    return pn.Column(
        pn.pane.HTML(
            "<div style='background:{bg}; padding:12px 16px; border-radius:8px; "
            "margin-bottom:12px'>"
            "Panel ships layout containers (Row, Column, Card, Tabs, Accordion, "
            "GridSpec) and pane types for Markdown, HTML, JSON, LaTeX, and more."
            "</div>".format(bg=ACCENT_LIGHT)
        ),
        pn.Row(card_markdown, card_html, card_json),
        pn.pane.HTML(
            "<h4 style='margin:20px 0 8px; border-bottom:2px solid {c}; "
            "padding-bottom:6px; display:inline-block'>Layout containers</h4>".format(
                c=ACCENT
            )
        ),
        accordion,
        margin=(5, 0),
    )
