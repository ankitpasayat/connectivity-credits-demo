"""Shared vector architecture for the README and PDF guide.

Run with Python and reportlab installed: python docs/build_architecture.py
The guide imports architecture() so both outputs use the same drawing.
"""
from pathlib import Path
from math import atan2, cos, sin, pi
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon, Path as ShapePath
from reportlab.graphics import renderSVG
from reportlab.lib.colors import HexColor, white


def architecture():
    d = Drawing(880, 850)
    d.add(Rect(0, 0, 880, 850, fillColor=white, strokeColor=None))
    ink, muted = HexColor('#16283F'), HexColor('#506178')
    blue, violet, green = '#2563EB', '#7C3AED', '#047857'

    def text(x, y, value, size=18, color=muted, bold=False, anchor='middle'):
        d.add(String(x, y, value, fontName='Helvetica-Bold' if bold else 'Helvetica',
                     fontSize=size, fillColor=color, textAnchor=anchor))

    def arrow(points, color='#64748B', dashed=False):
        color = HexColor(color)
        p = ShapePath(strokeColor=color, strokeWidth=2.4, fillColor=None,
                      strokeDashArray=[7, 6] if dashed else None)
        p.moveTo(*points[0])
        for point in points[1:]:
            p.lineTo(*point)
        d.add(p)
        x, y = points[-1]
        px, py = points[-2]
        a = atan2(y - py, x - px)
        pts = [x, y]
        for turn in [-pi / 6, pi / 6]:
            pts += [x - 11 * cos(a + turn), y - 11 * sin(a + turn)]
        d.add(Polygon(pts, fillColor=color, strokeColor=None))

    def label(x, y, lines, color=muted, size=17):
        for i, value in enumerate(lines):
            text(x, y - i * 23, value, size=size, color=color)

    def box(y, title, subtitle, fill, border):
        d.add(Rect(280, y, 320, 80, rx=14, ry=14,
                   fillColor=HexColor(fill), strokeColor=HexColor(border), strokeWidth=2))
        text(440, y + 48, title, 23, ink, True)
        text(440, y + 23, subtitle, 17, muted)

    # Primary event path: deliberately readable top to bottom.
    box(748, 'React console', 'TypeScript interface', '#EFF6FF', blue)
    box(612, 'Express API', 'Validate the submitted reading', '#ECFEFF', '#0891B2')

    # A database cylinder distinguishes persisted state from processing code.
    db = ShapePath(fillColor=HexColor('#F5F3FF'), strokeColor=HexColor(violet), strokeWidth=2)
    db.moveTo(280, 561)
    db.curveTo(280, 587, 600, 587, 600, 561)
    db.lineTo(600, 493)
    db.curveTo(600, 467, 280, 467, 280, 493)
    db.closePath()
    d.add(db)
    rim = ShapePath(fillColor=None, strokeColor=HexColor(violet), strokeWidth=1.5)
    rim.moveTo(280, 561)
    rim.curveTo(280, 535, 600, 535, 600, 561)
    d.add(rim)
    text(440, 512, 'PostgreSQL', 23, ink, True)
    text(440, 490, 'Readings, policies and results', 17)

    box(340, 'Outbox publisher', 'Send saved events to Kafka', '#EEF2FF', '#4F46E5')
    box(204, 'Apache Kafka', 'Deliver the event to the worker', '#FFFBEB', '#D97706')
    box(68, 'Worker', 'Apply policy and protect issuance', '#ECFDF5', green)

    arrow([(440, 748), (440, 692)], blue)
    label(566, 722, ['Submit reading'], HexColor(blue))
    arrow([(440, 612), (440, 581)], '#0891B2')
    label(614, 594, ['Save input + event + outbox'], HexColor('#0E7490'), 16)
    arrow([(440, 477), (440, 420)], violet)
    label(564, 446, ['Pending delivery'], HexColor(violet))
    arrow([(440, 340), (440, 284)], '#4F46E5')
    label(538, 310, ['Publish event'], HexColor('#4F46E5'))
    arrow([(440, 204), (440, 148)], '#D97706')
    label(542, 174, ['Consume event'], HexColor('#B45309'))

    # Reads and transactional writes are distinct and explicitly directed.
    arrow([(280, 510), (112, 510), (112, 108), (280, 108)], violet, dashed=True)
    d.add(Rect(24, 285, 182, 78, fillColor=white, strokeColor=None))
    label(112, 344, ['Read saved reading', 'and active policy'], HexColor(violet))
    arrow([(600, 108), (768, 108), (768, 510), (600, 510)], green)
    d.add(Rect(675, 272, 187, 105, fillColor=white, strokeColor=None))
    label(768, 355, ['Commit decision,', 'credit and audit', 'together'], HexColor(green))

    # Results travel back via the API; keep this short return beside the UI.
    arrow([(280, 652), (198, 652), (198, 788), (280, 788)], blue, dashed=True)
    d.add(Rect(82, 695, 145, 49, fillColor=white, strokeColor=None))
    label(152, 724, ['Display stored', 'results'], HexColor(blue), 16)

    d.add(Line(280, 26, 313, 26, strokeColor=HexColor('#64748B'), strokeWidth=2.4))
    text(324, 21, 'Event / write flow', 15, anchor='start')
    d.add(Line(503, 26, 537, 26, strokeColor=HexColor(violet), strokeWidth=2.4, strokeDashArray=[7, 6]))
    text(548, 21, 'Read / display flow', 15, anchor='start')
    return d


if __name__ == '__main__':
    target = Path(__file__).with_name('architecture.svg')
    renderSVG.drawToFile(architecture(), str(target))
    svg = target.read_text()
    svg = svg.replace('<title>...</title>', '<title>Connectivity Credits demo architecture</title>')
    svg = svg.replace('<desc>...</desc>', '<desc>React sends a reading to Express, which saves it and an outbox event in PostgreSQL. The publisher sends the event through Kafka. The worker reads the saved reading and active policy, then commits its decision, credit and audit together. The API returns stored results to React.</desc>')
    svg = svg.replace('font-family: Helvetica-Bold;', 'font-family: Arial, Helvetica, sans-serif; font-weight: 700;')
    svg = svg.replace('font-family: Helvetica;', 'font-family: Arial, Helvetica, sans-serif;')
    target.write_text(svg)
    print(target)
