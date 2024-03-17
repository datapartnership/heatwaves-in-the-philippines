
import bokeh
from bokeh.layouts import *
from bokeh.models import *
import matplotlib as mpl

from bokeh.core.validation.warnings import EMPTY_LAYOUT, MISSING_RENDERERS
bokeh.core.validation.silence(EMPTY_LAYOUT, True)
bokeh.core.validation.silence(MISSING_RENDERERS, True)
from bokeh.plotting import figure, output_file, show, output_notebook
from bokeh.models import Span


color_palette = [  '#4E79A7',  # Blue
    '#F28E2B',  # Orange
    '#E15759',  # Red
    '#76B7B2',  # Teal
    '#59A14F',  # Green
    '#EDC948',  # Yellow
    '#B07AA1',  # Purple
    '#FF9DA7',  # Pink
    '#9C755F',  # Brown
    '#BAB0AC',  # Gray
    '#7C7C7C',  # Dark gray
    '#6B4C9A',  # Violet
    '#D55E00',  # Orange-red
    '#CC61B0',  # Magenta
    '#0072B2',  # Bright blue
    '#329262',  # Peacock green
    '#9E5B5A',  # Brick red
    '#636363',  # Medium gray
    '#CD9C00',  # Gold
    '#5D69B1',  # Medium blue
]


bokeh.core.validation.silence(EMPTY_LAYOUT, True)

def get_line_plot(heatwaves,title, source, markers=False, subtitle=None, measure = 'heat_index', category = 'city'):

    p2 = figure(x_axis_type = 'datetime', width = 1000, height = 400, toolbar_location='above')
    p2.add_layout(Legend(), "right")

    for id, city in enumerate(heatwaves['city'].unique()):
        df = heatwaves[heatwaves[category]==city].groupby('time').mean().reset_index()[['time', measure]]
        p2.line(df['time'], df[measure], line_width=2, line_color = color_palette[id], legend_label=city)

    # p2.legend.click_policy='hide'
    # if subtitle is not None:
    #     p2.title = subtitle

    p2.title = title
    # title_fig = figure(title=title, toolbar_location=None,width=1000, height=40, )
    # title_fig.title.align = "left"
    # title_fig.title.text_font_size = "20pt"
    # title_fig.border_fill_alpha = 0
    # title_fig.outline_line_width=0

    # #with silence(MISSING_RENDERERS):
    sub_title = figure(title=source, toolbar_location=None,width=1000, height=40, )
    sub_title.title.align = "left"
    sub_title.title.text_font_size = "10pt"
    sub_title.title.text_font_style="normal"
    sub_title.border_fill_alpha = 0
    sub_title.outline_line_width=0

    layout = column(p2, sub_title)

#     if markers:
#         p2.renderers.extend([
#         Span(
#             location=datetime(2020, 3, 15),
#             dimension="height",
#             line_color='#7C7C7C',
#             line_width=2,
#             line_dash=(4,4)
#       ),
#         Span(
#             location=datetime(2021, 2, 15),
#             dimension="height",
#             line_color='#7C7C7C',
#             line_width=2,
#             line_dash=(4,4)
#         ),
#     ]
# )

    return layout