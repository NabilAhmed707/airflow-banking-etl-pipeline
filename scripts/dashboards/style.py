import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BG = "#F6F7FB"
PANEL = "white"
BORDER = "#D0D7DE"
TEXT = "#111827"
MUTED = "#6B7280"

def apply_theme():
    plt.rcParams.update({
        "figure.facecolor": "white",
        "axes.facecolor": BG,
        "axes.edgecolor": BORDER,
        "axes.linewidth": 1.0,
        "axes.grid": True,
        "grid.color": "#E5E7EB",
        "grid.linewidth": 0.8,
        "grid.alpha": 1.0,
        "font.size": 11,
        "axes.titleweight": "bold",
        "axes.titlesize": 12,
    })

def style_axes(ax):
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color(BORDER)
        spine.set_linewidth(1.0)
    ax.tick_params(colors=TEXT)
    ax.title.set_color(TEXT)

def add_kpi_card(fig, x, y, w, h, title, value, subtitle=None):
    ax = fig.add_axes([x, y, w, h])
    ax.set_facecolor(PANEL)
    for s in ax.spines.values():
        s.set_color(BORDER)
        s.set_linewidth(1.0)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.text(0.06, 0.68, title, fontsize=10, color=MUTED, weight="bold", transform=ax.transAxes)
    ax.text(0.06, 0.18, str(value), fontsize=18, color=TEXT, weight="bold", transform=ax.transAxes)
    if subtitle:
        ax.text(0.06, 0.02, subtitle, fontsize=9, color=MUTED, transform=ax.transAxes)
    return ax