"""Draw architecture.png to match the required topic flow."""

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

ROOT = Path(__file__).resolve().parents[1]


def box(ax, x, y, w, h, text, face, edge="#1f2933"):
    patch = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.02,rounding_size=0.08",
        facecolor=face, edgecolor=edge, linewidth=1.2,
    )
    ax.add_patch(patch)
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=8.5, color="#102027", wrap=True)


def arrow(ax, x1, y1, x2, y2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=12, linewidth=1.1, color="#334e68"))


def main() -> None:
    fig, ax = plt.subplots(figsize=(12.2, 4.6), dpi=160)
    ax.set_xlim(0, 12.2)
    ax.set_ylim(0, 4.6)
    ax.axis("off")
    ax.set_title("Every major processing stage consumes from Kafka and publishes back to Kafka", fontsize=11, pad=8)

    box(ax, 0.25, 1.85, 1.7, 0.85, "Taxi data\nproducer", "#d6e4f0")
    box(ax, 2.35, 1.85, 1.45, 0.85, "Kafka\ntaxi.raw", "#f6e7c1")
    box(ax, 4.15, 1.85, 1.7, 0.85, "Stage 1\nClean + Bloom", "#d9ead3")
    box(ax, 6.2, 1.85, 1.45, 0.85, "Kafka\ntaxi.clean", "#f6e7c1")
    box(ax, 8.05, 2.85, 1.8, 0.85, "Stage 2\nSampling + HLL\n+ DGIM", "#d9ead3")
    box(ax, 10.15, 2.85, 1.75, 0.85, "Kafka\ntaxi.metrics", "#f6e7c1")
    box(ax, 8.05, 0.85, 1.8, 0.85, "Stage 3\nMinHash-LSH", "#d9ead3")
    box(ax, 10.15, 0.85, 1.75, 0.85, "Kafka\ntaxi.lsh", "#f6e7c1")
    box(ax, 10.15, 1.85, 1.75, 0.7, "Final consumer\nresults.jsonl", "#f8d7da")

    arrow(ax, 1.95, 2.27, 2.35, 2.27)
    arrow(ax, 3.8, 2.27, 4.15, 2.27)
    arrow(ax, 5.85, 2.27, 6.2, 2.27)
    arrow(ax, 7.65, 2.45, 8.05, 3.2)
    arrow(ax, 7.65, 2.1, 8.05, 1.35)
    arrow(ax, 9.85, 3.27, 10.15, 3.27)
    arrow(ax, 9.85, 1.27, 10.15, 1.27)
    arrow(ax, 11.0, 2.85, 11.0, 2.55)
    arrow(ax, 11.0, 1.7, 11.0, 1.85)

    fig.tight_layout()
    dest = ROOT / "architecture.png"
    fig.savefig(dest, bbox_inches="tight", facecolor="white")
    print(dest)


if __name__ == "__main__":
    main()
