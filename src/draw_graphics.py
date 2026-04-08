import json
import os
import matplotlib.pyplot as plt
import numpy as np

def plot_heatmap(input_file="report/metrics_report.json", output_dir="report/images"):
    if not os.path.exists(input_file):
        print(f"File {input_file} not found! Run your Spark jobs first.")
        return

    with open(input_file, "r") as f:
        data = json.load(f)

    if not data:
        print("No data to plot in the JSON file.")
        return

    # Извлекаем все уникальные названия стадий и сортируем их (ось X)
    all_stages = set()
    for run in data:
        all_stages.update(run.get("stages_timing", {}).keys())
    stages = list(all_stages)
    stages.sort()

    # Извлекаем названия экспериментов (ось Y)
    experiments = [run["experiment"] for run in data]

    # Готовим 2D массив данных: строки - эксперименты, столбцы - стадии
    matrix = np.zeros((len(experiments), len(stages)))
    for i, run in enumerate(data):
        for j, stage in enumerate(stages):
            matrix[i, j] = run.get("stages_timing", {}).get(stage, 0)

    # Настраиваем размер графика динамически
    fig, ax = plt.subplots(figsize=(10 + len(stages) * 0.5, max(5, len(experiments) * 1.5)))

    # Рисуем хитмапу (цветовая схема YlOrRd: от светло-желтого к темно-красному)
    cax = ax.imshow(matrix, cmap="YlOrRd", aspect="auto")

    # Настраиваем тики (метки осей)
    ax.set_xticks(np.arange(len(stages)))
    ax.set_yticks(np.arange(len(experiments)))
    ax.set_xticklabels(stages, rotation=45, ha="right")
    ax.set_yticklabels(experiments)

    ax.set_title("Pipeline Stages Duration Heatmap", pad=20, fontsize=14)
    ax.set_xlabel("Pipeline Stages", fontsize=12)
    ax.set_ylabel("Experiments", fontsize=12)

    # Добавляем цветовую шкалу сбоку
    cbar = fig.colorbar(cax, ax=ax, fraction=0.046, pad=0.04)
    cbar.ax.set_ylabel('Duration (Seconds)', rotation=-90, va="bottom")

    # Проходим по всем ячейкам и пишем в них текст (время)
    # Порог для смены цвета текста (чтобы на темном фоне текст был белым, а на светлом - черным)
    threshold = matrix.max() / 2.
    for i in range(len(experiments)):
        for j in range(len(stages)):
            val = matrix[i, j]
            # Форматируем число: 1 знак после запятой
            text_color = "white" if val > threshold else "black"
            ax.text(j, i, f"{val:.1f}", ha="center", va="center", color=text_color, fontweight='bold')

    # Убираем рамку графика для эстетики
    for edge, spine in ax.spines.items():
        spine.set_visible(False)

    # Делаем сетку, чтобы ячейки были разделены (белые линии)
    ax.set_xticks(np.arange(matrix.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(matrix.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=2)
    ax.tick_params(which="minor", bottom=False, left=False)

    plt.tight_layout()

    # Сохраняем график
    os.makedirs(output_dir, exist_ok=True)
    plot_file = os.path.join(output_dir, "metrics_heatmap.png")
    plt.savefig(plot_file, bbox_inches="tight", dpi=300)
    print(f"Heatmap successfully saved to {plot_file}")

    # Показываем окно с графиком
    plt.show()

if __name__ == "__main__":
    plot_heatmap()
