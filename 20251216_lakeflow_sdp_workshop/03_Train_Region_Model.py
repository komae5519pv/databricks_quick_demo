# Databricks notebook source
# ノートブック名: 02_Train_Region_Model
# 役割: 受け取った地域ごとに売上予測モデルを学習する（※ デモ用シミュレーション）

import time
import random
import matplotlib.pyplot as plt
import numpy as np

# 1. パラメータの受け取り
# Job の For Each から地域名が渡される想定
dbutils.widgets.text("region_name", "Default_Region", "処理対象の地域")
region = dbutils.widgets.get("region_name")

print("==========================================")
print(f"🚀 モデル学習を開始します（地域: {region}）")
print("==========================================")

# 2. AI学習のシミュレーション（ランダムな待機時間）
# 実行時間にばらつきを持たせることで、並列実行のリアル感を演出
wait_time = random.randint(15, 45)

# ログを出力して「学習している感」を演出
for epoch in range(1, 6):
    print(f"エポック {epoch}/5：学習中です… loss={random.random():.4f}")
    time.sleep(wait_time / 5)  # 小刻みに待機

# 3. 【見せ場】学習曲線のグラフ描画（※ 文字化け防止のため英語表記）
x = np.linspace(0, 10, 100)
y = np.exp(-0.5 * x) + 0.1 * np.random.normal(size=100)

plt.figure(figsize=(10, 4))
plt.plot(x, y, label="Training Loss", linewidth=2)
plt.title(f"Model Training Result: {region}", fontsize=15)
plt.xlabel("Epoch")
plt.ylabel("Loss")
plt.grid(True, linestyle="--", alpha=0.6)
plt.legend()
plt.show()  # ← デモではここが重要

print(f"✅ 地域「{region}」のモデルをモデルレジストリに登録しました（想定）")
