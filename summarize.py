import pandas as pd
import glob
import os


def generate_daily_summary():
    # 1. 扫描 data 文件夹下所有的原始碎片文件
    all_files = glob.glob("data/*.csv")
    if not all_files:
        print(" data 文件夹暂无数据，跳过汇总。")
        return

    # 2. 合并所有数据，并提取逻辑日期和时段
    dfs = []
    for f in all_files:
        try:
            df = pd.read_csv(f)
            df['slot'] = df['local_time'].str[-5:]  # 提取 09:30 等
            df['local_day_clean'] = df['local_time'].str[:10].str.replace("-", "")  # 提取 20260208
            dfs.append(df)
        except Exception as e:
            print(f"读取 {f} 出错: {e}")

    if not dfs:
        return

    full_df = pd.concat(dfs, ignore_index=True)

    # 3. 按 (逻辑日期, 城市) 分组
    for (day_str, city), group in full_df.groupby(['local_day_clean', 'city']):

        # 4. 创建按天命名的文件夹，例如 summary/20260208/
        day_dir = os.path.join("summary", day_str)
        if not os.path.exists(day_dir):
            os.makedirs(day_dir)

        # 5. 执行透视变换：将 stock 展开为列
        summary = group.pivot_table(
            index=[
                'item_id', 'item_name', 'store_id', 'store_name',
                'category', 'original_price', 'price'
            ],
            columns='slot',
            values='stock',
            aggfunc='first'
        ).reset_index()

        # 6. 整理库存列名 (09:30 -> stock_0930 等)
        slots_map = {"09:30": "stock_0930", "16:30": "stock_1630", "20:30": "stock_2030"}
        for s_val, s_col in slots_map.items():
            if s_val in summary.columns:
                summary.rename(columns={s_val: s_col}, inplace=True)
            else:
                summary[s_col] = None

        # 7. 计算库存变化列 (2-1, 3-2, 3-1)
        # stock_1630 - stock_0930
        if 'stock_0930' in summary.columns and 'stock_1630' in summary.columns:
            summary['change_2_1'] = summary['stock_1630'] - summary['stock_0930']
        else:
            summary['change_2_1'] = None

        # stock_2030 - stock_1630
        if 'stock_1630' in summary.columns and 'stock_2030' in summary.columns:
            summary['change_3_2'] = summary['stock_2030'] - summary['stock_1630']
        else:
            summary['change_3_2'] = None

        # stock_2030 - stock_0930
        if 'stock_0930' in summary.columns and 'stock_2030' in summary.columns:
            summary['change_3_1'] = summary['stock_2030'] - summary['stock_0930']
        else:
            summary['change_3_1'] = None

        # 8. 最终列排序
        cols_to_keep = [
            'item_id', 'item_name', 'store_id', 'store_name',
            'original_price', 'price', 'category',
            'stock_0930', 'stock_1630', 'stock_2030',
            'change_2_1', 'change_3_2', 'change_3_1'
        ]
        final_cols = [c for c in cols_to_keep if c in summary.columns]
        summary = summary[final_cols]

        # 9. 保存文件：summary/20260208/Toronto.csv
        output_path = os.path.join(day_dir, f"{city}.csv")
        summary.to_csv(output_path, index=False, encoding="utf_8_sig")
        print(f" 已更新日期文件夹数据: {output_path}")


if __name__ == "__main__":
    generate_daily_summary()