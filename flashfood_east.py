from curl_cffi import requests
import pandas as pd
from datetime import datetime
import time
import random
import pytz
import os

# --- 配置 (美东版) ---
CITY_CONFIG = {
    "Toronto": {"coords": (43.8083, -79.2317), "tz": "America/Toronto"},
    "Detroit": {"coords": (42.3314, -83.0458), "tz": "America/Detroit"},
}

HEADERS = {
    "Host": "app.shopper.flashfood.com",
    "x-ff-api-key": "wEqsr63WozvJwNV4XKPv",
    "flashfood-app-info": "app/shopper,appversion/3.3.3,appbuild/37021,os/ios,osversion/18.5,devicemodel/Apple_iPhone13_2",
    "User-Agent": "Flashfood/37021 CFNetwork/3826.500.131 Darwin/24.5.0",
}


def get_city_data(city_name, lat, lon):
    print(f"\n [正在抓取] {city_name}")

    # 锁定当地研究时间点
    tz = pytz.timezone(CITY_CONFIG[city_name]["tz"])
    now_local = datetime.now(tz)
    h = now_local.hour

    if h < 12:
        slot = "09:30"
    elif h < 18:
        slot = "16:30"
    else:
        slot = "20:30"

    local_day = now_local.strftime("%Y-%m-%d")
    local_time_aligned = f"{local_day} {slot}"


    beijing_tz = pytz.timezone("Asia/Shanghai")
    beijing_now = datetime.now(beijing_tz).strftime("%Y-%m-%d %H:%M")

    all_items = []
    try:
        r_stores = requests.get("https://app.shopper.flashfood.com/api/v1/stores",
                                headers=HEADERS,
                                params={"searchLatitude": str(lat), "searchLongitude": str(lon),
                                        "maxDistance": "50000"},
                                impersonate="chrome110", timeout=20)
        stores_list = r_stores.json().get("data", [])
        store_ids = [s.get("id") for s in stores_list if s.get("id")][:20]

        if not store_ids:
            return [], slot, local_day

        r_items = requests.get("https://app.shopper.flashfood.com/api/v1/items",
                               headers=HEADERS,
                               params=[("storeIds", s_id) for s_id in store_ids],
                               impersonate="chrome110", timeout=20)
        items_data_dict = r_items.json().get("data", {})

        if isinstance(items_data_dict, dict):
            for s_id, items_list in items_data_dict.items():
                for item in items_list:
                    store_obj = item.get("store", {})
                    all_items.append({
                        "city": city_name,
                        "beijing_time": beijing_now,
                        "local_time": local_time_aligned,
                        "store_id": s_id,
                        "store_name": store_obj.get("name", "Unknown"),
                        "item_id": item.get("id"),
                        "item_name": item.get("name"),
                        "price": item.get("price"),
                        "original_price": item.get("originalPrice"),
                        "stock": item.get("quantityAvailable"),
                        "category": item.get("legacyDepartment")
                    })
        return all_items, slot, local_day
    except Exception as e:
        print(f" {city_name} 错误: {e}")
        return [], "unknown", "unknown"


if __name__ == "__main__":
    # 首城漂移 0-3 分钟
    first_drift = random.randint(20, 180)
    print(f" 启动漂移: {first_drift}s...")
    time.sleep(first_drift)

    final_results = []
    current_slot = ""
    current_day = ""

    cities = list(CITY_CONFIG.keys())
    for i, city in enumerate(cities):
        data, slot, l_day = get_city_data(city, *CITY_CONFIG[city]["coords"])
        current_slot = slot
        current_day = l_day.replace("-", "")
        if data:
            final_results.extend(data)
        if i < len(cities) - 1:
            time.sleep(random.uniform(20, 40))

    if final_results:
        if not os.path.exists("data"):
            os.makedirs("data")

        file_slot = current_slot.replace(":", "")
        fn = f"data/west_{current_day}-{file_slot}.csv"
        pd.DataFrame(final_results).to_csv(fn, index=False, encoding="utf_8_sig")
        print(f"完成并保存: {fn}")