import os
import json
import pandas as pd
from pathlib import Path

json_dir = Path('./output_per_game') 
output_csv = './merged_events.csv'

# 전체 이벤트 누적 리스트
all_events = []

# 디렉토리 내 모든 .json 파일 순회
for file_path in json_dir.glob('*.json'):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # game_id = data.get("gameId", file_path.stem)
        game_id = file_path.stem
        frames = data.get("details", {}).get("frames", [])

        for frame in frames:
            for event in frame.get("events", []):
                flat_event = {
                    "gameId": game_id,
                    "timestamp": event.get("timestamp")
                }
                for key, value in event.items():
                    if key != "timestamp" and key!="gameId":
                        flat_event[key] = value
                all_events.append(flat_event)

    except Exception as e:
        print(f"[오류] {file_path.name}: {e}")

# pandas DataFrame으로 변환 및 저장
df = pd.json_normalize(all_events)
df.to_csv(output_csv, index=False)
print(f"✅ 모든 이벤트가 {output_csv} 에 저장되었습니다.")
