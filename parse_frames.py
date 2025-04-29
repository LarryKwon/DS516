import os
import json
import pandas as pd
from pathlib import Path

def process_participant_frames(json_path: Path, output_dir: Path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    game_id = data.get("gameId", json_path.stem)
    frames = data.get("details", {}).get("frames", [])
    participants = data.get("stats", {}).get("participants", [])
    id_to_champion = {str(p["participantId"]): p.get("championName") for p in participants}

    rows = []
    for frame in frames:
        timestamp = frame.get("timestamp")
        participant_frames = frame.get("participantFrames", {})
        for pid, pframe in participant_frames.items():
            row = {
                "gameId": game_id,
                "timestamp": timestamp,
                "participantId": pid,
                "championName": id_to_champion.get(str(pid))
            }
            for key, value in pframe.items():
                if isinstance(value, dict):
                    for sub_key, sub_val in value.items():
                        row[f"{key}.{sub_key}"] = sub_val
                else:
                    row[key] = value
            rows.append(row)

    df = pd.DataFrame(rows)
    output_path = output_dir / f"{game_id}_participant_frames.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ {output_path.name} 저장 완료")

# 사용 예시
input_dir = Path("./output_per_game")  # JSON 파일이 들어있는 폴더
output_dir = Path("./participant_frames_output")
output_dir.mkdir(exist_ok=True)

for file in input_dir.glob("*.json"):
    try:
        process_participant_frames(file, output_dir)
    except Exception as e:
        print(f"❌ {file.name} 처리 중 오류 발생: {e}")
