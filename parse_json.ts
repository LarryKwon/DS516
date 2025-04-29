import * as fs from 'fs';
import * as path from 'path';

// 중괄호 매칭해서 JSON 덩어리 자르는 함수
function splitJsons(line: string): string[] {
  const results: string[] = [];
  let depth = 0;
  let start = 0;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '{') {
      if (depth === 0) start = i;
      depth++;
    } else if (char === '}') {
      depth--;
      if (depth === 0) {
        results.push(line.slice(start, i + 1));
      }
    }
  }
  return results;
}

const inputPath = path.join(__dirname, 'data_for_kyungmi.csv');
const outputDir = path.join(__dirname, 'output_per_game');

if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir);
}

const readStream = fs.createReadStream(inputPath, { encoding: 'utf8' });

let buffer = '';

// 🔥 parseLine: 한 줄 읽어서 파일로 저장
function parseLineAndWrite(input: string): void {
  try {
    let index = input.indexOf(',');
    const gameId = index === -1 ? input : input.slice(0, index);
    let rest = index === -1 ? '' : input.slice(index + 1);

    index = rest.indexOf(',');
    const tournamentId = index === -1 ? rest : rest.slice(0, index);
    rest = index === -1 ? '' : rest.slice(index + 1);

    index = rest.indexOf(',');
    const gameCreation = index === -1 ? rest : rest.slice(0, index);
    rest = index === -1 ? '' : rest.slice(index + 1);

    index = rest.indexOf(',');
    const gamePatchShort = index === -1 ? rest : rest.slice(0, index);
    rest = index === -1 ? '' : rest.slice(index + 1);

    const jsoncolumns = splitJsons(rest);

    if (jsoncolumns.length > 1) {
      const stats = JSON.parse(jsoncolumns[0]);
      const details = JSON.parse(jsoncolumns[1]);

      const merged = {
        gameId,
        tournamentId,
        gameCreation,
        gamePatchShort,
        stats,
        details,
      };

      const outputFilePath = path.join(outputDir, `${gameId}.json`);
      fs.writeFileSync(outputFilePath, JSON.stringify(merged, null, 2), 'utf8');
    }
  } catch (err) {
    console.error('파싱 실패:', err, input);
  }
}

// CSV 라인별 처리
readStream.on('data', (chunk) => {
  buffer += chunk;
  const lines = buffer.split('\n');
  buffer = lines.pop()!;

  for (const line of lines) {
    if (!line.trim()) continue;
    parseLineAndWrite(line);
  }
});

readStream.on('end', () => {
  if (buffer.trim()) {
    parseLineAndWrite(buffer);
  }
  console.log('게임별 JSON 파일 저장 완료!');
});
