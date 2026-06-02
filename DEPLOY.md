# Jensen Tracker — 배포 가이드

## 프로젝트 개요
- FastAPI 백엔드 (`backend/main.py`) + HTML 프론트엔드 (`jensen-tracker.html`)
- Claude (Haiku) × Gemini 듀얼 AI로 뉴스 분석 후 국장/미장 수혜주 추천
- GitHub: https://github.com/aichoboking/Jenson-tracker

## 환경변수 (배포 시 반드시 입력)
```
ANTHROPIC_API_KEY=...
GEMINI_API_KEY=...
```

## 미완료 수정 사항 (집에서 이어서)
- Gemini 모델: 현재 `gemini-2.5-flash-lite` → `gemini-2.0-flash`로 교체 권장
  - 위치: `backend/main.py` line ~877
  - 이유: 2.5-flash-lite는 미리보기 모델이라 503 과부하 잦음

## Railway 배포 순서
1. https://railway.app 가입 (GitHub 계정으로 로그인)
2. New Project → Deploy from GitHub repo
3. `aichoboking/Jenson-tracker` 선택
4. Variables 탭 → 환경변수 2개 입력
5. 자동 배포 완료 → 생성된 URL로 접속

## 로컬 실행 (집 PC에서 테스트할 때)
```bash
git clone https://github.com/aichoboking/Jenson-tracker
cd Jenson-tracker/backend
pip install -r requirements.txt
uvicorn main:app --reload
```

## 주요 수정 이력
- KST 날짜 필터 적용: UTC→KST 변환 정상화, 오늘 기사만 수집
- Gemini RPD: gemini-2.0-flash 기준 1500회/일 (캐시로 실제 호출 적음)
- 분석 캐시 TTL: 1시간 (같은 뉴스셋 재호출 없음)
