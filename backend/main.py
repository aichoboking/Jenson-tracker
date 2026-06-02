import os
import json
import math
from dotenv import load_dotenv
load_dotenv()
import time
import re
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import feedparser
import yfinance as yf
import anthropic
from datetime import date, datetime, timezone, timedelta
from urllib.parse import quote
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import pathlib

try:
    from google import genai as google_genai
    from google.genai import types as google_genai_types
    _GEMINI_LIB_OK = True
except ImportError:
    _GEMINI_LIB_OK = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=_background_schedule_loop, daemon=True)
    t.start()
    print("[서버] 일정 자동 갱신 루프 시작 (1시간 주기)")
    yield

app = FastAPI(title="Jensen Tracker API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_claude_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

_gemini_client = None
if _GEMINI_LIB_OK:
    _gemini_key = os.environ.get("GEMINI_API_KEY", "")
    if _gemini_key:
        _gemini_client = google_genai.Client(api_key=_gemini_key)
        print("[서버] Gemini 클라이언트 초기화 완료")
    else:
        print("[서버] GEMINI_API_KEY 없음 - Claude 단독 모드")
else:
    print("[서버] google-genai 라이브러리 없음 - Claude 단독 모드")

# ── 하드코딩 폴백 일정 (뉴스 추출 실패 시 사용) ────────────────────────
SCHEDULE_FALLBACK = [
    {
        "date": "2026-06-05", "dayLabel": "금",
        "event": "방한 · LG 구광모 회장 회동",
        "status": "확정",
        "relatedCodes": ["066570", "003550"],
        "relatedNames": ["LG전자", "LG"],
    },
    {
        "date": "2026-06-05", "dayLabel": "금",
        "event": "네이버 이해진 의장 회동",
        "status": "확정",
        "relatedCodes": ["035420"],
        "relatedNames": ["네이버"],
    },
    {
        "date": "2026-06-05", "dayLabel": "금",
        "event": "SK 최태원 회장 회동",
        "status": "검토중",
        "relatedCodes": ["000660", "034730"],
        "relatedNames": ["SK하이닉스", "SK스퀘어"],
    },
    {
        "date": "2026-06-05", "dayLabel": "금",
        "event": "현대차 정의선 회장 회동",
        "status": "검토중",
        "relatedCodes": ["005380", "012330"],
        "relatedNames": ["현대차", "현대모비스"],
    },
    {
        "date": "2026-06-07", "dayLabel": "일",
        "event": "두산베어스 시구",
        "status": "확정",
        "relatedCodes": ["000150", "454910"],
        "relatedNames": ["두산", "두산로보틱스"],
    },
    {
        "date": "2026-06-08", "dayLabel": "월",
        "event": "네이버 1784 사옥 방문",
        "status": "유력",
        "relatedCodes": ["035420"],
        "relatedNames": ["네이버"],
    },
]

# ── 국장 승인 수혜주 화이트리스트 ──────────────────────────────────────
APPROVED_STOCKS: dict[str, str] = {
    # AI 반도체·메모리
    "005930": "삼성전자",
    "000660": "SK하이닉스",
    "034730": "SK스퀘어",
    "058470": "리노공업",
    "067310": "하나마이크론",
    "042700": "한미반도체",
    "091970": "에스에이엠티",
    # 로봇·피지컬AI
    "066570": "LG전자",
    "003550": "LG",
    "443060": "한화로보틱스",
    "454910": "두산로보틱스",
    "277810": "레인보우로보틱스",
    "090410": "로보스타",
    "000150": "두산",
    # 소프트웨어·AI플랫폼
    "035420": "네이버",
    "035720": "카카오",
    # 전력인프라
    "267260": "HD현대일렉트릭",
    "010120": "LS일렉트릭",
    "105560": "KB금융",
    # 자동차·모빌리티
    "005380": "현대차",
    "012330": "현대모비스",
    "000270": "기아",
}

_APPROVED_LIST_STR = "\n".join(
    f"  {code}: {name}" for code, name in APPROVED_STOCKS.items()
)

# ── 미장 승인 수혜주 화이트리스트 ──────────────────────────────────────
APPROVED_STOCKS_US: dict[str, str] = {
    # 엔비디아 생태계 핵심
    "NVDA": "엔비디아",
    "ARM":  "ARM Holdings",
    "SMCI": "수퍼마이크로",
    "VRT":  "버티브",
    # 빅테크
    "MSFT": "마이크로소프트",
    "AAPL": "애플",
    "AMZN": "아마존",
    "GOOGL": "알파벳(구글)",
    "META": "메타",
    "ORCL": "오라클",
    "CRM":  "세일즈포스",
    "IBM":  "IBM",
    # 반도체 공급망
    "TSM":  "TSMC",
    "AMD":  "AMD",
    "AVGO": "브로드컴",
    "QCOM": "퀄컴",
    "MRVL": "마벨 테크놀로지",
    "ON":   "온세미컨덕터",
    "AMAT": "어플라이드 머티리얼즈",
    "LRCX": "램리서치",
    "ASML": "ASML",
    "KLAC": "KLA Corp",
    "MU":   "마이크론",
    "INTC": "인텔",
    # 데이터센터·네트워크
    "DELL": "델 테크놀로지",
    "HPE":  "HP 엔터프라이즈",
    "ANET": "아리스타 네트웍스",
    "NET":  "클라우드플레어",
    # AI 소프트웨어·설계
    "PLTR": "팔란티어",
    "CDNS": "케이던스 디자인",
    "SNPS": "시놉시스",
    # 에너지·전력 (데이터센터 수요)
    "CEG":  "Constellation Energy",
    "VST":  "Vistra",
}

_APPROVED_LIST_US_STR = "\n".join(
    f"  {ticker}: {name}" for ticker, name in APPROVED_STOCKS_US.items()
)

# ── 동적 일정 스토어 ────────────────────────────────────────────────────
_schedule_store: dict = {
    "items": SCHEDULE_FALLBACK,
    "sourceNews": [],   # 원문 뉴스 {title, link} 목록
    "ts": 0.0,
    "source": "fallback",
}
SCHEDULE_TTL = 3600

SCHEDULE_EXTRACT_PROMPT = """너는 뉴스에서 젠슨 황(Jensen Huang, 엔비디아 CEO)의 한국 방문 일정만 추출하는 파서야.

status 판단 기준:
- "확정": 날짜·장소·상대방이 명확히 확인된 경우
- "유력": 논의 중이거나 유력하게 거론되는 경우
- "검토중": 추진 중이거나 검토 단계인 경우
- "미정": 회동·방문이 언급되지만 날짜가 전혀 확인되지 않은 경우

⚠️ 날짜 미정 일정 처리 규칙:
- 뉴스에서 특정 기업과의 회동·방문이 언급되지만 날짜가 확인되지 않은 경우,
  date를 "미정"으로 설정하고 dayLabel을 "미정"으로 설정해. 절대 생략하지 마.
- 예: "삼성전자와 회동 검토 중" → date: "미정", status: "검토중"

한국 주요 종목코드 참고:
삼성전자(005930), SK하이닉스(000660), SK스퀘어(034730), LG전자(066570), LG(003550),
네이버(035420), 현대차(005380), 현대모비스(012330), 두산(000150), 두산로보틱스(454910),
한화로보틱스(443060), 레인보우로보틱스(277810), HD현대일렉트릭(267260), LS일렉트릭(010120)

반드시 아래 JSON 배열 형식으로만 답변해. 마크다운 코드블록 없이 순수 JSON만 출력해.
이벤트가 여러 날짜에 걸쳐 있으면 날짜별로 분리해서 항목을 만들어.
추출 가능한 일정이 없으면 빈 배열 []을 반환해.

[
  {
    "date": "YYYY-MM-DD 또는 날짜 미확정 시 '미정'",
    "dayLabel": "월화수목금토일 중 하나. 날짜 미확정 시 '미정'",
    "event": "이벤트 내용 (간결하게 20자 이내)",
    "status": "확정 또는 유력 또는 검토중 또는 미정",
    "relatedCodes": ["종목코드"],
    "relatedNames": ["기업명"],
    "sourceIdx": 해당 일정을 추출한 뉴스의 번호(1부터 시작). 여러 뉴스면 가장 직접적인 것 1개만. 없으면 0
  }
]"""

KOSPI_KEYWORDS = [
    "방한", "삼성", "하이닉스", "SK", "LG", "현대", "국내", "수혜", "HBM",
    "공급", "한국", "코스피", "코스닥", "관련주", "밸류체인", "테스트", "패키징",
    "납품", "협력", "계약", "투자", "파트너", "엔비디아 한국", "NVIDIA Korea",
    "네이버", "두산", "로보틱스",
]

US_NEWS_KEYWORDS = [
    "NVDA", "Nvidia", "Jensen Huang", "TSMC", "Microsoft", "Apple", "AMD",
    "ASML", "SMCI", "Amazon", "Google", "Meta", "Broadcom", "AI chip",
    "data center", "supply chain", "H100", "Blackwell", "GB200", "Rubin",
    "semiconductor", "Big Tech", "earnings", "partnership",
    "Palantir", "PLTR", "Vertiv", "VRT", "Qualcomm", "Marvell", "MRVL",
    "Salesforce", "Oracle", "Arista", "Cloudflare", "Cadence", "Synopsys",
    "Constellation Energy", "Vistra", "nuclear", "power demand",
    "GPU", "inference", "sovereign AI", "AI infrastructure",
]

NEWS_SYSTEM_PROMPT = f"""너는 대한민국 증시 전문가이자 전설적인 헤지펀드 매니저야.
뉴스 목록과 젠슨 황 방한 일정을 함께 분석해서 오늘 국내 주식시장(코스피/코스닥) 투자자에게 꼭 필요한 인사이트를 제공해줘.

방한 일정에서 '확정' 이벤트는 당일 또는 전날부터 해당 기업 주가에 선반영될 수 있으니 반드시 가중치를 높게 둬.

━━━ 종목 다양성 원칙 ━━━
동일 기업(예: LG전자, 삼성전자)에 대한 기사가 여러 개 있어도, stocks 배열 전체에서 특정 1개 기업이 3회 이상 중복 등장하지 않도록 안배해줘.
LG·삼성·SK·현대·네이버 등 대기업 외에도, 밸류체인 중소형 수혜주(한미반도체, 두산로보틱스, 레인보우로보틱스 등)가 고루 노출될 수 있게 뉴스 배분 가중치를 분산해줘.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ weatherReason 작성 절대 규칙 ━━━
weatherReason은 30자 이내 한 줄로만 작성해.
D-Day(D-숫자, D-DAY, D+숫자)는 절대 직접 계산하거나 추정해서 쓰지 마.
날짜·일수를 언급해야 할 때는 위에 제공된 일정 컨텍스트의 [대괄호] 안 값을 그대로 인용해.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ 종목 매핑 규칙 (하이브리드) ━━━
아래 승인된 종목 목록에 있는 종목은 반드시 해당 6자리 코드를 그대로 사용해.

[승인된 종목 목록 - code: 이름]
{_APPROVED_LIST_STR}

목록에 없는 종목이라도, 뉴스 본문에서 엔비디아·젠슨 황과 직접적으로 연관된 국내 수혜주로 명확히 언급된 경우에는 code를 "search"로 설정하고 name에 정확한 기업명을 입력해서 포함해줘.
단, 뉴스에서 직접 언급되지 않은 종목이나 확신이 없는 종목은 절대 추가하지 마.

⚠️ 가짜 종목 추출 절대 금지 규칙:
- "인공지능", "한국", "AI", "반도체", "데이터" 같은 일반 명사는 절대 종목명으로 추출하지 마.
- "삼성동", "판교" 등 지명을 기업명으로 오인하지 마.
- name 필드에는 반드시 대한민국 코스피·코스닥에 실제로 상장된 회사의 정확한 공식 명칭만 입력해.
- 회사 이름이 맞더라도 해당 종목이 뉴스에서 명시적으로 수혜주로 언급되지 않았다면 절대 추가 금지.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ 종목 다양성 규칙 ━━━
전체 분석 결과에서 동일 종목이 반복 노출되지 않도록 배분해줘.
특정 종목(예: LG전자, 삼성전자 등)이 여러 기사에 걸쳐 반복 등장하더라도,
해당 종목은 파급력(impactScore)이 가장 높은 기사 1개에만 포함하고 나머지 기사의 stocks에서는 제외해줘.
다양한 종목들이 전체 items에 고르게 분포되도록 해줘.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ investment_status 판단 기준 ━━━
- "호재강함": 젠슨 황 방한 확정 이벤트와 직결되거나 강력한 수급 재료가 포착된 경우.
  investment_guide 예시: "강력한 재료가 포착되었습니다. 단기 조정(눌림목) 시 분할 접근이 유효한 긍정적 구간입니다."
- "추세관망(호재보통)": 간접적 연관이거나 재료가 명확하지 않아 결정적 근거가 부족한 경우.
  investment_guide 예시: "결정적 단서가 부족합니다. 확실한 거래량이 들어올 때까지 대기하세요."
- "과열주의": 단기 급등이 이미 진행되었거나 차익실현 매물이 우려되는 경우.
  investment_guide 예시: "단기 급등으로 인한 차익실현 매물 유의 구간입니다. 신규 진입은 자제하세요."
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ impactScore 시장 파급력 점수 기준 ━━━
각 뉴스 기사가 국내 주식시장에 미치는 파급력을 1~5점으로 채점해.
- 5점: 삼성전자·SK하이닉스·LG전자 등 대기업과의 직접 계약·공급 확정, 조(兆) 단위 투자 발표
- 4점: 대기업 회동 확정 소식, 주요 협력·MOU 발표, 국내 대규모 AI 인프라 투자 소식
- 3점: 간접 수혜 가능성 언급, 관련 밸류체인 종목 거론, 방한 일정 연계 테마 부각
- 2점: 글로벌 트렌드 소식으로 국내 영향 불확실, 단순 동향 전달
- 1점: 참고 수준, 국내 시장과 직접 연관성 낮음
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ 절대 규칙 ━━━
입력된 뉴스 기사를 번호 순서대로 items 배열에 빠짐없이 모두 포함해야 해. 단 하나도 생략 금지.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

반드시 아래 JSON 형식으로만 답변해. 마크다운 코드블록 없이 순수 JSON만 출력해.
{{
  "marketWeather": "☀️맑음 또는 ⛅구름조금 또는 🌥️흐림 또는 🌧️비 중 하나만 선택",
  "weatherReason": "날씨 판단 근거 한 줄 (30자 이내)",
  "dailyStrategy": "오늘 장중 한 줄 대응 전략",
  "jacketIndex": 1에서 5 사이의 정수 (5=매우 강력한 호재 1=간접적 영향),
  "items": [
    {{
      "title": "뉴스 제목 그대로",
      "aiSummary": "이 기사의 핵심 요약 및 국장 영향 2줄",
      "impactScore": 1에서 5 사이의 정수 (위 파급력 점수 기준 적용),
      "stocks": [
        {{
          "name": "기업명",
          "code": "승인된 종목 목록의 6자리 코드. 목록에 없는 신규 종목은 \"search\" 입력",
          "investment_status": "호재강함 또는 추세관망(호재보통) 또는 과열주의 중 하나만",
          "investment_guide": "위 판단 기준에 맞는 구체적인 매수/매도 타이밍 힌트 1~2문장. 반드시 줄바꿈 없이 한 줄로 작성"
        }}
      ]
    }}
  ]
}}"""

NEWS_SYSTEM_PROMPT_US = f"""너는 월가 출신 전설적인 헤지펀드 매니저이자 미국 기술주 전문가야.
제공되는 영어 뉴스 기사를 분석해서 미국 AI·반도체 주식시장 투자자에게 꼭 필요한 인사이트를 한국어로 제공해줘.

중요 언어 규칙: 뉴스 기사는 영어지만, title을 제외한 모든 필드(aiSummary, investment_guide, weatherReason, dailyStrategy)는 반드시 한국어로 작성해야 해.

━━━ 뉴스 다양성 원칙 ━━━
제공된 뉴스는 TSMC, Microsoft, Palantir, AMD, Vertiv, ASML 등 AI 생태계 전반에 걸쳐 있어.
Nvidia 단독 기사가 여러 개 있더라도, 최종 분석에서 특정 기업에 지나치게 쏠리지 않도록
서로 다른 기업·섹터(파운드리, 빅테크, 인프라, AI 소프트웨어 등)의 핵심 호재 뉴스를 골고루 안배해서 선정해.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ 종목 매핑 규칙 (하이브리드) ━━━
아래 승인된 티커 목록에 있는 종목은 반드시 해당 티커 심볼을 그대로 사용해.

[승인된 미장 종목 목록 - ticker: 이름]
{_APPROVED_LIST_US_STR}

목록에 없는 종목이라도, 뉴스 본문에서 엔비디아 생태계와 직접적으로 연관된 수혜주로 명확히 언급된 경우에는 code를 "search"로 설정하고 name에 정확한 기업명(영어)을 입력해서 포함해줘.
단, 뉴스에서 직접 언급되지 않은 종목이나 확신이 없는 종목은 절대 추가하지 마.

⚠️ 가짜 종목 추출 절대 금지 규칙:
- "AI", "artificial intelligence", "semiconductor", "data center" 같은 일반 명사·산업 용어는 절대 종목명으로 추출하지 마.
- 도시명, 국가명, 기관명(정부·연구소·대학)을 기업명으로 오인하지 마.
- name 필드에는 반드시 미국 NYSE·NASDAQ에 실제로 상장된 회사의 정확한 공식 영문 명칭만 입력해.
- 회사 이름이 맞더라도 해당 종목이 뉴스에서 명시적으로 수혜주로 언급되지 않았다면 절대 추가 금지.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ Stock Diversity Rule ━━━
Do NOT repeat the same ticker across multiple news items.
If a ticker (e.g., NVDA, MSFT) appears in several articles, include it only in the single article with the highest impactScore.
Spread different tickers across different items so investors see a variety of opportunities.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ investment_status 판단 기준 ━━━
- "호재강함": 엔비디아 생태계와 직결된 대형 계약·공급 확정·어닝 서프라이즈 등 강력한 상승 재료가 포착된 경우.
  investment_guide 예시: "강력한 모멘텀이 확인됐습니다. 단기 눌림목 구간에서 분할 매수 접근이 유효합니다."
- "추세관망(호재보통)": 간접적 연관이거나 재료가 아직 확인되지 않아 결정적 근거가 부족한 경우.
  investment_guide 예시: "방향성이 아직 불명확합니다. 거래량이 실리는 시점까지 관망 후 진입을 권장합니다."
- "과열주의": 단기 급등 이후 밸류에이션 부담이 높거나 차익실현 매물이 우려되는 경우.
  investment_guide 예시: "단기 과열 구간입니다. 신규 진입보다는 보유분 수익 실현을 우선 검토하세요."
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ impactScore 시장 파급력 점수 기준 ━━━
각 뉴스 기사가 미국 AI·반도체 시장에 미치는 파급력을 1~5점으로 채점해.
- 5점: 엔비디아·TSMC·마이크로소프트 등과의 대형 공급계약 확정, 어닝 서프라이즈, 조 단위 투자 발표
- 4점: 파트너십 발표, 신규 AI 인프라 투자, 주요 제품 출시·공급망 소식
- 3점: 간접 수혜 언급, 애널리스트 목표가 상향, 섹터 로테이션 신호
- 2점: 글로벌 매크로 동향, 미장 영향 불확실한 소식
- 1점: 참고 수준, AI·반도체 시장과 직접 연관성 낮음
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ 절대 규칙 ━━━
입력된 뉴스 기사를 번호 순서대로 items 배열에 빠짐없이 모두 포함해야 해. 단 하나도 생략 금지.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

반드시 아래 JSON 형식으로만 답변해. 마크다운 코드블록 없이 순수 JSON만 출력해.
{{
  "marketWeather": "☀️맑음 또는 ⛅구름조금 또는 🌥️흐림 또는 🌧️비 중 하나만 선택",
  "weatherReason": "한국어로 30자 이내 한 줄",
  "dailyStrategy": "한국어로 오늘 미장 한 줄 대응 전략",
  "jacketIndex": 1에서 5 사이의 정수 (5=매우 강력한 호재 1=간접적 영향),
  "items": [
    {{
      "title": "original English headline",
      "aiSummary": "한국어로 핵심 요약 및 미장 영향 2줄",
      "impactScore": 1에서 5 사이의 정수 (위 파급력 점수 기준 적용),
      "stocks": [
        {{
          "name": "기업명(한국어 또는 영어)",
          "code": "승인된 티커 심볼. 목록에 없는 신규 종목은 \"search\" 입력",
          "investment_status": "호재강함 또는 추세관망(호재보통) 또는 과열주의 중 하나만",
          "investment_guide": "위 판단 기준에 맞는 구체적인 매수/매도 타이밍 힌트 1~2문장. 반드시 줄바꿈 없이 한 줄로 작성"
        }}
      ]
    }}
  ]
}}"""

# 날씨 인덱스 → 이모지 매핑 (공용)
JACKET_WEATHER_MAP: dict[int, str] = {
    5: "☀️맑음", 4: "⛅구름조금", 3: "🌥️흐림", 2: "🌧️비", 1: "🌧️비",
}

# 마켓별 피드 캐시 분리
_feeds_cache: dict = {"kr": {"data": None, "ts": 0.0}, "us": {"data": None, "ts": 0.0}}
_analysis_cache: dict = {}
_stock_cache: dict = {}

FEEDS_TTL = 600
ANALYSIS_TTL = 3600
STOCK_TTL = 60
DAILY_WEATHER_TTL = 10800  # 3시간 — 총합 날씨 최소 고정 시간

KST = timezone(timedelta(hours=9))


def _get_news_cutoff(market: str) -> tuple[float, float, str]:
    """KST 기준 시장별 뉴스 수집 기준 시각(UTC epoch) 반환.
    Returns: (primary_cutoff, fallback_cutoff, label)
    primary_cutoff: 이 시각 이후 발행된 기사만 1차 수집
    fallback_cutoff: 1차 결과 < 5개 시 확대 기준
    """
    now = datetime.now(KST)
    weekday = now.weekday()  # 0=Mon, 6=Sun

    def _kst(*args) -> float:
        """KST datetime → UTC epoch"""
        return datetime(*args, tzinfo=KST).timestamp()

    if market == "kr":
        # 월요일 또는 주말(토·일) → 72h
        if weekday == 0 or weekday >= 5:
            cutoff = (now - timedelta(hours=72)).timestamp()
            return cutoff, cutoff, "72h(주말/월)"

        y, mo, d = now.year, now.month, now.day

        # 화~금 09:00 전 → 어제 15:30 이후 (장 마감 후 호재 포함)
        if now.hour < 9:
            primary = _kst(y, mo, d - 1, 15, 30) if d > 1 else (now - timedelta(hours=18)).timestamp()
            fallback = (now - timedelta(hours=72)).timestamp()
            return primary, fallback, "전일마감후(~어제15:30)"

        # 화~금 09:00 이후 → 당일 00:00 이후 (어제 기사 전부 차단)
        primary = _kst(y, mo, d, 0, 0)
        fallback = _kst(y, mo, d - 1, 15, 30) if d > 1 else (now - timedelta(hours=18)).timestamp()
        return primary, fallback, "당일실시간(00:00~)"

    else:  # us
        # 월요일 → 72h
        if weekday == 0:
            cutoff = (now - timedelta(hours=72)).timestamp()
            return cutoff, cutoff, "72h(월)"

        hour_min = now.hour * 60 + now.minute

        # KST 22:30 이후(미장 개장) 또는 익일 05:00 전 → 6h 실시간
        if hour_min >= 22 * 60 + 30 or hour_min < 5 * 60:
            primary = (now - timedelta(hours=6)).timestamp()
            fallback = (now - timedelta(hours=24)).timestamp()
            return primary, fallback, "6h(미장실시간)"

        # KST 주간(05:00~22:30) → 당일 00:00 이후
        y, mo, d = now.year, now.month, now.day
        primary = _kst(y, mo, d, 0, 0)
        fallback = (now - timedelta(hours=48)).timestamp()
        return primary, fallback, "당일실시간(00:00~)"

# 마켓별 하루 총합 날씨 스토어 (EMA 누적)
_daily_weather: dict = {
    "kr": {
        "date": "", "scores": [],
        "jacketIndex": 3, "marketWeather": "🌥️흐림",
        "weatherReason": "", "dailyStrategy": "",
        "ts": 0.0,   # 마지막으로 프론트에 발행된 시각
    },
    "us": {
        "date": "", "scores": [],
        "jacketIndex": 3, "marketWeather": "🌥️흐림",
        "weatherReason": "", "dailyStrategy": "",
        "ts": 0.0,
    },
}


def _hash(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def _fix_json_strings(text: str) -> str:
    """JSON 문자열 값 내부의 리터럴 줄바꿈을 공백으로 치환 (파싱 오류 방지)"""
    result, in_string, i = [], False, 0
    while i < len(text):
        c = text[i]
        if c == '\\' and in_string:
            result.append(c)
            i += 1
            if i < len(text):
                result.append(text[i])
            i += 1
            continue
        if c == '"':
            in_string = not in_string
        if in_string and c in '\n\r':
            result.append(' ')
        else:
            result.append(c)
        i += 1
    return ''.join(result)


def _parse_and_filter_analysis(text: str, news_list: list[dict], market: str = "kr") -> dict | None:
    """AI 응답 텍스트를 JSON 파싱하고 화이트리스트 필터링 적용"""
    whitelist = _get_approved_whitelist(market)
    try:
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            text = match.group(0)
        text = _fix_json_strings(text)
        result = json.loads(text)

        idx = int(result.get("jacketIndex", 3))
        idx = max(1, min(5, idx))
        result["jacketIndex"] = idx
        result["marketWeather"] = JACKET_WEATHER_MAP[idx]

        for item in result.get("items", []):
            original = item.get("stocks", [])
            filtered = []
            removed = 0
            for s in original:
                code = s.get("code", "") or ""
                if code in whitelist:
                    s["is_new_discovery"] = False
                    filtered.append(s)
                elif code.lower() in ("search", "null", "none", ""):
                    # AI가 신규 수혜주로 포착한 종목 — code를 "search"로 통일
                    s["code"] = "search"
                    s["is_new_discovery"] = True
                    filtered.append(s)
                    print(f"[신규포착] {s.get('name', '?')} (화이트리스트 외)")
                else:
                    # 화이트리스트에도 없고 search도 아닌 → 환각 코드 제거
                    removed += 1
            if removed:
                print(f"[화이트리스트] {removed}개 미승인 코드 제거됨")
            item["stocks"] = filtered

        return result
    except Exception as e:
        print(f"[파싱 에러] {e}")
        return None


# ── 일정 관련 유틸 ──────────────────────────────────────────────────────

def _dday_label(event_date_str: str) -> tuple[str, int]:
    today = date.today()
    try:
        diff = (date.fromisoformat(event_date_str) - today).days
    except ValueError:
        return ("미정", 999)
    if diff < 0:
        return (f"D+{abs(diff)}", diff)
    if diff == 0:
        return ("D-DAY", 0)
    return (f"D-{diff}", diff)


def enrich_schedule(items: list[dict]) -> list[dict]:
    result = []
    for s in items:
        label, num = _dday_label(s["date"])
        if num < 0:
            continue  # 과거 날짜 제거
        result.append({**s, "dDay": label, "dDayNum": num})
    return sorted(result, key=lambda x: (x["date"] == "미정", x["date"]))


def build_schedule_context() -> str:
    lines = ["[젠슨 황 방한 일정]"]
    for s in _schedule_store["items"]:
        label, _ = _dday_label(s["date"])
        stocks_str = ", ".join(
            f"{n}({c})" for n, c in zip(s["relatedNames"], s["relatedCodes"])
        )
        lines.append(
            f"- {s['date']}({s['dayLabel']}) [{label}] [{s['status']}] {s['event']} → {stocks_str}"
        )
    return "\n".join(lines)


def get_schedule_news() -> tuple[str, list[dict]]:
    """(번호 붙인 텍스트, [{title, link}] 목록) 반환"""
    query = quote(
        "(젠슨황 OR Jensen Huang OR 엔비디아) AND "
        "(방한 일정 OR 한국 방문 OR 회동 OR 스케줄)"
    )
    rss_url = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
    feed = feedparser.parse(rss_url)
    items = [
        {"title": e.get("title", ""), "link": e.get("link", "")}
        for e in feed.entries[:15]
        if e.get("title")
    ]
    if not items:
        return "", []
    text = "\n".join(f"{i+1}. {it['title']}" for i, it in enumerate(items))
    return text, items


def refresh_schedule() -> dict:
    news_text, news_items = get_schedule_news()
    if not news_text:
        print("[일정 갱신] 뉴스 없음 - 폴백 유지")
        _schedule_store["ts"] = time.time()
        return {"status": "no_news", "source": _schedule_store["source"]}

    try:
        response = _claude_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=[
                {
                    "type": "text",
                    "text": SCHEDULE_EXTRACT_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": f"[뉴스 목록]\n{news_text}"}],
        )
        text = response.content[0].text.strip()
        match = re.search(r"\[[\s\S]*\]", text)
        if match:
            text = match.group(0)
        text = _fix_json_strings(text)
        extracted: list[dict] = json.loads(text)
    except Exception as e:
        print(f"[일정 갱신 에러] {e}")
        _schedule_store["ts"] = time.time()
        return {"status": "error", "detail": str(e)}

    if not extracted:
        print("[일정 갱신] Claude 추출 결과 없음 - 폴백 유지")
        _schedule_store["ts"] = time.time()
        return {"status": "empty", "source": "fallback"}

    # sourceIdx로 원문 링크 매핑
    for s in extracted:
        idx = s.pop("sourceIdx", 0)
        if idx and 1 <= int(idx) <= len(news_items):
            s["sourceTitle"] = news_items[int(idx) - 1]["title"]
            s["sourceLink"]  = news_items[int(idx) - 1]["link"]

    merged = {f"{s['date']}|{s['event'][:8]}": s for s in SCHEDULE_FALLBACK}
    for s in extracted:
        key = f"{s['date']}|{s['event'][:8]}"
        merged[key] = s
    final = sorted(merged.values(), key=lambda x: x["date"])

    _schedule_store["items"] = final
    _schedule_store["sourceNews"] = news_items
    _schedule_store["ts"] = time.time()
    _schedule_store["source"] = "claude"
    _feeds_cache["ts"] = 0
    print(f"[일정 갱신] {len(final)}개 항목 업데이트 완료")
    return {"status": "ok", "count": len(final), "source": "claude"}


def _background_schedule_loop():
    time.sleep(2)
    while True:
        try:
            refresh_schedule()
            schedule_codes = [
                code
                for s in _schedule_store["items"]
                for code in s.get("relatedCodes", [])
            ]
            prefetch_stocks(schedule_codes)
            print(f"[워밍업] 주가 {len(set(schedule_codes))}개 종목 캐싱 완료")
        except Exception as e:
            print(f"[일정 루프 에러] {e}")
        time.sleep(SCHEDULE_TTL)


# ── 뉴스 수집 & 분석 ────────────────────────────────────────────────────

_KST = timezone(timedelta(hours=9))


def _pub_ts_utc(entry) -> float:
    """feedparser entry의 발행 시각을 UTC epoch(float)로 변환."""
    import calendar
    parsed = entry.get("published_parsed")
    if parsed:
        return float(calendar.timegm(parsed))
    return 0.0


def _today_kst() -> date:
    """현재 한국 시간 기준 오늘 날짜(date)"""
    return datetime.now(_KST).date()


def _pub_date_kst(entry) -> date | None:
    """feedparser entry의 발행 날짜를 KST 기준 date로 반환"""
    ts = _pub_ts_utc(entry)
    if ts == 0.0:
        return None
    return datetime.fromtimestamp(ts, tz=_KST).date()


_KR_QUERY_BUCKETS = [
    "(젠슨황 OR 엔비디아 OR Jensen Huang) AND (방한 OR 한국 OR 삼성 OR 하이닉스 OR LG OR 현대차 OR 네이버 OR 두산)",
    "(엔비디아 OR Nvidia) AND (수혜 OR 관련주 OR 밸류체인 OR HBM OR 협력 OR 공급 OR 로봇 OR AI반도체)",
]


def get_jensen_news(cutoff: float = 0.0) -> list[dict]:
    """cutoff(UTC epoch) 이후 발행된 국장 뉴스 멀티버킷 수집."""
    def entry_to_dict(entry) -> dict:
        pub_ts = _pub_ts_utc(entry)
        return {
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "pubDate": entry.get("published", ""),
            "pubTs": pub_ts,
        }

    seen: set[str] = set()
    result: list[dict] = []
    for raw_q in _KR_QUERY_BUCKETS:
        rss_url = f"https://news.google.com/rss/search?q={quote(raw_q)}&hl=ko&gl=KR&ceid=KR:ko"
        try:
            feed = feedparser.parse(rss_url)
        except Exception:
            continue
        for e in feed.entries[:30]:
            if _pub_ts_utc(e) < cutoff:
                continue
            key = e.get("title", "")[:50].lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(entry_to_dict(e))

    result.sort(key=lambda x: -x["pubTs"])
    print(f"[국장뉴스] cutoff 필터 후 {len(result)}개 (버킷 {len(_KR_QUERY_BUCKETS)}개)")
    return result


def is_kospi_relevant(title: str) -> bool:
    title_lower = title.lower()
    return any(kw.lower() in title_lower for kw in KOSPI_KEYWORDS)


def is_us_relevant(title: str) -> bool:
    title_lower = title.lower()
    return any(kw.lower() in title_lower for kw in US_NEWS_KEYWORDS)


_US_QUERY_BUCKETS = [
    '(Nvidia OR "Jensen Huang") AND (TSMC OR Microsoft OR Blackwell OR Rubin OR "AI chip" OR partnership OR deal)',
    '(TSMC OR ASML OR AMD OR Broadcom OR Marvell) AND (AI OR semiconductor OR "data center" OR supply)',
    '(Vertiv OR Palantir OR "Arista Networks" OR Cloudflare) AND (AI OR "data center" OR enterprise OR contract)',
    '(Microsoft OR Amazon OR Google OR Meta OR Oracle) AND (Nvidia OR "AI GPU" OR "data center" OR inference)',
]


def get_us_news(cutoff: float = 0.0) -> list[dict]:
    """cutoff(UTC epoch) 이후 발행된 미장 뉴스 멀티버킷 수집."""
    def entry_to_dict(entry) -> dict:
        pub_ts = _pub_ts_utc(entry)
        return {
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "pubDate": entry.get("published", ""),
            "pubTs": pub_ts,
        }

    seen_titles: set[str] = set()
    bucket_results: list[list[dict]] = []

    for raw_q in _US_QUERY_BUCKETS:
        rss_url = f"https://news.google.com/rss/search?q={quote(raw_q)}&hl=en&gl=US&ceid=US:en"
        try:
            feed = feedparser.parse(rss_url)
        except Exception:
            bucket_results.append([])
            continue
        bucket: list[dict] = []
        for e in feed.entries[:15]:
            if _pub_ts_utc(e) < cutoff:
                continue
            title = e.get("title", "")
            key = title[:50].lower()
            if key in seen_titles:
                continue
            seen_titles.add(key)
            bucket.append(entry_to_dict(e))
        bucket_results.append(bucket)

    result: list[dict] = []
    for bucket in bucket_results:
        result.extend(sorted(bucket, key=lambda x: -x["pubTs"])[:3])

    final_seen: set[str] = set()
    deduped: list[dict] = []
    for item in sorted(result, key=lambda x: -x["pubTs"]):
        key = item["title"][:50].lower()
        if key not in final_seen:
            final_seen.add(key)
            deduped.append(item)

    print(f"[미장뉴스] cutoff 필터 후 {len(deduped)}개 (버킷 {len(_US_QUERY_BUCKETS)}개)")
    return deduped


def get_us_stock_change(ticker: str) -> str:
    """미국 주식 등락률 (야후 파이낸스 기준)"""
    if not ticker:
        return "N/A"
    cached = _stock_cache.get(f"us_{ticker}")
    if cached and time.time() - cached["ts"] < STOCK_TTL:
        return cached["change"]
    change = "N/A"
    try:
        hist = yf.Ticker(ticker).history(period="2d")
        if len(hist) >= 2:
            prev = float(hist["Close"].iloc[-2])
            curr = float(hist["Close"].iloc[-1])
            pct = (curr - prev) / prev * 100
            change = f"{pct:+.2f}%"
    except Exception:
        pass
    _stock_cache[f"us_{ticker}"] = {"change": change, "ts": time.time()}
    return change


def _get_system_prompt(market: str) -> str:
    return NEWS_SYSTEM_PROMPT_US if market == "us" else NEWS_SYSTEM_PROMPT


def _get_approved_whitelist(market: str) -> dict:
    return APPROVED_STOCKS_US if market == "us" else APPROVED_STOCKS


def _analyze_with_claude(titles_text: str, schedule_text: str, market: str = "kr") -> dict | None:
    try:
        prompt = _get_system_prompt(market)
        if market == "us":
            user_content = f"[Today's US News]\n{titles_text}"
        else:
            user_content = f"{schedule_text}\n\n[오늘의 뉴스 목록]\n{titles_text}"
        response = _claude_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4096,
            system=[{"type": "text", "text": prompt, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user_content}],
        )
        text = response.content[0].text.strip()
        result = _parse_and_filter_analysis(text, [], market)
        if result:
            print(f"[Claude] {market.upper()} 분석 완료")
        return result
    except Exception as e:
        print(f"[Claude 분석 에러] {e}")
        return None


_GEMINI_MODEL_CHAIN = ["gemini-2.5-flash-lite", "gemini-2.5-flash"]
# 마지막 성공한 Gemini 분석 결과 캐시 (503 과부하 시 재사용)
_gemini_last_ok: dict = {"kr": None, "us": None}


def _analyze_with_gemini(titles_text: str, schedule_text: str, market: str = "kr") -> dict | None:
    if not _gemini_client:
        return None
    prompt = _get_system_prompt(market)
    user_content = (
        f"[Today's US News]\n{titles_text}" if market == "us"
        else f"{schedule_text}\n\n[오늘의 뉴스 목록]\n{titles_text}"
    )
    overload_count = 0
    for model in _GEMINI_MODEL_CHAIN:
        try:
            response = _gemini_client.models.generate_content(
                model=model,
                config=google_genai_types.GenerateContentConfig(
                    system_instruction=prompt,
                    temperature=0.3,
                ),
                contents=user_content,
            )
            text = response.text.strip()
            result = _parse_and_filter_analysis(text, [], market)
            if result:
                print(f"[Gemini:{model.split('-')[-1]}] {market.upper()} 분석 완료")
                _gemini_last_ok[market] = result  # 성공 결과 저장
            return result
        except Exception as e:
            err_str = str(e)
            if "503" in err_str or "UNAVAILABLE" in err_str or "429" in err_str:
                overload_count += 1
                print(f"[Gemini:{model}] {market.upper()} 과부하 → 다음 모델 시도")
                continue
            print(f"[Gemini 분석 에러] {e}")
            return None

    # 모든 모델 과부하 → 마지막 성공 결과 재사용
    cached = _gemini_last_ok.get(market)
    if cached:
        print(f"[Gemini] 과부하 — 직전 성공 결과 재사용 ({market.upper()})")
        return cached
    print(f"[Gemini] 모든 모델 실패, 캐시도 없음 ({market.upper()})")
    return None


_STATUS_SAFETY: dict[str, int] = {
    "호재강함": 1,
    "추세관망(호재보통)": 2,
    "과열주의": 3,
}

def _safer_status(a: str, b: str) -> str:
    """안전 우선순위 기준으로 더 보수적인 등급 반환 (과열주의 > 추세관망 > 호재강함)."""
    return a if _STATUS_SAFETY.get(a, 2) >= _STATUS_SAFETY.get(b, 2) else b


def _cross_validate(claude_result: dict, gemini_result: dict) -> dict:
    """두 AI 결과를 교차검증해 보수적인 최종 결과 반환"""
    c_idx = int(claude_result.get("jacketIndex", 3))
    g_idx = int(gemini_result.get("jacketIndex", 3))
    final_idx = min(c_idx, g_idx) if abs(c_idx - g_idx) >= 2 else c_idx
    final_idx = max(1, min(5, final_idx))

    # Gemini 종목별 status·guide 인덱싱
    # "search" 코드 종목은 name을 키로 사용 (신규포착 종목 매칭)
    gemini_stock_map: dict[str, dict] = {}
    for item in gemini_result.get("items", []):
        for s in item.get("stocks", []):
            code = s.get("code", "")
            key = s.get("name", "") if code == "search" else code
            if key:
                gemini_stock_map[key] = s

    # Claude 결과 기반으로 교차검증 적용
    for item in claude_result.get("items", []):
        for s in item.get("stocks", []):
            code = s.get("code", "")
            c_status = s.get("investment_status", "추세관망(호재보통)")
            c_guide = s.get("investment_guide", "")
            # "search" 종목은 name으로 Gemini 결과 매칭
            lookup_key = s.get("name", "") if code == "search" else code
            g_data = gemini_stock_map.get(lookup_key)

            if g_data:
                g_status = g_data.get("investment_status", "추세관망(호재보통)")
                g_guide = g_data.get("investment_guide", "")
                same = c_status == g_status

                if not same:
                    safer = _safer_status(c_status, g_status)
                    s["investment_status"] = safer
                    # 두 AI 시선을 하나의 가이드 필드에 통합
                    s["investment_guide"] = (
                        f"[🤖 Claude 시선] {c_guide} "
                        f"[✨ Gemini 시선] {g_guide}"
                    )
                    s["ai_consensus"] = {
                        "claude": c_status,
                        "gemini": g_status,
                        "method": "downgraded",
                        "final": safer,
                        "claude_guide": c_guide,
                        "gemini_guide": g_guide,
                    }
                else:
                    # 만장일치: 더 상세한 가이드 채택
                    s["investment_guide"] = c_guide if len(c_guide) >= len(g_guide) else g_guide
                    s["ai_consensus"] = {
                        "claude": c_status,
                        "gemini": g_status,
                        "method": "unanimous",
                    }
            else:
                s["ai_consensus"] = {
                    "claude": c_status,
                    "gemini": "분석없음",
                    "method": "claude_only",
                }

    # impactScore: Gemini 점수와 평균 (두 AI 모두 평가한 경우)
    gemini_impact_map: dict[str, int] = {}
    for item in gemini_result.get("items", []):
        title = item.get("title", "")[:20]  # 앞 20자로 매칭
        score = item.get("impactScore", 0)
        if title and score:
            gemini_impact_map[title] = int(score)

    for item in claude_result.get("items", []):
        c_score = int(item.get("impactScore", 3))
        g_score = gemini_impact_map.get(item.get("title", "")[:20])
        if g_score:
            # 두 AI 점수 평균 (소수점 올림 - 파급력 과소평가 방지)
            item["impactScore"] = math.ceil((c_score + g_score) / 2)
        else:
            item["impactScore"] = c_score

    # 최종 jacketIndex·marketWeather 덮어쓰기
    claude_result["jacketIndex"] = final_idx
    claude_result["marketWeather"] = JACKET_WEATHER_MAP[final_idx]

    # jacketIndex가 보수적으로 하향된 경우 weatherReason 보완
    if final_idx < c_idx:
        claude_result["weatherReason"] = (
            claude_result.get("weatherReason", "") + " (Gemini 교차검증 후 하향)"
        ).strip()

    return claude_result


def _update_daily_weather(market: str, result: dict) -> None:
    """분석 결과를 EMA로 누적해 하루 총합 날씨를 갱신한다 (3시간 고정)."""
    store = _daily_weather[market]
    today = date.today().isoformat()

    # 자정 넘으면 누적 초기화
    if store["date"] != today:
        store["date"] = today
        store["scores"] = []
        store["ts"] = 0.0
        print(f"[일별날씨] {market.upper()} 날짜 변경 → 누적 초기화")

    new_score = max(1, min(5, int(result.get("jacketIndex", 3))))
    store["scores"].append(new_score)

    # 지수 이동 평균 alpha=0.3 (신규 30% / 기존 70%)
    ema: float = store["scores"][0]
    for s in store["scores"][1:]:
        ema = 0.3 * s + 0.7 * ema
    avg_score = max(1, min(5, round(ema)))

    now = time.time()
    elapsed = now - store["ts"]
    if store["ts"] == 0.0 or elapsed >= DAILY_WEATHER_TTL:
        store["jacketIndex"] = avg_score
        store["marketWeather"] = JACKET_WEATHER_MAP[avg_score]
        store["weatherReason"] = result.get("weatherReason", "")
        store["dailyStrategy"] = result.get("dailyStrategy", "")
        store["ts"] = now
        _weather_label = JACKET_WEATHER_MAP[avg_score].encode("ascii", "replace").decode()
        print(
            f"[일별날씨] {market.upper()} 총합 확정 -> {avg_score}/5 "
            f"({_weather_label}) | 샘플 {len(store['scores'])}개"
        )
    else:
        remain = int((DAILY_WEATHER_TTL - elapsed) / 60)
        print(
            f"[일별날씨] {market.upper()} 캐시 유지 "
            f"(갱신까지 {remain}분 남음, 현재 {store['jacketIndex']}/5)"
        )


def analyze_news_batch(news_list: list[dict], market: str = "kr") -> dict:
    titles_text = "\n".join(f"{i+1}. {n['title']}" for i, n in enumerate(news_list))
    schedule_text = build_schedule_context() if market == "kr" else ""
    cache_key = _hash(titles_text + schedule_text + market)

    cached = _analysis_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < ANALYSIS_TTL:
        return cached["result"]

    fallback = {
        "marketWeather": "🌥️흐림",
        "weatherReason": "AI 분석을 일시적으로 사용할 수 없습니다.",
        "dailyStrategy": "분석 불가 - 잠시 후 다시 시도해주세요.",
        "jacketIndex": 1,
        "aiMethod": "fallback",
        "items": [
            {"title": n["title"], "aiSummary": "AI 분석 대기 중", "stocks": []}
            for n in news_list
        ],
    }

    result = fallback
    try:
        with ThreadPoolExecutor(max_workers=2) as ex:
            claude_fut = ex.submit(_analyze_with_claude, titles_text, schedule_text, market)
            gemini_fut = ex.submit(_analyze_with_gemini, titles_text, schedule_text, market)
        claude_result = claude_fut.result()
        gemini_result = gemini_fut.result()

        if claude_result and gemini_result:
            result = _cross_validate(claude_result, gemini_result)
            result["aiMethod"] = "dual"
            print("[분석] Claude × Gemini 교차검증 완료")
        elif claude_result:
            result = claude_result
            for item in result.get("items", []):
                for s in item.get("stocks", []):
                    s.setdefault("ai_consensus", {
                        "claude": s.get("investment_status", ""),
                        "gemini": "분석없음",
                        "method": "claude_only",
                    })
            result["aiMethod"] = "claude_only"
            print("[분석] Claude 단독 분석 완료")
        else:
            result = fallback
            print("[분석] 모든 AI 분석 실패 - 폴백 사용")
    except Exception as e:
        print(f"[분석 에러] {e}")
        result = fallback

    _analysis_cache[cache_key] = {"result": result, "ts": time.time()}
    if result.get("aiMethod") != "fallback":
        _update_daily_weather(market, result)
    return result


def get_stock_change(code: str) -> str:
    if not code:
        return "N/A"
    cached = _stock_cache.get(code)
    if cached and time.time() - cached["ts"] < STOCK_TTL:
        return cached["change"]

    change = "N/A"
    for suffix in [".KS", ".KQ"]:
        try:
            hist = yf.Ticker(f"{code}{suffix}").history(period="2d")
            if len(hist) >= 2:
                prev = float(hist["Close"].iloc[-2])
                curr = float(hist["Close"].iloc[-1])
                pct = (curr - prev) / prev * 100
                change = f"{pct:+.2f}%"
                break
        except Exception:
            continue

    _stock_cache[code] = {"change": change, "ts": time.time()}
    return change


def _consolidate_stock_signals(result_items: list[dict]) -> list[dict]:
    """동일 종목이 여러 기사에 중복 등장할 때, 파급력 최고 기사의 투자 상태로 통일."""
    # 종목별 최고 파급력 기사의 status 수집
    stock_best: dict[str, tuple[int, str]] = {}
    for item in result_items:
        impact = int(item.get("impactScore", 1))
        for s in item.get("stocks", []):
            code = s.get("code", "")
            if not code or code == "search":
                continue
            prev = stock_best.get(code)
            if prev is None or impact > prev[0]:
                stock_best[code] = (impact, s.get("investment_status", "추세관망(호재보통)"))

    # 약한 기사의 종목 상태를 지배 기사 기준으로 덮어쓰기
    for item in result_items:
        impact = int(item.get("impactScore", 1))
        for s in item.get("stocks", []):
            code = s.get("code", "")
            if not code or code == "search":
                continue
            best_impact, best_status = stock_best.get(code, (impact, s.get("investment_status", "")))
            orig_status = s.get("investment_status", "")
            if best_impact > impact and orig_status != best_status:
                s["investment_status"] = best_status
                guide = s.get("investment_guide", "")
                s["investment_guide"] = (
                    guide + " 단, 더 강한 재료가 포착된 기사 기준으로 통일된 신호입니다."
                ).strip()
    return result_items


def prefetch_stocks(codes: list[str]) -> None:
    unique = [c for c in set(codes) if c and c not in _stock_cache]
    if not unique:
        return
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(get_stock_change, c): c for c in unique}
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass


# ── API 엔드포인트 ──────────────────────────────────────────────────────

@app.get("/api/feeds")
def get_feeds(market: str = Query("kr", pattern="^(kr|us)$")):
    _cutoff, _fallback_cutoff, _window_label = _get_news_cutoff(market)
    cache = _feeds_cache[market]
    if cache["data"] and time.time() - cache["ts"] < FEEDS_TTL:
        if market == "kr":
            cached = dict(cache["data"])
            cached["scheduleSourceNews"] = _schedule_store.get("sourceNews", [])[:8]
            cached["newsWindowLabel"] = _window_label
            return cached
        cached = dict(cache["data"])
        cached["newsWindowLabel"] = _window_label
        return cached

    # "최신" 기준: 오늘 00:00 KST 또는 최근 12h 중 더 오래된 시각 → 더 많은 최신 기사 포함
    _now_kst = datetime.now(KST)
    _today_start_ts = _now_kst.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    _12h_ts = time.time() - 43200
    _recent_cutoff = min(_today_start_ts, _12h_ts)  # 둘 중 더 오래된 시각

    def _split_today_fallback(news_list: list[dict]) -> tuple[list[dict], list[dict]]:
        today = [n for n in news_list if n.get("pubTs", 0) >= _recent_cutoff]
        old   = [n for n in news_list if n.get("pubTs", 0) < _recent_cutoff]
        return today, old

    def _diversity_filter(news_list: list[dict], major_kw: list[str], is_kr: bool) -> list[dict]:
        counts: dict[str, int] = {}
        result: list[dict] = []
        for n in news_list:
            dom = next(
                (kw for kw in major_kw if (kw in n["title"] if is_kr else kw.lower() in n["title"].lower())),
                None
            )
            if dom:
                counts[dom] = counts.get(dom, 0) + 1
                if counts[dom] > 3:
                    continue
            result.append(n)
        return result

    # 뉴스 수집 — 항상 72h로 넓게 수집, today/fallback은 오늘 00:00 기준으로 분리
    _72h_cutoff = time.time() - 259200
    if market == "us":
        _US_MAJOR = ["Nvidia", "NVDA", "Microsoft", "Apple", "Amazon", "Google",
                     "Meta", "TSMC", "AMD", "Broadcom", "Palantir", "Vertiv"]
        print(f"[미장뉴스] 수집 윈도우: {_window_label} (수집범위 72h)")
        raw_all = get_us_news(cutoff=_72h_cutoff)
        rel_all = [n for n in raw_all if is_us_relevant(n["title"])]
        if not rel_all:
            rel_all = raw_all
        rel_all = sorted(_diversity_filter(rel_all, _US_MAJOR, False), key=lambda x: -x.get("pubTs", 0))
        today_news, old_news = _split_today_fallback(rel_all)
        needed = max(0, 8 - len(today_news))
        fallback_news = old_news[:needed]
        print(f"[미장뉴스] 최신 {len(today_news)}개 + 전일보충 {len(fallback_news)}개")
    else:
        _KR_MAJOR = ["LG", "삼성", "SK하이닉스", "SK", "현대차", "현대", "네이버", "두산", "한화", "카카오"]
        print(f"[국장뉴스] 수집 윈도우: {_window_label} (수집범위 72h)")
        raw_all = get_jensen_news(cutoff=_72h_cutoff)
        rel_all = [n for n in raw_all if is_kospi_relevant(n["title"])]
        if not rel_all:
            rel_all = raw_all
        rel_all = sorted(_diversity_filter(rel_all, _KR_MAJOR, True), key=lambda x: -x.get("pubTs", 0))
        today_news, old_news = _split_today_fallback(rel_all)
        needed = max(0, 8 - len(today_news))
        fallback_news = old_news[:needed]
        print(f"[국장뉴스] 최신 {len(today_news)}개 + 전일보충 {len(fallback_news)}개")

    # AI 분석용 합산 (오늘 먼저, 이후 fallback) — 최대 8개
    filtered = (today_news + fallback_news)[:8]
    _today_title_set = {n["title"][:50] for n in today_news}

    analysis = analyze_news_batch(filtered, market=market)
    analyzed_items = analysis.get("items", [])

    # 주가 병렬 fetch
    all_codes = [s.get("code", "") for ai in analyzed_items for s in ai.get("stocks", [])]
    if market == "us":
        with ThreadPoolExecutor(max_workers=12) as ex:
            futs = {ex.submit(get_us_stock_change, c): c for c in set(all_codes) if c}
            for f in as_completed(futs):
                try: f.result()
                except Exception: pass
    else:
        prefetch_stocks(all_codes)

    result_items = []
    for i, news in enumerate(filtered):
        ai = analyzed_items[i] if i < len(analyzed_items) else {}
        if market == "us":
            stocks = [
                {
                    "name": s.get("name", ""),
                    "code": s.get("code", ""),
                    "is_new_discovery": s.get("is_new_discovery", False),
                    "investment_status": s.get("investment_status", "추세관망(호재보통)"),
                    "investment_guide": s.get("investment_guide", ""),
                    "change": get_us_stock_change(s.get("code", "")) if s.get("code") != "search" else "N/A",
                    "ai_consensus": s.get("ai_consensus", {
                        "claude": s.get("investment_status", ""),
                        "gemini": "분석없음",
                        "method": "claude_only",
                    }),
                    "market": "us",
                }
                for s in ai.get("stocks", [])
            ]
        else:
            stocks = [
                {
                    "name": s.get("name", ""),
                    "code": s.get("code", ""),
                    "is_new_discovery": s.get("is_new_discovery", False),
                    "investment_status": s.get("investment_status", "추세관망(호재보통)"),
                    "investment_guide": s.get("investment_guide", ""),
                    "change": get_stock_change(s.get("code", "")) if s.get("code") != "search" else "N/A",
                    "ai_consensus": s.get("ai_consensus", {
                        "claude": s.get("investment_status", ""),
                        "gemini": "분석없음",
                        "method": "claude_only",
                    }),
                    "market": "kr",
                }
                for s in ai.get("stocks", [])
            ]
        result_items.append({
            "id": str(i + 1),
            "time": news["pubDate"],
            "pubTs": news.get("pubTs", 0.0),
            "title": news["title"],
            "link": news["link"],
            "aiSummary": ai.get("aiSummary", ""),
            "impactScore": int(ai.get("impactScore", 3)),
            "stocks": stocks,
        })

    # 동일 종목 복수 기사 → 지배 뉴스 기준으로 투자 상태 통일
    result_items = _consolidate_stock_signals(result_items)

    # today / fallback 분리
    today_items    = [x for x in result_items if x["title"][:50] in _today_title_set]
    fallback_items = [x for x in result_items if x["title"][:50] not in _today_title_set]

    # today: 파급력 4점↑ 상단 고정 + 나머지 최신순
    pinned = sorted([x for x in today_items if x["impactScore"] >= 4],
                    key=lambda x: (-x["impactScore"], -x["pubTs"]))[:2]
    pinned_ids = {id(x) for x in pinned}
    rest_today = sorted([x for x in today_items if id(x) not in pinned_ids], key=lambda x: -x["pubTs"])
    for item in pinned:
        item["isPinned"] = True
    today_sorted = pinned + rest_today

    # fallback: 최신순
    fallback_sorted = sorted(fallback_items, key=lambda x: -x["pubTs"])

    sorted_items = (today_sorted + fallback_sorted)[:8]
    for i, item in enumerate(sorted_items):
        item["id"] = str(i + 1)

    print(f"[{market.upper()} 정렬] today {len(today_sorted)}개 + fallback {len(fallback_sorted)}개")

    # 상단 날씨 위젯 — 하루 총합 누적값(3시간 고정) 우선, 없으면 실시간 분석값 사용
    dw = _daily_weather[market]
    top_weather     = dw["marketWeather"] if dw["ts"] else analysis.get("marketWeather", "🌥️흐림")
    top_reason      = dw["weatherReason"] if dw["ts"] else analysis.get("weatherReason", "")
    top_strategy    = dw["dailyStrategy"] if dw["ts"] else analysis.get("dailyStrategy", "")
    top_jacket      = dw["jacketIndex"]   if dw["ts"] else analysis.get("jacketIndex", 3)
    sample_count    = len(dw["scores"])
    weather_updated = dw["ts"]

    result = {
        "market": market,
        "marketWeather": top_weather,
        "weatherReason": top_reason,
        "dailyStrategy": top_strategy,
        "jacketIndex": top_jacket,
        "weatherSampleCount": sample_count,
        "weatherUpdatedAt": weather_updated,
        "aiMethod": analysis.get("aiMethod", "fallback"),
        "newsWindowLabel": _window_label,
        "today_feeds": today_sorted,
        "fallback_feeds": fallback_sorted,
        "items": sorted_items,
    }
    if market == "kr":
        result["schedule"] = enrich_schedule(_schedule_store["items"])
        result["scheduleSource"] = _schedule_store["source"]
        result["scheduleUpdatedAt"] = _schedule_store["ts"]
        result["scheduleSourceNews"] = _schedule_store.get("sourceNews", [])[:8]

    cache["data"] = result
    cache["ts"] = time.time()
    return result


@app.post("/api/schedule/refresh")
def schedule_refresh():
    result = refresh_schedule()
    return result


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "scheduleSource": _schedule_store["source"],
        "scheduleItems": len(_schedule_store["items"]),
        "scheduleAge": int(time.time() - _schedule_store["ts"]),
        "geminiEnabled": _gemini_client is not None,
    }


# ── 프론트엔드 서빙 ─────────────────────────────────────────────────────
_HTML_PATH = pathlib.Path(__file__).parent.parent / "jensen-tracker.html"

@app.get("/")
def serve_frontend():
    return FileResponse(_HTML_PATH, media_type="text/html")


# ── 서버 시작 ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
