#!/usr/bin/env python3
"""
열린재정 Open API 기후재정 데이터 수집기 v3
확인된 엔드포인트:
  - OPFI172           : 분야별 프로그램 예산 (16대 분야별 예산규모·사업수)
  - ExpenditureBudgetAdd7 : 세출 세부사업 예산편성현황(추경포함)
  - ExpenditureBudgetAdd8 : 세출 세목 예산편성현황(추경포함)
"""
import os, json, time, ssl, sys, urllib.request, urllib.parse
from datetime import datetime
from pathlib import Path

API_KEY   = os.environ.get('FISCAL_API_KEY', '')
BASE      = 'https://openapi.openfiscaldata.go.kr'
DATA_DIR  = Path('data')
THIS_YEAR = datetime.now().year
RAW_YEARS = os.environ.get('COLLECT_YEARS', '')
YEARS     = [int(y.strip()) for y in RAW_YEARS.split(',') if y.strip()] if RAW_YEARS \
            else list(range(2022, THIS_YEAR + 1))

# 기후 관련 분야명 (OPFI172 분야별 프로그램에서 필터)
CLIMATE_FIELDS = {'환경','산업·중소기업및에너지','교통및물류','국토및지역개발','농림수산','과학기술'}

# 기후 관련 세부사업명 키워드 (ExpenditureBudgetAdd7 필터)
CLIMATE_KEYWORDS = [
    '기후','온실가스','탄소','재생에너지','신재생','에너지전환','수소','풍력','태양광',
    '전기차','무공해','저탄소','친환경','환경','대기','미세먼지','수질','생태',
    '녹색','탄소중립','공정한전환','에너지기술','에너지효율',
]

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode   = ssl.CERT_NONE

def log(msg): print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def api_call(endpoint, params, timeout=30):
    """API 호출 → dict 반환 보장. Key 파라미터는 대문자 K"""
    # 파라미터에 Key(대문자) 보장
    safe_params = {}
    for k, v in params.items():
        safe_params[k] = v
    url = BASE + '/' + endpoint + '?' + urllib.parse.urlencode(safe_params)
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (climate-fiscal/3.0)',
        'Accept':     'application/json, */*',
        'Referer':    'https://www.openfiscaldata.go.kr/',
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
            raw = r.read().decode('utf-8')
    except Exception as e:
        raise RuntimeError(f"HTTP 오류: {e}")

    if not raw or not raw.strip():
        raise RuntimeError("빈 응답")

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"JSON 파싱 실패: {e} | 원문: {raw[:300]}")

    if not isinstance(parsed, dict):
        raise RuntimeError(f"응답이 dict 아님: {type(parsed).__name__} | {str(parsed)[:200]}")

    return parsed

def parse_any(data):
    """
    다양한 열린재정 API 응답 구조에서 rows 추출.
    응답 구조를 모를 때 범용으로 사용.
    반환: (total, rows, field_names)
    """
    if not isinstance(data, dict):
        return 0, [], []

    # RESULT 오류 확인
    result = data.get('RESULT', {})
    if isinstance(result, dict) and result:
        code = str(result.get('CODE', ''))
        msg  = str(result.get('MESSAGE', ''))
        if code and code not in ('00', 'INFO-000', 'INFO-001', ''):
            raise RuntimeError(f"API 오류 [{code}]: {msg}")

    # 모든 값을 탐색해서 rows 배열 찾기
    for k, v in data.items():
        if k in ('RESULT', 'result'):
            continue

        # 패턴 A: { EP: { list: [...], totalCount: N } }
        if isinstance(v, dict):
            lst = v.get('list') or v.get('List') or v.get('row') or []
            if isinstance(lst, list) and lst:
                total = int(v.get('totalCount') or v.get('TotalCount') or len(lst))
                fields = list(lst[0].keys()) if isinstance(lst[0], dict) else []
                return total, lst, fields

        # 패턴 B: { EP: [ {totalCount:N}, {row:[...]} ] }
        if isinstance(v, list):
            # 직접 rows 배열인 경우
            if v and isinstance(v[0], dict) and len(v[0]) > 2:
                return len(v), v, list(v[0].keys())
            # [ header, {row:[...]} ] 구조
            total = 0
            rows  = []
            for item in v:
                if not isinstance(item, dict): continue
                try: total = int(item.get('totalCount', total))
                except: pass
                r = item.get('row') or item.get('list') or []
                if isinstance(r, list) and r:
                    rows = r
            if rows:
                fields = list(rows[0].keys()) if isinstance(rows[0], dict) else []
                return total, rows, fields

    # 최상위에 바로 list/row
    for key in ('list', 'List', 'row', 'data', 'items'):
        val = data.get(key)
        if isinstance(val, list) and val and isinstance(val[0], dict):
            return len(val), val, list(val[0].keys())

    return 0, [], []

def fetch_pages(endpoint, base_params, max_pages=50):
    """전체 페이지 수집"""
    first = api_call(endpoint, {**base_params, 'pIndex': '1'})

    log(f"    [디버그] {endpoint} 응답 키: {list(first.keys())}")
    total, rows, fields = parse_any(first)

    if not rows:
        log(f"    데이터 없음 (total={total})")
        log(f"    [디버그] 전체응답: {json.dumps(first, ensure_ascii=False)[:600]}")
        return [], []

    log(f"    총 {total:,}건, 필드: {fields[:6]}")
    all_rows = list(rows)
    if total == 0: total = len(rows)
    page_size = int(base_params.get('pSize', 300))
    pages = min(max_pages, max(1, (total + page_size - 1) // page_size))

    for page in range(2, pages + 1):
        d = api_call(endpoint, {**base_params, 'pIndex': str(page)})
        _, pr, _ = parse_any(d)
        if not pr: break
        all_rows.extend(pr)
        if page % 5 == 0:
            log(f"    {page}/{pages}p ({len(all_rows):,}건)")
        time.sleep(0.3)

    return all_rows, fields

def save(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, separators=(',', ':'))
    log(f"  💾 {path} ({path.stat().st_size // 1024}KB)")

def is_climate(row):
    """기후 관련 행 여부 판단"""
    text = ' '.join(str(v) for v in row.values() if isinstance(v, str))
    return any(kw in text for kw in CLIMATE_KEYWORDS)

def main():
    if not API_KEY:
        log("❌ FISCAL_API_KEY 없음"); sys.exit(1)

    log("=" * 60)
    log(f"기후재정 수집 v3 | 연도: {YEARS} | Key끝자리: {API_KEY[-4:]}")
    log("=" * 60)

    # ── 1. OPFI172: 분야별 프로그램 예산 (핵심) ────────────────
    log("\n🌱 [1/3] 분야별 프로그램 예산 (OPFI172)")
    opfi172_data = {}
    base_172 = {'Key': API_KEY, 'Type': 'json', 'pSize': '300'}

    # 연도 파라미터명 확인 필요 - 일단 ofYy, FSCL_YR 둘 다 시도
    for year in YEARS:
        log(f"  {year}년...")
        rows, fields = [], []
        # 파라미터 여러 조합 시도
        for yr_param in [{'FSCL_YR': str(year)}, {'ofYy': str(year)}, {'year': str(year)}, {}]:
            try:
                rows, fields = fetch_pages('OPFI172', {**base_172, **yr_param})
                if rows:
                    log(f"  ✅ {year}년 성공 (파라미터: {yr_param}), {len(rows)}건")
                    break
            except Exception as e:
                log(f"  ⚠ {yr_param} 실패: {str(e)[:80]}")
        if rows:
            opfi172_data[year] = rows
        time.sleep(0.5)

    if opfi172_data:
        save(DATA_DIR / 'field_program.json', {
            'updated': datetime.now().isoformat(),
            'years': list(opfi172_data.keys()),
            'fields_sample': fields[:15] if fields else [],
            'data': opfi172_data,
        })
        log(f"  ✅ OPFI172 저장 완료: {sum(len(v) for v in opfi172_data.values())}건")
    else:
        log("  ❌ OPFI172 데이터 없음")

    # ── 2. ExpenditureBudgetAdd7: 세출 세부사업(추경포함) ───────
    log("\n💰 [2/3] 세출 세부사업 예산편성현황 (ExpenditureBudgetAdd7)")
    budget_data = {}
    base_exp = {'Key': API_KEY, 'Type': 'json', 'pSize': '300'}

    for year in YEARS:
        log(f"  {year}년...")
        rows = []
        for yr_param in [{'FSCL_YR': str(year)}, {'ofYy': str(year)}, {'회계연도': str(year)}]:
            try:
                rows, fields = fetch_pages('ExpenditureBudgetAdd7', {**base_exp, **yr_param})
                if rows:
                    log(f"  ✅ {year}년 {len(rows)}건, 파라미터: {yr_param}")
                    break
            except Exception as e:
                log(f"  ⚠ {yr_param}: {str(e)[:80]}")

        if rows:
            climate_rows = [r for r in rows if is_climate(r)]
            budget_data[str(year)] = {
                'total': len(rows),
                'climate_count': len(climate_rows),
                'climate_rows': climate_rows,
                'fields': fields,
            }
            log(f"  기후관련: {len(climate_rows)}건")
        time.sleep(0.5)

    if budget_data:
        save(DATA_DIR / 'expenditure_budget.json', {
            'updated': datetime.now().isoformat(),
            'data': budget_data,
        })

    # ── 3. ExpenditureBudgetAdd8: 세출 세목(추경포함) ──────────
    log("\n📊 [3/3] 세출 세목 예산편성현황 (ExpenditureBudgetAdd8)")
    semo_data = {}

    for year in YEARS:
        log(f"  {year}년...")
        rows = []
        for yr_param in [{'FSCL_YR': str(year)}, {'ofYy': str(year)}]:
            try:
                rows, fields = fetch_pages('ExpenditureBudgetAdd8', {**base_exp, **yr_param})
                if rows:
                    log(f"  ✅ {year}년 {len(rows)}건")
                    break
            except Exception as e:
                log(f"  ⚠ {yr_param}: {str(e)[:80]}")

        if rows:
            climate_rows = [r for r in rows if is_climate(r)]
            semo_data[str(year)] = {
                'total': len(rows),
                'climate_count': len(climate_rows),
                'climate_rows': climate_rows,
            }
        time.sleep(0.5)

    if semo_data:
        save(DATA_DIR / 'expenditure_semo.json', {
            'updated': datetime.now().isoformat(),
            'data': semo_data,
        })

    # ── 메타 저장 ────────────────────────────────────────────
    collected_years = list(opfi172_data.keys()) or list(budget_data.keys())
    save(DATA_DIR / 'meta.json', {
        'last_updated':    datetime.now().isoformat(),
        'collected_years': [int(y) for y in sorted(set(map(str, collected_years)))],
        'endpoints_used': ['OPFI172', 'ExpenditureBudgetAdd7', 'ExpenditureBudgetAdd8'],
        'climate_keywords': CLIMATE_KEYWORDS,
        'source': 'openapi.openfiscaldata.go.kr',
    })

    total_ok = len(opfi172_data) + len(budget_data)
    log(f"\n완료: OPFI172={len(opfi172_data)}개년, 세부사업={len(budget_data)}개년")
    if total_ok == 0:
        sys.exit(1)

if __name__ == '__main__':
    main()
