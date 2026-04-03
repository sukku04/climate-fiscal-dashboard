#!/usr/bin/env python3
"""
열린재정 Open API 기후재정 데이터 수집기
GitHub Actions에서 실행: FISCAL_API_KEY 환경변수 필요

수집 대상:
  - dFUncBudgetInfo: 기능별 세출예산 현황
  - dFUncSettleInfo: 기능별 세출결산 현황

출력:
  - data/budget_{year}.json  : 연도별 예산 데이터
  - data/settle_{year}.json  : 연도별 결산 데이터
  - data/meta.json           : 수집 메타데이터
"""
import os, json, time, ssl, sys, urllib.request, urllib.parse
from datetime import datetime
from pathlib import Path

# ── 설정 ──────────────────────────────────────────────────
API_KEY   = os.environ.get('FISCAL_API_KEY', '')
BASE      = 'https://openapi.openfiscaldata.go.kr'
DATA_DIR  = Path('data')
THIS_YEAR = datetime.now().year

# 수집 연도 (환경변수로 오버라이드 가능)
RAW_YEARS = os.environ.get('COLLECT_YEARS', '')
if RAW_YEARS:
    YEARS = [int(y.strip()) for y in RAW_YEARS.split(',') if y.strip()]
else:
    YEARS = list(range(2022, THIS_YEAR + 1))  # 2022 ~ 현재연도

# 기후 관련 프로그램명 필터 (이상민 연구위원 분류 기준)
CLIMATE_PROGRAMS = {
    '공정한전환', '기후변화 과학', '기후변화대응', '대기환경 보전',
    '에너지기술개발', '온실가스감축', '재생에너지및에너지신산업활성화',
    '저탄소생태계조성', '탄소중립기반구축', '환경보건관리',
    '화학물질 안전관리', '수질 및 수생태계 관리', '자연생태 보전',
    '친환경경제사회 활성화', '저탄소농업기반구축', '해양환경보전',
    '탄소중립도시숲조성', '탄소중립그린도시',
}

# 기후 관련 분야 필터 (전체 수집 시 분야 필터로 범위 제한)
CLIMATE_FIELDS = {
    '환경', '산업·중소기업및에너지', '교통및물류',
    '국토및지역개발', '농림수산', '과학기술',
}

# SSL 우회 (일부 정부 API 인증서 체인 문제)
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode   = ssl.CERT_NONE

def log(msg): print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── API 호출 ───────────────────────────────────────────────
def api_call(endpoint, params, timeout=30):
    url = f"{BASE}/{endpoint}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (climate-fiscal-dashboard/1.0)',
        'Accept':     'application/json',
        'Referer':    'https://www.openfiscaldata.go.kr/',
    })
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
        return json.loads(r.read().decode('utf-8'))

def parse_response(data, ep):
    """응답에서 (total, rows) 추출 — 다양한 구조 대응"""
    root = data.get(ep, data)
    if isinstance(root, dict):
        lst   = root.get('list') or root.get('List') or []
        total = int(root.get('totalCount') or root.get('TotalCount') or len(lst))
        if lst: return total, lst
    if isinstance(root, list):
        total, rows = 0, []
        for item in root:
            if isinstance(item, dict):
                try: total = int(item.get('totalCount', total))
                except: pass
                if 'row' in item: rows = item['row']
        if rows: return total, rows
    for key in ('list', 'List', 'row'):
        if key in data and isinstance(data[key], list):
            return len(data[key]), data[key]
    return 0, []

# ── 연도 전체 수집 ─────────────────────────────────────────
def fetch_all_pages(endpoint, year, page_size=300, max_pages=50):
    """전체 페이지네이션 수집"""
    all_rows = []

    # 1페이지로 총 건수 확인
    first = api_call(endpoint, {
        'key': API_KEY, 'Type': 'json',
        'FSCL_YR': str(year), 'pIndex': '1', 'pSize': str(page_size),
    })

    # API 오류 확인
    result = first.get('RESULT', {})
    code   = result.get('CODE', '')
    if code and code not in ('00', 'INFO-000', 'INFO-001'):
        raise RuntimeError(f"API 오류 [{code}]: {result.get('MESSAGE', '')}")

    total, rows = parse_response(first, endpoint)
    if not rows:
        log(f"  {year}년 데이터 없음 (totalCount={total})")
        return []

    all_rows.extend(rows)
    pages = min(max_pages, max(1, (total + page_size - 1) // page_size))
    log(f"  {year}년 총 {total:,}건 ({pages}페이지)")

    for page in range(2, pages + 1):
        data = api_call(endpoint, {
            'key': API_KEY, 'Type': 'json',
            'FSCL_YR': str(year), 'pIndex': str(page), 'pSize': str(page_size),
        })
        _, page_rows = parse_response(data, endpoint)
        if not page_rows:
            log(f"  {page}페이지 빈 응답 → 완료")
            break
        all_rows.extend(page_rows)
        log(f"  {page}/{pages}p ({len(all_rows):,}건)")
        time.sleep(0.3)

    return all_rows

# ── 행 정규화 ──────────────────────────────────────────────
FIELD_MAP = {
    'FSCL_YR':              'year',
    'OFFC_NM':              'ministry',
    'ACNT_NM':              'account',
    'FILD_NM':              'field',
    'SECT_NM':              'sector',
    'PGM_NM':               'program',
    'UNIT_ERND_BUSN_NM':    'unit',
    'DTL_BUSN_NM':          'detail',
    'OFCL_BUDGET_AMT':      'budget_init',   # 본예산
    'CRNT_BUDGET_AMT':      'budget_cur',    # 현액
    'EXCUT_AMT':            'exec_amt',      # 집행액
    'BF_CRNT_BUDGET_AMT':   'budget_prev',   # 전년현액
    'SETL_AMT':             'settle_amt',    # 결산액
}
AMT_FIELDS = {'budget_init', 'budget_cur', 'exec_amt', 'budget_prev', 'settle_amt'}

def normalize(row):
    out = {}
    for src, dst in FIELD_MAP.items():
        v = row.get(src, '')
        if dst in AMT_FIELDS:
            try: out[dst] = int(str(v).replace(',', '').strip() or '0')
            except: out[dst] = 0
        else:
            out[dst] = str(v).strip()
    # 추경증감 계산
    out['budget_supp'] = out.get('budget_cur', 0) - out.get('budget_init', 0)
    # 기후 관련 여부 태깅
    out['is_climate'] = out.get('program', '') in CLIMATE_PROGRAMS
    out['is_climate_field'] = out.get('field', '') in CLIMATE_FIELDS
    return out

# ── 집계 ──────────────────────────────────────────────────
def aggregate(rows, year):
    """프로그램별, 분야별, 부처별 집계"""
    by_prog, by_field, by_min, by_sector = {}, {}, {}, {}

    for r in rows:
        prog   = r.get('program', '')
        field  = r.get('field', '')
        sector = r.get('sector', '')
        ministry = r.get('ministry', '')
        amt    = r.get('budget_cur', 0) or r.get('budget_init', 0)

        if prog:
            if prog not in by_prog:
                by_prog[prog] = {'budget_cur':0,'budget_init':0,'exec_amt':0,'is_climate':r['is_climate'],'field':field,'sector':sector}
            by_prog[prog]['budget_cur']  += r.get('budget_cur', 0)
            by_prog[prog]['budget_init'] += r.get('budget_init', 0)
            by_prog[prog]['exec_amt']    += r.get('exec_amt', 0)

        if field:
            by_field[field] = by_field.get(field, 0) + amt

        if sector:
            by_sector[sector] = by_sector.get(sector, 0) + amt

        if ministry:
            if ministry not in by_min:
                by_min[ministry] = {'budget_cur':0,'budget_init':0}
            by_min[ministry]['budget_cur']  += r.get('budget_cur', 0)
            by_min[ministry]['budget_init'] += r.get('budget_init', 0)

    return {
        'year':      year,
        'by_program': by_prog,
        'by_field':   by_field,
        'by_sector':  by_sector,
        'by_ministry':by_min,
        'gov_total':  sum(r.get('budget_cur', 0) or r.get('budget_init', 0) for r in rows),
        'climate_total': sum(
            r.get('budget_cur', 0) or r.get('budget_init', 0)
            for r in rows if r['is_climate']
        ),
    }

# ── 저장 ──────────────────────────────────────────────────
def save(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, separators=(',', ':'))
    kb = path.stat().st_size // 1024
    log(f"  💾 {path} ({kb}KB)")

# ── 메인 ──────────────────────────────────────────────────
def main():
    if not API_KEY:
        log("❌ FISCAL_API_KEY 환경변수가 없습니다.")
        log("   GitHub Secrets에 FISCAL_API_KEY를 등록하세요.")
        sys.exit(1)

    log("=" * 56)
    log("기후재정 데이터 수집 시작")
    log(f"수집 연도: {YEARS}")
    log(f"API Key:   {'*' * (len(API_KEY)-4)}{API_KEY[-4:]}")
    log("=" * 56)

    collected = []
    errors    = []

    for year in YEARS:
        log(f"\n📅 {year}년 예산 수집")
        try:
            rows_raw  = fetch_all_pages('dFUncBudgetInfo', year)
            rows_norm = [normalize(r) for r in rows_raw if r]
            agg       = aggregate(rows_norm, year)

            # 전체 행 저장 (기후 필터링 포함)
            save(DATA_DIR / f'budget_{year}.json', {
                'year':    year,
                'updated': datetime.now().isoformat(),
                'total':   len(rows_norm),
                'summary': agg,
                # rows는 기후 관련 필드만 (파일 크기 절약)
                'climate_rows': [r for r in rows_norm if r['is_climate'] or r['is_climate_field']],
            })

            collected.append(year)
            log(f"  ✅ {year}년 완료: {len(rows_norm):,}건 (기후관련: {sum(1 for r in rows_norm if r['is_climate'])}건)")

        except Exception as e:
            log(f"  ❌ {year}년 오류: {e}")
            errors.append({'year': year, 'error': str(e)})

        time.sleep(1)

        # 결산 (가능하면)
        log(f"\n📅 {year}년 결산 수집")
        try:
            settle_raw  = fetch_all_pages('dFUncSettleInfo', year)
            if settle_raw:
                settle_norm = [normalize(r) for r in settle_raw if r]
                save(DATA_DIR / f'settle_{year}.json', {
                    'year':    year,
                    'updated': datetime.now().isoformat(),
                    'total':   len(settle_norm),
                    'climate_rows': [r for r in settle_norm if r['is_climate'] or r['is_climate_field']],
                })
                log(f"  ✅ {year}년 결산 완료: {len(settle_norm):,}건")
        except Exception as e:
            log(f"  ⚠ {year}년 결산 오류 (예산은 저장됨): {e}")

        time.sleep(0.5)

    # 메타 저장
    save(DATA_DIR / 'meta.json', {
        'last_updated': datetime.now().isoformat(),
        'collected_years': collected,
        'errors': errors,
        'source': 'openfiscaldata.go.kr',
        'climate_programs': sorted(CLIMATE_PROGRAMS),
    })

    log("\n" + "=" * 56)
    log(f"✅ 완료: 성공 {len(collected)}개 연도, 오류 {len(errors)}개")
    if errors:
        for e in errors:
            log(f"  ❌ {e['year']}: {e['error']}")
    log("=" * 56)

    if len(collected) == 0:
        sys.exit(1)

if __name__ == '__main__':
    main()
