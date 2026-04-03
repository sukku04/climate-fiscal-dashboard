#!/usr/bin/env python3
"""열린재정 Open API 기후재정 데이터 수집기 v2"""
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

CLIMATE_PROGRAMS = {
    '공정한전환','기후변화 과학','기후변화대응','대기환경 보전',
    '에너지기술개발','온실가스감축','재생에너지및에너지신산업활성화',
    '저탄소생태계조성','탄소중립기반구축','환경보건관리',
    '화학물질 안전관리','수질 및 수생태계 관리','자연생태 보전',
    '친환경경제사회 활성화','해양환경보전','탄소중립그린도시',
}
CLIMATE_FIELDS = {'환경','산업·중소기업및에너지','교통및물류','국토및지역개발','농림수산','과학기술'}

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode   = ssl.CERT_NONE

def log(msg): print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def api_call(endpoint, params, timeout=30):
    """API 호출 → dict 반환 보장"""
    url = BASE + '/' + endpoint + '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (climate-fiscal-dashboard/2.0)',
        'Accept':     'application/json, */*',
        'Referer':    'https://www.openfiscaldata.go.kr/',
    })
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
        raw = r.read().decode('utf-8')

    if not raw or not raw.strip():
        raise RuntimeError("빈 응답")

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"JSON 파싱 실패: {e} | 원문: {raw[:300]}")

    if not isinstance(parsed, dict):
        raise RuntimeError(f"응답이 dict가 아님: {type(parsed).__name__} | {str(parsed)[:200]}")

    return parsed

def parse_response(data, ep):
    """(total, rows) 추출 — 열린재정 다양한 응답 구조 대응"""
    if not isinstance(data, dict):
        return 0, []

    # API 오류 확인
    result = data.get('RESULT', {})
    if isinstance(result, dict) and result:
        code = result.get('CODE', '')
        msg  = result.get('MESSAGE', '')
        if code and code not in ('00', 'INFO-000', 'INFO-001', ''):
            raise RuntimeError(f"API 오류 [{code}]: {msg}")

    # 패턴 A: { EP: { "list": [...], "totalCount": N } }
    root = data.get(ep)
    if isinstance(root, dict):
        lst   = root.get('list') or root.get('List') or []
        total = int(root.get('totalCount') or root.get('TotalCount') or len(lst))
        if lst: return total, lst

    # 패턴 B: { EP: [ {totalCount:N}, {row:[...]} ] }
    if isinstance(root, list):
        total, rows = 0, []
        for item in root:
            if not isinstance(item, dict): continue
            try: total = int(item.get('totalCount', total))
            except: pass
            if 'row' in item and isinstance(item['row'], list):
                rows = item['row']
        if rows: return total, rows

    # 패턴 C: 최상위 list/row
    for key in ('list', 'List', 'row'):
        val = data.get(key)
        if isinstance(val, list) and val: return len(val), val

    # 패턴 D: 임의 키 배열 탐색
    for k, v in data.items():
        if k == 'RESULT': continue
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return len(v), v

    return 0, []

FIELD_MAP = {
    'FSCL_YR':'year','OFFC_NM':'ministry','ACNT_NM':'account',
    'FILD_NM':'field','SECT_NM':'sector','PGM_NM':'program',
    'UNIT_ERND_BUSN_NM':'unit','DTL_BUSN_NM':'detail',
    'OFCL_BUDGET_AMT':'budget_init','CRNT_BUDGET_AMT':'budget_cur',
    'EXCUT_AMT':'exec_amt','BF_CRNT_BUDGET_AMT':'budget_prev','SETL_AMT':'settle_amt',
}
AMT_FIELDS = {'budget_init','budget_cur','exec_amt','budget_prev','settle_amt'}

def normalize(row):
    if not isinstance(row, dict): return {}
    out = {}
    for src, dst in FIELD_MAP.items():
        v = row.get(src, '')
        if dst in AMT_FIELDS:
            try: out[dst] = int(str(v).replace(',','').strip() or '0')
            except: out[dst] = 0
        else:
            out[dst] = str(v).strip() if v else ''
    out['budget_supp']     = out.get('budget_cur',0) - out.get('budget_init',0)
    out['is_climate']      = out.get('program','') in CLIMATE_PROGRAMS
    out['is_climate_field']= out.get('field','') in CLIMATE_FIELDS
    return out

def aggregate(rows, year):
    by_prog, by_field, by_sector, by_min = {},{},{},{}
    for r in rows:
        if not isinstance(r, dict): continue
        prog=r.get('program',''); field=r.get('field','')
        sector=r.get('sector',''); ministry=r.get('ministry','')
        cur=r.get('budget_cur',0) or r.get('budget_init',0)
        init=r.get('budget_init',0); exec_a=r.get('exec_amt',0)
        if prog:
            if prog not in by_prog:
                by_prog[prog]={'budget_cur':0,'budget_init':0,'exec_amt':0,
                               'is_climate':r.get('is_climate',False),'field':field,'sector':sector}
            by_prog[prog]['budget_cur']+=cur; by_prog[prog]['budget_init']+=init; by_prog[prog]['exec_amt']+=exec_a
        if field: by_field[field]=by_field.get(field,0)+cur
        if sector: by_sector[sector]=by_sector.get(sector,0)+cur
        if ministry:
            if ministry not in by_min: by_min[ministry]={'budget_cur':0,'budget_init':0}
            by_min[ministry]['budget_cur']+=cur; by_min[ministry]['budget_init']+=init
    gov_total = sum((r.get('budget_cur',0) or r.get('budget_init',0)) for r in rows if isinstance(r,dict))
    clim_total= sum((r.get('budget_cur',0) or r.get('budget_init',0)) for r in rows if isinstance(r,dict) and r.get('is_climate'))
    return {'year':year,'by_program':by_prog,'by_field':by_field,'by_sector':by_sector,
            'by_ministry':by_min,'gov_total':gov_total,'climate_total':clim_total}

def fetch_all_pages(endpoint, year, page_size=300, max_pages=50):
    base_params = {'key':API_KEY,'Type':'json','FSCL_YR':str(year),'pIndex':'1','pSize':str(page_size)}
    first = api_call(endpoint, base_params)

    # 디버그 출력
    log(f"  [디버그] 응답 키: {list(first.keys())}")
    ep_root = first.get(endpoint)
    if isinstance(ep_root, dict):
        log(f"  [디버그] {endpoint} 하위 키: {list(ep_root.keys())}, totalCount={ep_root.get('totalCount')}, list길이={len(ep_root.get('list') or [])}")

    total, rows = parse_response(first, endpoint)
    if not rows:
        log(f"  {year}년 데이터 없음 (total={total})")
        log(f"  [디버그] 전체응답: {json.dumps(first, ensure_ascii=False)[:600]}")
        return []

    all_rows = list(rows)
    if total == 0: total = len(rows)
    pages = min(max_pages, max(1,(total+page_size-1)//page_size))
    log(f"  {year}년 총 {total:,}건 ({pages}페이지)")

    for page in range(2, pages+1):
        data_p = api_call(endpoint, {**base_params,'pIndex':str(page)})
        _, pr = parse_response(data_p, endpoint)
        if not pr: log(f"  {page}p 빈응답 → 완료"); break
        all_rows.extend(pr)
        log(f"  {page}/{pages}p ({len(all_rows):,}건)")
        time.sleep(0.3)
    return all_rows

def save(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path,'w',encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, separators=(',',':'))
    log(f"  💾 {path} ({path.stat().st_size//1024}KB)")

def main():
    if not API_KEY:
        log("❌ FISCAL_API_KEY 환경변수 없음"); sys.exit(1)

    log("="*56)
    log(f"기후재정 데이터 수집 v2 | 연도: {YEARS} | Key: {'*'*(len(API_KEY)-4)}{API_KEY[-4:]}")
    log("="*56)

    # 사전 연결 테스트
    log("\n🔍 API 연결 테스트...")
    try:
        test = api_call('dFUncBudgetInfo', {'key':API_KEY,'Type':'json','FSCL_YR':str(YEARS[-1]),'pIndex':'1','pSize':'1'})
        log(f"  응답 키: {list(test.keys())}")
        root = test.get('dFUncBudgetInfo',{})
        if isinstance(root, dict):
            log(f"  dFUncBudgetInfo 키: {list(root.keys())}")
            lst = root.get('list',[])
            if lst and isinstance(lst[0], dict):
                log(f"  첫 행 키: {list(lst[0].keys())[:10]}")
        log("  ✅ 연결 성공")
    except Exception as e:
        log(f"  ❌ 연결 실패: {e}"); sys.exit(1)

    collected, errors = [], []

    for year in YEARS:
        log(f"\n📅 {year}년 예산")
        try:
            raw  = fetch_all_pages('dFUncBudgetInfo', year)
            norm = [r for r in (normalize(r) for r in raw) if r]
            agg  = aggregate(norm, year)
            save(DATA_DIR/f'budget_{year}.json', {
                'year':year,'updated':datetime.now().isoformat(),'total':len(norm),
                'summary':agg,
                'climate_rows':[r for r in norm if r.get('is_climate') or r.get('is_climate_field')],
            })
            collected.append(year)
            log(f"  ✅ {year}년: 전체 {len(norm):,}건, 기후 {sum(1 for r in norm if r.get('is_climate'))}건")
        except Exception as e:
            log(f"  ❌ {year}년 오류: {e}"); errors.append({'year':year,'error':str(e)})
        time.sleep(0.5)

        log(f"\n📅 {year}년 결산")
        try:
            raw_s = fetch_all_pages('dFUncSettleInfo', year)
            if raw_s:
                norm_s = [r for r in (normalize(r) for r in raw_s) if r]
                save(DATA_DIR/f'settle_{year}.json', {
                    'year':year,'updated':datetime.now().isoformat(),'total':len(norm_s),
                    'climate_rows':[r for r in norm_s if r.get('is_climate') or r.get('is_climate_field')],
                })
                log(f"  ✅ {year}년 결산: {len(norm_s):,}건")
            else:
                log(f"  ℹ {year}년 결산 없음")
        except Exception as e:
            log(f"  ⚠ {year}년 결산 오류: {e}")
        time.sleep(0.5)

    save(DATA_DIR/'meta.json', {
        'last_updated':datetime.now().isoformat(),
        'collected_years':collected,'errors':errors,
        'source':'openapi.openfiscaldata.go.kr',
        'climate_programs':sorted(CLIMATE_PROGRAMS),
    })

    log(f"\n완료: 성공 {len(collected)}개, 오류 {len(errors)}개")
    for e in errors: log(f"  ❌ {e['year']}: {e['error']}")

    if not collected: sys.exit(1)

if __name__ == '__main__':
    main()
