import shutil
import os
import sys
import time
import signal
import asyncio
import html
import json
import logging
import re
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException, BackgroundTasks
import uuid
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel
import requests
import base64

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Moduły aplikacji
from app.core.config import config_manager, config_ai_manager
from app.services.database import db_service
from app.services.zapisy_service import ZapisyService
from app.services.router import jpk_router, ai_router, api_router
from app.services.router_obszary import router as obszary_router
from app.services.agent_chat import AgentChat

# Define lifespan event handler to manage startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Start aplikacji (lifespan startup)...")
    app.state.last_heartbeat = time.time()
    app.state.ai_sessions = {}

    async def check_connection():
        # Początkowy grace period (5s) na start systemu
        await asyncio.sleep(5)
        while True:
            await asyncio.sleep(5)
            # Kill if no heartbeat for > Configured delay
            timeout = config_manager.config.server_timeout
            if time.time() - app.state.last_heartbeat > timeout:
                print(f"Brak aktywności (timeout {timeout}s) - zamykanie serwera...")
                os._exit(0)
                break

    asyncio.create_task(check_connection())

    logger.info("Wczytywanie konfiguracji...")

    last_db = config_manager.get_last_db()
    if last_db and Path(last_db).exists():
        try:
            msg = f"Otwieranie bazy: {Path(last_db).name}..."
            logger.info(msg)
            db_service.connect(last_db)
            print(f"Restored last database: {last_db}")
        except Exception as e:
            err_msg = f"KRYTYCZNY BŁĄD BAZY: {e}"
            logger.critical(err_msg)
            config_manager.set_last_db(None)
    
    yield
    # Shutdown logic
    try:
        db_service.close()
    except:
        pass

app = FastAPI(lifespan=lifespan)
app.include_router(ai_router)
app.include_router(api_router)
app.include_router(obszary_router)


@app.middleware("http")
async def add_privacy_headers(request: Request, call_next):
    response = await call_next(request)
    # List of routes containing sensitive JPK data
    sensitive_paths = ["/zois", "/dziennik", "/active-podmiot-name"]
    if request.url.path in sensitive_paths:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

@app.post("/api/hard-reset")
async def hard_reset():
    logger.info("Zarządzono twardy restart aplikacji.")
    
    async def _do_shutdown():
        await asyncio.sleep(0.5)
        try:
            db_service.close()
        except Exception as e:
            logger.error(f"Błąd zamykania bazy: {e}")
        os._exit(5) # Kod 5 oznacza prośbę o RESTART dla launchera
    
    asyncio.create_task(_do_shutdown())
    return HTMLResponse("System is restarting...")

@app.post("/api/heartbeat")
async def heartbeat():
    app.state.last_heartbeat = time.time()
    return {"status": "ok"}


@app.get("/health")
async def health_check():
    return {"status": "ok"}

class ZScoreRequest(BaseModel):
    record_ids: List[int]
    min_amount: float

class AnomalyRequest(BaseModel):
    record_ids: List[int]

@app.get("/api/parameters/{jpk_id}")
async def get_parameters(jpk_id: str):
    """Zwraca wartość Istotnosc_Trywialna dla danego badania."""
    # jpk_id is currently ignored as we use the active database connection
    val = db_service.get_trivial_materiality()
    return {"Istotnosc_Trywialna": val}

@app.post("/api/test/z-score")
async def test_z_score(request_data: ZScoreRequest):
    """Endpoint do wykrywania anomalii Z-Score z progiem kwotowym."""
    anomaly_ids = db_service.get_zscore_anomaly_ids(request_data.record_ids, request_data.min_amount)
    return {"anomaly_ids": anomaly_ids}

@app.post("/api/zois/map-accounts")
def map_zois_accounts(
    bz: float = Form(...),
    bo: Optional[float] = Form(None),
    side: str = Form("Wn"),
    prefixes: str = Form("")
):
    try:
        matched_leaves = db_service.find_account_mapping(bz_target=bz, bo_target=bo, side=side, prefixes=prefixes, timeout=30)
        
        if not matched_leaves:
            return {"status": "error", "message": "Nie znaleziono dopasowania"}
            
        conn = db_service.get_connection()
        rows = conn.execute("SELECT S_1, S_3 FROM ZOiS").fetchall()
        parent_map = {row['S_1']: row['S_3'] for row in rows}
        
        forced_ids = set(matched_leaves)
        for leaf in matched_leaves:
            curr = leaf
            while curr in parent_map and parent_map[curr]:
                if parent_map[curr] == curr: break
                curr = parent_map[curr]
                forced_ids.add(curr)
                
        return {"status": "success", "forced_ids": list(forced_ids)}
    except TimeoutError:
        return {"status": "error", "message": "Zbyt długi czas wyszukiwania. Spróbuj zawęzić prefixy."}
    except Exception as e:
        logger.error(f"Map accounts error: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/test/z-score-anomalies")
async def test_z_score_anomalies(request_data: AnomalyRequest):
    """Legacy endpoint (aliased to new logic with 0 threshold)."""
    anomaly_ids = db_service.detect_zscore_anomalies(request_data.record_ids)
    return {"anomaly_ids": anomaly_ids}

from collections import defaultdict

@app.get("/api/download_raport_ksiegowan")
async def download_raport_ksiegowan():
    try:
        conn = db_service.get_connection()
        query = """
            SELECT Z_GrupaKont, Z_4, Z_7, Dziennik_Id 
            FROM Zapisy 
            WHERE Z_GrupaKont IS NOT NULL AND Z_GrupaKont NOT LIKE '9%'
            ORDER BY Dziennik_Id
        """
        rows = conn.execute(query).fetchall()
        
        groups = defaultdict(lambda: {"wn": [], "ma": []})
        for row in rows:
            gid = row["Dziennik_Id"]
            syn = row["Z_GrupaKont"]
            wn = float(row["Z_4"] or 0)
            ma = float(row["Z_7"] or 0)
            if wn > 0:
                groups[gid]["wn"].append({"syn": syn, "val": wn})
            if ma > 0:
                groups[gid]["ma"].append({"syn": syn, "val": ma})
                
        account_wn_sources = defaultdict(lambda: defaultdict(float))
        account_ma_targets = defaultdict(lambda: defaultdict(float))
        
        for gid, data in groups.items():
            tot_wn = sum(x["val"] for x in data["wn"])
            tot_ma = sum(x["val"] for x in data["ma"])
            if tot_wn == 0 or tot_ma == 0:
                continue
                
            for w in data["wn"]:
                for m in data["ma"]:
                    flow_wn = w["val"] * (m["val"] / tot_ma)
                    flow_ma = m["val"] * (w["val"] / tot_wn)
                    account_wn_sources[w["syn"]][m["syn"]] += flow_wn
                    account_ma_targets[m["syn"]][w["syn"]] += flow_ma
                    
        all_accounts = set(account_wn_sources.keys()).union(account_ma_targets.keys())
        sorted_accounts = sorted(list(all_accounts))
        
        zois_names = {row['S_1']: row['S_2'] for row in conn.execute("SELECT S_1, S_2 FROM ZOiS").fetchall()}

        md_lines = []
        for acc in sorted_accounts:
            name = zois_names.get(acc, "")
            header = f"Konto {acc} - {name}" if name else f"Konto {acc}"
            md_lines.append(header)
            
            w_sources = account_wn_sources.get(acc, {})
            if w_sources:
                suma_wn_exact = sum(w_sources.values())
                suma_wn_round = round(suma_wn_exact)
                if suma_wn_round > 0:
                    md_lines.append(f"    Obroty Wn {suma_wn_round}")
                    for src_acc in sorted(w_sources.keys()):
                        amt = w_sources[src_acc]
                        amt_round = round(amt)
                        if amt_round > 0:
                            pct = round((amt / suma_wn_exact) * 100) if suma_wn_exact > 0 else 0
                            md_lines.append(f"        Konto {src_acc} o kwocie {amt_round} ({pct}%)")
                            
            m_targets = account_ma_targets.get(acc, {})
            if m_targets:
                suma_ma_exact = sum(m_targets.values())
                suma_ma_round = round(suma_ma_exact)
                if suma_ma_round > 0:
                    md_lines.append(f"    Obroty Ma {suma_ma_round}")
                    for tgt_acc in sorted(m_targets.keys()):
                        amt = m_targets[tgt_acc]
                        amt_round = round(amt)
                        if amt_round > 0:
                            pct = round((amt / suma_ma_exact) * 100) if suma_ma_exact > 0 else 0
                            md_lines.append(f"        Konto {tgt_acc} o kwocie {amt_round} ({pct}%)")
                            
        md_content = "\r\n".join(md_lines)
        return Response(
            content=md_content,
            media_type="text/markdown; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=Raport_Ksiegowan.md"}
        )
    except Exception as e:
        logger.error(f"Raport error: {e}")
        return HTMLResponse(f"Błąd generowania raportu: {str(e)}", status_code=500)

@app.get("/api/download_raport_graf_img")
async def download_raport_graf_img():
    try:
        conn = db_service.get_connection()
        query = """
            SELECT Z_GrupaKont, Z_4, Z_7, Dziennik_Id 
            FROM Zapisy 
            WHERE Z_GrupaKont IS NOT NULL AND Z_GrupaKont NOT LIKE '9%'
            ORDER BY Dziennik_Id
        """
        rows = conn.execute(query).fetchall()
        
        groups = defaultdict(lambda: {"wn": [], "ma": []})
        for row in rows:
            gid = row["Dziennik_Id"]
            syn = row["Z_GrupaKont"]
            wn = float(row["Z_4"] or 0)
            ma = float(row["Z_7"] or 0)
            if wn > 0:
                groups[gid]["wn"].append({"syn": syn, "val": wn})
            if ma > 0:
                groups[gid]["ma"].append({"syn": syn, "val": ma})
                
        account_wn_sources = defaultdict(lambda: defaultdict(float))
        account_ma_targets = defaultdict(lambda: defaultdict(float))
        
        for gid, data in groups.items():
            tot_wn = sum(x["val"] for x in data["wn"])
            tot_ma = sum(x["val"] for x in data["ma"])
            if tot_wn == 0 or tot_ma == 0:
                continue
                
            for w in data["wn"]:
                for m in data["ma"]:
                    flow = w["val"] * (m["val"] / tot_ma)
                    account_wn_sources[w["syn"]][m["syn"]] += flow
                    account_ma_targets[m["syn"]][w["syn"]] += flow
                    
        mermaid_lines = ["graph LR"]
        edges_added = set()
        
        def get_acc_id(acc_name):
            return "acc_" + str(acc_name).replace("-", "_").replace(".", "_")

        for acc in set(account_wn_sources.keys()).union(account_ma_targets.keys()):
            acc_id = get_acc_id(acc)
            
            # Wypływy (Obroty Ma) - strzałka OD nas DO kogoś (Konto -> Tgt)
            m_targets = account_ma_targets.get(acc, {})
            suma_ma_exact = sum(m_targets.values())
            if suma_ma_exact > 0:
                for tgt_acc, amt in m_targets.items():
                    pct = (amt / suma_ma_exact) * 100
                    if pct > 20:
                        tgt_acc_id = get_acc_id(tgt_acc)
                        edge_key = f"{acc_id}->{tgt_acc_id}"
                        if edge_key not in edges_added:
                            pct_round = round(pct)
                            mermaid_lines.append(f'    {acc_id}["{acc}"] -->|{pct_round}%| {tgt_acc_id}["{tgt_acc}"]')
                            edges_added.add(edge_key)
            
            # Wpływy (Obroty Wn) - strzałka OD kogoś DO nas (Src -> Konto)
            w_sources = account_wn_sources.get(acc, {})
            suma_wn_exact = sum(w_sources.values())
            if suma_wn_exact > 0:
                for src_acc, amt in w_sources.items():
                    pct = (amt / suma_wn_exact) * 100
                    if pct > 20:
                        src_acc_id = get_acc_id(src_acc)
                        edge_key = f"{src_acc_id}->{acc_id}"
                        if edge_key not in edges_added:
                            pct_round = round(pct)
                            mermaid_lines.append(f'    {src_acc_id}["{src_acc}"] -->|{pct_round}%| {acc_id}["{acc}"]')
                            edges_added.add(edge_key)

        if len(mermaid_lines) == 1:
            mermaid_lines.append('    Empty["Brak powiązań > 20%"]')

        mmd_text = "\n".join(mermaid_lines)
        
        # Call Mermaid.ink API to generate PNG
        # Encode syntax in base64 as required by Mermaid Ink
        # We use standard Mermaid Ink endpoint: https://mermaid.ink/img/<base64>
        mmd_b64 = base64.b64encode(mmd_text.encode('utf-8')).decode('utf-8')
        img_url = f"https://mermaid.ink/img/{mmd_b64}"
        
        img_response = requests.get(img_url, timeout=15)
        if img_response.status_code == 200:
            return Response(
                content=img_response.content,
                media_type="image/png",
                headers={"Content-Disposition": "attachment; filename=Raport_Graf.png"}
            )
        else:
            return HTMLResponse(content=f"Błąd API Mermaid: {img_response.status_code}", status_code=500)
            
    except Exception as e:
        logger.error(f"Graf IMG error: {e}")
        return HTMLResponse(content=str(e), status_code=500)

# Setup Templates & Static Files
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# Routes
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    # Fetch parameters for badanie_tab.html
    params = {}
    try:
        conn = db_service.get_connection()
        keys = ["Biegly", "Istotnosc_Ogolna", "Istotnosc_Wykonawcza", "Istotnosc_Trywialna", "Uwagi"]
        for key in keys:
            row = conn.execute("SELECT Tekst, Kwota FROM ParametryBadania WHERE Klucz = ?", (key,)).fetchone()
            if row:
                kwota = int(round(row["Kwota"])) if row["Kwota"] is not None else 0
                params[key] = {"Tekst": row["Tekst"], "Kwota": kwota}
            else:
                params[key] = {"Tekst": "", "Kwota": 0}
    except:
        # Fallback if no DB or table doesn't exist yet
        params = {k: {"Tekst": "", "Kwota": 0} for k in ["Biegly", "Istotnosc_Ogolna", "Istotnosc_Wykonawcza", "Istotnosc_Trywialna", "Uwagi"]}

    # Fetch config for ai_tab.html
    config = config_ai_manager.get_config()
    
    # Fetch AI Assistant setting for asystent_ai_tab.html
    # We use db_path="" as it is only for config reading
    dummy_agent = AgentChat(db_path="")
    ai_assistant = {
        "api_key": dummy_agent.config.get("api_key", "")
    }
    
    grouped_obszary = {}
    try:
        conn = db_service.get_connection()
        rows = conn.execute("SELECT Id, Nazwa, Typ FROM Obszary ORDER BY Typ, Id").fetchall()
        for row in rows:
            typ = row['Typ'] or "Inne"
            if typ not in grouped_obszary:
                grouped_obszary[typ] = []
            grouped_obszary[typ].append(dict(row))
    except Exception:
        pass
        
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "params": params, 
        "config": config,
        "ai_assistant": ai_assistant,
        "grouped_obszary": grouped_obszary
    })

@app.get("/api/settings/ai-assistant")
async def get_ai_assistant_settings():
    dummy_agent = AgentChat(db_path="")
    return {
        "api_key": dummy_agent.config.get("api_key", "")
    }

@app.post("/api/settings/ai-assistant")
async def save_ai_assistant_settings(
    request: Request,
    api_key: str = Form("")
):
    try:
        dummy_agent = AgentChat(db_path="")
        dummy_agent.update_config(api_key)
        
        # Clear active AI sessions in state so they re-init on next use
        if hasattr(request.app.state, "ai_sessions"):
            request.app.state.ai_sessions.clear()
            
        return HTMLResponse(content="Ustawienia Asystenta AI zapisane")
    except Exception as e:
        return HTMLResponse(content=f"Błąd zapisu: {e}", status_code=500)

@app.post("/upload", response_class=HTMLResponse)
@app.post("/import", response_class=HTMLResponse)
async def import_jpk(file: UploadFile = File(...)):
    upload_dir = Path("app/data/uploads")
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    safe_filename = Path(file.filename).name
    file_path = upload_dir / safe_filename
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Uruchomienie importu w osobnym wątku, czekając na wynik
        db_path_str = await asyncio.to_thread(
            jpk_router.route_import, 
            str(file_path)
        )
        db_path = Path(db_path_str)
        db_name = db_path.name
        
        # Connect and persist
        db_service.connect(str(db_path))
        config_manager.set_last_db(str(db_path))
        
        return HTMLResponse(f"""
            <div class="flex flex-col items-center justify-center py-10">
                <span class="loading loading-spinner loading-lg text-success"></span>
                <div class="mt-4 text-sm font-bold text-success">Finalizowanie importu...</div>
                
                <script>
                    // 1. Odśwież dane w tle
                    htmx.trigger('body', 'db-changed');
                    
                    // 2. Wyświetl okno walidacji (ono jest najważniejsze)
                    htmx.trigger('body', 'show-consistency');
                    
                    // 3. Zamknij to okno (import_modal) automatycznie
                    const modal = document.getElementById('import_modal');
                    if (modal) modal.close();
                    
                    // 4. Aktualizacja etykiety bazy
                    const label = document.getElementById('active-db-label');
                    if (label) label.innerText = "{db_name}";
                </script>
            </div>
        """)
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        error_msg = html.escape(str(e))
        return HTMLResponse(content=f"""
            <div class="flex flex-col items-center text-center gap-6 py-6">
                <div class="bg-error/20 p-6 rounded-full shadow-inner">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-14 w-14 text-error" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="3" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                    </svg>
                </div>
                
                <div class="space-y-1">
                    <h4 class="text-2xl font-black text-error tracking-tight">Błąd importu</h4>
                    <p class="text-xs opacity-60">Wystąpił krytyczny problem podczas przetwarzania pliku.</p>
                </div>

                <div class="w-full bg-error/5 p-4 rounded-xl border border-error/20 text-left">
                    <pre class="text-[10px] text-error font-mono whitespace-pre-wrap leading-relaxed">{error_msg}</pre>
                </div>

                <div class="w-full pt-4">
                    <button class="btn btn-ghost w-full border-base-300" 
                            onclick="document.getElementById('import_modal').close()">
                        Zamknij i spróbuj ponownie
                    </button>
                </div>
            </div>
        """, status_code=200)

@app.post("/connect-db")
async def connect_db(file: UploadFile = File(...)):
    # Create databases folder if not exists
    db_dir = Path("databases")
    db_dir.mkdir(exist_ok=True)
    
    # Path Traversal protection
    safe_filename = Path(file.filename).name
    target_path = db_dir / safe_filename
    
    with open(target_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    try:
        db_service.verify_db(str(target_path))
        db_service.connect(str(target_path))
        config_manager.set_last_db(str(target_path))
        
        response = HTMLResponse(content='<div id="alert-container" hx-swap-oob="true"></div>')
        response.headers["HX-Trigger"] = "db-changed"
        return response
    except Exception as e:
        if target_path.exists():
            target_path.unlink()
        
        # XSS protection
        escaped_error = html.escape(str(e))
        alert_html = f"""
        <div id="alert-container" hx-swap-oob="true" class="fixed bottom-4 right-4 z-50">
            <div class="alert alert-error shadow-lg border-2 border-base-100/20">
                <svg xmlns="http://www.w3.org/2000/svg" class="stroke-current shrink-0 h-6 w-6" fill="none" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                <span class="text-xs font-bold leading-tight">{escaped_error}</span>
                <button class="btn btn-ghost btn-xs" onclick="document.getElementById('alert-container').innerHTML=''">✕</button>
            </div>
        </div>
        """
        return HTMLResponse(content=alert_html)

@app.get("/active-db-name", response_class=HTMLResponse)
async def get_active_db_name():
    last_db = config_manager.get_last_db()
    if last_db:
        return Path(last_db).name
    return "Brak"

@app.get("/active-podmiot-name", response_class=HTMLResponse)
async def get_active_podmiot_name():
    try:
        conn = db_service.get_connection()
        row = conn.execute("SELECT PelnaNazwa FROM Podmiot").fetchone()
        if row and row['PelnaNazwa']:
            return row['PelnaNazwa']
    except:
        pass
    return "---"

@app.get("/recent", response_class=HTMLResponse)
async def get_recent():
    recent = config_manager.get_recent_dbs()
    html_out = ""
    for db_path in recent:
        p = Path(db_path)
        name = html.escape(p.name)
        # Shorten path for subtitle
        parts = p.parts
        if len(parts) > 3:
            short_path = html.escape(str(Path(*parts[-3:-1])))
        else:
            short_path = html.escape(str(p.parent))
            
        vals = html.escape(json.dumps({"path": db_path}))
        
        html_out += f"""
        <li class="group relative flex flex-col p-2 hover:bg-base-200 transition-colors rounded-lg cursor-pointer"
            hx-post="/select-db" hx-vals='{vals}' hx-swap="none" hx-indicator="#main-spinner">
            <div class="truncate font-bold text-xs">{name}</div>
            <div class="text-[9px] opacity-40 truncate pr-16">{short_path}</div>
            
            <div class="absolute right-2 top-1/2 -translate-y-1/2 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity bg-base-100/80 backdrop-blur-sm pl-2 rounded-l-lg"
                 onclick="event.stopPropagation()">
                <button hx-post="/open-db-folder" hx-vals='{vals}' hx-swap="none" hx-indicator="#main-spinner"
                        class="btn btn-ghost btn-xs btn-square" title="Otwórz lokalizację">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
                    </svg>
                </button>
                <button hx-post="/detach-db" hx-vals='{vals}' hx-swap="none" hx-indicator="#main-spinner"
                        class="btn btn-ghost btn-xs btn-square text-warning" title="Odłącz (usuń z listy)">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <line x1="2" y1="2" x2="22" y2="22"></line>
                        <path d="M18 13v1a4 4 0 0 1-4 4h-1"></path>
                        <path d="M10 18h-1a4 4 0 0 1-4-4v-1"></path>
                        <path d="M6 11V10a4 4 0 0 1 4-4h1"></path>
                        <path d="M14 6h1a4 4 0 0 1 4 4v1"></path>
                    </svg>
                </button>
                <button hx-post="/delete-db" hx-vals='{vals}' hx-confirm="CZY NA PEWNO CHCESZ TRWALE USUNĄĆ PLIK BAZY: {name}?" hx-swap="none" hx-indicator="#main-spinner"
                        class="btn btn-ghost btn-xs btn-square text-error" title="USUŃ PLIK Z DYSKU">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                    </svg>
                </button>
            </div>
        </li>
        """
    return html_out

@app.post("/detach-db")
async def detach_db(path: str = Form(...)):
    config_manager.remove_from_recent(path)
    return HTMLResponse(content="Detached", headers={"HX-Trigger": "db-changed"})

@app.post("/delete-db")
async def delete_db(path: str = Form(...)):
    try:
        norm_path = Path(path).resolve()
        
        # Close ALL connections to this path across all threads (essential for Windows)
        db_service.close_all_connections_to_path(norm_path)
        
        # If it was the active DB, clear the state
        if db_service.current_db_path and db_service.current_db_path.resolve() == norm_path:
            config_manager.set_last_db(None)
            db_service.current_db_path = None
            
        if norm_path.exists():
            norm_path.unlink()
            
        config_manager.remove_from_recent(str(norm_path).replace("\\", "/"))
        return HTMLResponse(content="Deleted", headers={"HX-Trigger": "db-changed"})
    except Exception as e:
        logger.error(f"Failed to delete database: {e}")
        return HTMLResponse(content=f"Błąd usuwania pliku (może być otwarty): {html.escape(str(e))}", status_code=500)

@app.post("/open-db-folder")
async def open_db_folder(path: str = Form(...)):
    try:
        folder = Path(path).parent
        if folder.exists():
            os.startfile(str(folder)) # Windows only
        return HTMLResponse(content="Opened")
    except Exception as e:
        logger.error(f"Failed to open folder: {e}")
        return HTMLResponse(content=f"Error: {e}", status_code=500)


@app.post("/select-db")
async def select_db(path: str = Form(...)):
    requested_path = Path(path).resolve()
    
    # Path whitelist: resolve against app's known data folders
    base_uploads = Path("app/data/uploads").resolve()
    base_databases = Path("databases").resolve()
    
    # Boundary check
    is_allowed = False
    try:
        # Check if requested path is inside allowed directories
        is_allowed = requested_path.is_relative_to(base_uploads) or requested_path.is_relative_to(base_databases)
    except (ValueError, AttributeError):
        # Fallback for older python or complex path errors
        is_allowed = str(requested_path).startswith(str(base_uploads)) or str(requested_path).startswith(str(base_databases))

    if not is_allowed:
        logger.warning(f"Security: Blocked path traversal attempt at {requested_path}")
        raise HTTPException(status_code=403, detail="Forbidden: Unauthorized directory access")

    if requested_path.exists():
        db_service.connect(str(requested_path))
        config_manager.set_last_db(str(requested_path))
        response = HTMLResponse(content="OK")
        response.headers["HX-Trigger"] = "db-changed"
        return response
    raise HTTPException(status_code=404, detail="Database file not found")

def format_amount(value):
    if value is None:
        return ""
    try:
        val_float = float(value)
        if val_float == 0:
            return ""
        # Format with 2 decimal places and comma as decimal separator
        # Then use space as thousands separator
        formatted = "{:,.2f}".format(val_float).replace(",", " ").replace(".", ",")
        return formatted
    except (ValueError, TypeError):
        return ""

def sanitize_text(text):
    if text is None: return ""
    # Aggressive cleaning: replace all newline variants with space, then strip
    raw_text = str(text)
    return raw_text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').strip()

def polish_sort_key(text):
    if not text: return ""
    # Map Polish characters to base character + a unique sorting suffix.
    # Using a dictionary with str.maketrans allows mapping 1 character to multiple characters correctly.
    # \ufff0 and \ufff1 are used as suffixes that sort after standard alphanumeric characters.
    d = {
        'ą': 'a\ufff0', 'ć': 'c\ufff0', 'ę': 'e\ufff0', 'ł': 'l\ufff0', 'ń': 'n\ufff0',
        'ó': 'o\ufff0', 'ś': 's\ufff0', 'ź': 'z\ufff0', 'ż': 'z\ufff1',
        'Ą': 'a\ufff0', 'Ć': 'c\ufff0', 'Ę': 'e\ufff0', 'Ł': 'l\ufff0', 'Ń': 'n\ufff0',
        'Ó': 'o\ufff0', 'Ś': 's\ufff0', 'Ź': 'z\ufff0', 'Ż': 'z\ufff1'
    }
    return text.translate(str.maketrans(d)).lower()

# parse_smart_query and _build_where_clause moved to db_service

def build_zois_tree(rows):
    nodes = {}
    for row in rows:
        node = dict(row)
        node['children'] = []
        nodes[node['S_1']] = node
        
    tree = []
    for node in nodes.values():
        s_1 = node['S_1']
        s_3 = node['S_3']
        if not s_3 or s_1 == s_3 or s_3 not in nodes:
            tree.append(node)
        else:
            nodes[s_3]['children'].append(node)
    return tree

def render_zois_tree(nodes, depth=0, expand_all=False):
    html = ""
    padding_base = 0.5 # rem
    padding = depth * padding_base
    
    # Generate tree lines for hierarchy visualization
    tree_lines = "".join([f'<div class="absolute border-l border-base-content/10 h-full" style="left: {i * 0.5 + 0.25}rem; top: 0;"></div>' for i in range(depth)])
    
    for node in nodes:
        konto = node['S_1']
        nazwa = node['S_2']
        bo_wn = format_amount(node['S_4'])
        bo_ma = format_amount(node['S_5'])
        obroty_wn = format_amount(node['S_8'])
        obroty_ma = format_amount(node['S_9'])
        saldo_wn = format_amount(node['S_10'])
        saldo_ma = format_amount(node['S_11'])
        
        is_analytical = node.get('IsAnalytical', 0) == 1
        has_children = len(node['children']) > 0
        
        konto_full = sanitize_text(konto)
        nazwa_full = sanitize_text(nazwa)
        
        # Improved Truncation & Tooltip Logic
        # Escape quotes for HTML data-tip attribute
        nazwa_escaped = nazwa_full.replace('"', '&quot;')
        konto_escaped = konto_full.replace('"', '&quot;')

        # Account code tooltip (longer than 15 chars)
        if len(konto_full) > 15:
            konto_html = f'<div class="tooltip tooltip-right z-50" data-tip="{konto_escaped}"><div class="truncate max-w-[9rem]">{konto_full}</div></div>'
        else:
            konto_html = f'<div class="truncate">{konto_full}</div>'

        # Account name tooltip (Extra wide and multiline)
        if nazwa_full:
            nazwa_html = f'<div class="tooltip tooltip-right tooltip-multiline text-left z-50 w-full" data-tip="{nazwa_escaped}"><div class="truncate w-full">{nazwa_full}</div></div>'
        else:
            nazwa_html = f'<div class="truncate">{nazwa_full}</div>'
            
        # Minimalist mapping icon logic
        obszar_id = node.get('Obszar_Id')
        is_direct = node.get('Is_Direct', 0)
        
        if is_direct:
            icon_class = "text-success"
            icon_title = f"Mapowanie bezpośrednie (ID: {obszar_id})"
        elif obszar_id:
            icon_class = "text-info opacity-40"
            icon_title = "Dziedziczy mapowanie z konta nadrzędnego"
        else:
            icon_class = "opacity-20"
            icon_title = "Brak mapowania - kliknij, aby przypisać"
        
        obszar_html = f"""
        <div class="flex justify-center">
            <button class="btn btn-ghost btn-xs btn-square {icon_class}" 
                    hx-get="/api/zois/get-map-modal?konto_id={konto_full}" 
                    hx-target="#map_obszar_modal" 
                    hx-swap="innerHTML"
                    onclick="document.getElementById('map_obszar_modal').showModal(); event.preventDefault(); event.stopPropagation();" 
                    title="{icon_title}">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
                </svg>
            </button>
        </div>
        """

        nazwa_html = f'<div class="flex items-center w-full min-w-0">{nazwa_html}</div>'
            
        weight_class = ""
        if is_analytical or not has_children:
            weight_class = "font-normal text-base-content/80 text-[11px]"
        else:
            weight_class = "font-bold text-base-content text-xs"

        expander_svg = '<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 shrink-0 transition-transform duration-200" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" /></svg>'

        # Grid config: Konto (10rem), Nazwa (flex), Obszar (3rem), Amounts (6x 7rem)
        grid_class = f"grid grid-cols-[10rem_minmax(0,1fr)_3rem_7rem_7rem_7rem_7rem_7rem_7rem] gap-4 p-1 border-b border-base-content/5 items-center hover:bg-base-200 w-full overflow-hidden {weight_class}"


        if has_children:
            children_html = render_zois_tree(node['children'], depth + 1, expand_all)
            open_attr = ' open' if expand_all else ''
            child_class = ' child-row' if depth > 0 else ''
            html += f"""
            <details class="group zoistree{child_class}"{open_attr}>
                <summary class="list-none [&::-webkit-details-marker]:hidden outline-none cursor-pointer">
                    <div class="{grid_class}">
                        <div class="flex items-center gap-1 relative h-full min-w-0" style="padding-left: {padding}rem;">
                            {tree_lines}
                            <span class="z-10 group-open:rotate-90 transition-transform duration-200 inline-block cursor-pointer p-0.5 hover:bg-base-300 rounded" onclick="this.closest('details').open = !this.closest('details').open; event.preventDefault(); event.stopPropagation();">{expander_svg}</span>
                            <div class="z-10 flex-grow hover:underline min-w-0" onclick="drillDown('{konto_full}'); event.preventDefault(); event.stopPropagation();">{konto_html}</div>
                        </div>
                        <div class="hover:underline min-w-0" onclick="drillDown('{konto_full}'); event.preventDefault(); event.stopPropagation();">{nazwa_html}</div>
                        <div>{obszar_html}</div>
                        <div class="text-right font-mono min-w-0" onclick="drillDown('{konto_full}'); event.preventDefault(); event.stopPropagation();">{sanitize_text(bo_wn)}</div>
                        <div class="text-right font-mono min-w-0" onclick="drillDown('{konto_full}'); event.preventDefault(); event.stopPropagation();">{sanitize_text(bo_ma)}</div>
                        <div class="text-right font-mono min-w-0" onclick="drillDown('{konto_full}'); event.preventDefault(); event.stopPropagation();">{sanitize_text(obroty_wn)}</div>
                        <div class="text-right font-mono min-w-0" onclick="drillDown('{konto_full}'); event.preventDefault(); event.stopPropagation();">{sanitize_text(obroty_ma)}</div>
                        <div class="text-right font-mono min-w-0" onclick="drillDown('{konto_full}'); event.preventDefault(); event.stopPropagation();">{sanitize_text(saldo_wn)}</div>
                        <div class="text-right font-mono min-w-0" onclick="drillDown('{konto_full}'); event.preventDefault(); event.stopPropagation();">{sanitize_text(saldo_ma)}</div>
                    </div>
                </summary>
                <div>
                    {children_html}
                </div>
            </details>
            """
        else:
            empty_expander = '<span class="inline-block w-4 h-4 shrink-0 mx-0.5"></span>'
            child_class = ' child-row' if depth > 0 else ''
            html += f"""
            <div class="{grid_class} cursor-pointer{child_class}" onclick="drillDown('{konto_full}')">
                <div class="flex items-center gap-1 relative h-full min-w-0" style="padding-left: {padding}rem;">
                    {tree_lines}
                    <div class="z-10 flex items-center gap-1 min-w-0">
                        {empty_expander}
                        <span class="hover:underline min-w-0">{konto_html}</span>
                    </div>
                </div>
                <div class="hover:underline min-w-0">{nazwa_html}</div>
                <div>{obszar_html}</div>
                <div class="text-right font-mono min-w-0">{sanitize_text(bo_wn)}</div>
                <div class="text-right font-mono min-w-0">{sanitize_text(bo_ma)}</div>
                <div class="text-right font-mono min-w-0">{sanitize_text(obroty_wn)}</div>
                <div class="text-right font-mono min-w-0">{sanitize_text(obroty_ma)}</div>
                <div class="text-right font-mono min-w-0">{sanitize_text(saldo_wn)}</div>
                <div class="text-right font-mono min-w-0">{sanitize_text(saldo_ma)}</div>
            </div>
            """
            
    return html

@app.get("/zois", response_class=HTMLResponse)
async def get_zois(request: Request, q: str = "", type: str = "", forced_ids: str = "", 
                   synthetic: str = "true", expand: str = "false", empty: str = "true", obszar_id: str = ""):
    is_synthetic = synthetic.lower() == "true"
    is_expand = expand.lower() == "true"
    is_empty = empty.lower() == "true"
    # Skeleton of table
    try:
        conn = db_service.get_connection()
        
        forced_ids_list = forced_ids.split(",") if forced_ids else None
        
        # Ensure empty strings are treated as None for filtering purposes in build_zois_where
        # This allows build_zois_where to correctly ignore parameters that are not provided.
        # The instruction for build_zois_where is applied conceptually here.
        where_sql, params = db_service.build_zois_where(
            q=q if q else None,
            type=type if type else None,
            forced_ids=forced_ids_list,
            synthetic=is_synthetic,
            empty=is_empty
        )

        try:
            # Używamy denormalizowanych kolumn (Obszar_Id, Strona_Salda, Is_Direct_Mapping) dla maksymalnej wydajności
            sql = f"""
                SELECT S_1, S_2, S_3, S_4, S_5, S_8, S_9, S_10, S_11, IsAnalytical, TypKonta,
                       Obszar_Id, Strona_Salda, Is_Direct_Mapping AS Is_Direct
                FROM ZOiS
                {where_sql}
                ORDER BY S_1
            """
            rows = conn.execute(sql, params).fetchall()
            
            row_dicts = [dict(r) for r in rows]
            
            if obszar_id:
                try:
                    target_obszar = int(obszar_id)
                    matched_s1 = set(r['S_1'] for r in row_dicts if r['Obszar_Id'] == target_obszar)
                    
                    keep_s1 = set(matched_s1)
                    parent_map = {r['S_1']: r['S_3'] for r in row_dicts}
                    
                    for s1 in matched_s1:
                        curr = s1
                        while curr in parent_map and parent_map[curr] and parent_map[curr] != curr:
                            curr = parent_map[curr]
                            keep_s1.add(curr)
                            
                    row_dicts = [r for r in row_dicts if r['S_1'] in keep_s1]
                except ValueError:
                    pass
            
            # Calculate totals and check types for the filtered set - only analytical accounts for sums
            if obszar_id:
                try:
                    target_obszar = int(obszar_id)
                    totals_sql = f"""
                        SELECT 
                            SUM(S_10) as sum_wn, 
                            SUM(S_11) as sum_ma,
                            COUNT(S_1) as total_count,
                            SUM(CASE WHEN TypKonta LIKE 'PASYWA%' OR TypKonta LIKE 'WYNIKOWE%' THEN 1 ELSE 0 END) as special_count
                        FROM ZOiS
                        WHERE Obszar_Id = :oid AND IsAnalytical = 1
                        {where_sql.replace('WHERE', 'AND') if where_sql else ""}
                    """
                    totals_params = params.copy()
                    totals_params["oid"] = target_obszar
                except ValueError:
                    totals_where = where_sql + (" AND " if where_sql else "WHERE ") + "IsAnalytical = 1"
                    totals_sql = f"""
                        SELECT 
                            SUM(S_10) as sum_wn, 
                            SUM(S_11) as sum_ma,
                            COUNT(*) as total_count,
                            SUM(CASE WHEN TypKonta LIKE 'PASYWA%' OR TypKonta LIKE 'WYNIKOWE%' THEN 1 ELSE 0 END) as special_count
                        FROM ZOiS {totals_where}
                    """
                    totals_params = params
            else:
                totals_where = where_sql + (" AND " if where_sql else "WHERE ") + "IsAnalytical = 1"
                totals_sql = f"""
                    SELECT 
                        SUM(S_10) as sum_wn, 
                        SUM(S_11) as sum_ma,
                        COUNT(*) as total_count,
                        SUM(CASE WHEN TypKonta LIKE 'PASYWA%' OR TypKonta LIKE 'WYNIKOWE%' THEN 1 ELSE 0 END) as special_count
                    FROM ZOiS {totals_where}
                """
                totals_params = params
                
            totals = conn.execute(totals_sql, totals_params).fetchone()
            sum_wn = totals['sum_wn'] or 0
            sum_ma = totals['sum_ma'] or 0
            total_count = totals['total_count'] or 0
            special_count = totals['special_count'] or 0
            
            # Determine Persaldo formula: Ma - Wn if ALL accounts are PASYWA or WYNIKOWE
            is_special = total_count > 0 and special_count == total_count
            persaldo = sum_ma - sum_wn if is_special else sum_wn - sum_ma
            persaldo_label = "Persaldo (Ma-Wn)" if is_special else "Persaldo (Wn-Ma)"
            
        except Exception as e:
            print(f"ZOiS Error: {e}")
            return "<div>Brak danych w tabeli ZOiS.</div>"
            
        # Build and render the tree structure
        tree_nodes = build_zois_tree(row_dicts)
        html_rows = render_zois_tree(tree_nodes, expand_all=is_expand)
        
        if not html_rows:
            html_rows = "<div class='text-center p-4 border-b border-base-content/10'>Brak danych</div>"
            
        transition_btn = ""
        if obszar_id:
            # Pobieramy nazwę obszaru dla czytelniejszego przycisku
            obszar_name = "tego obszaru"
            try:
                res = conn.execute("SELECT Nazwa FROM Obszary WHERE Id = ?", (obszar_id,)).fetchone()
                if res:
                    obszar_name = res['Nazwa']
            except:
                pass

            transition_btn = f"""
            <button class="btn btn-primary btn-sm gap-2 shadow-sm"
                    onclick="document.getElementById('tab-zapisy').click(); const sel = document.getElementById('filter-obszar-zapisy'); sel.value='{obszar_id}'; sel.dispatchEvent(new Event('change'));">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 5l7 7-7 7M5 5l7 7-7 7" /></svg>
                Pokaż zapisy dla obszaru {html.escape(obszar_name)}
            </button>
            """
            
        return f"""
        <div class="flex flex-col h-full overflow-hidden">
            <!-- Summation Panel (Top) -->
            <div class="mb-4 p-3 bg-base-200 rounded-xl border border-base-content/5 flex justify-between items-center gap-8 px-6 shadow-inner">
                <div>{transition_btn}</div>
                <div class="flex gap-8">
                    <div class="flex flex-col items-end">
                        <span class="text-[9px] font-bold opacity-50 uppercase leading-none mb-1">Suma Saldo Wn</span>
                        <span class="text-lg font-black text-secondary font-mono">{format_amount(sum_wn)}</span>
                    </div>
                    <div class="flex flex-col items-end border-l border-base-content/10 pl-8">
                        <span class="text-[9px] font-bold opacity-50 uppercase leading-none mb-1">Suma Saldo Ma</span>
                        <span class="text-lg font-black text-secondary font-mono">{format_amount(sum_ma)}</span>
                    </div>
                    <div class="flex flex-col items-end border-l border-base-content/10 pl-8">
                        <span class="text-[9px] font-bold opacity-50 uppercase leading-none mb-1">{persaldo_label}</span>
                        <span class="text-lg font-black text-primary font-mono">{format_amount(persaldo)}</span>
                    </div>
                </div>
            </div>

            <!-- Table Container (Bottom) -->
            <div class="overflow-x-auto flex-grow bg-base-100 pb-8">
                <div class="w-full">
                    <!-- Tree Table Header -->
                    <div class="grid grid-cols-[10rem_minmax(0,1fr)_3rem_7rem_7rem_7rem_7rem_7rem_7rem] gap-4 p-2 border-b-2 border-base-content/20 text-[10px] font-bold text-base-content/60 uppercase tracking-wider bg-base-200 sticky top-0 z-10 shadow-sm w-full overflow-hidden" style="padding-left: 0.5rem;">
                        <div class="min-w-0">Konto</div>
                        <div class="min-w-0">Nazwa</div>
                        <div class="text-center min-w-0">Obszar</div>
                        <div class="text-right min-w-0">BO Wn</div>
                        <div class="text-right min-w-0">BO Ma</div>
                        <div class="text-right min-w-0">Obroty N Wn</div>
                        <div class="text-right min-w-0">Obroty N Ma</div>
                        <div class="text-right min-w-0">Saldo Wn</div>
                        <div class="text-right min-w-0">Saldo Ma</div>
                    </div>
                    <!-- Tree Body -->
                    <div class="flex flex-col">
                        {html_rows}
                    </div>
                </div>
            </div>
        </div>
        """
    except RuntimeError:
        return "<div class='alert alert-warning'>Nie otwarto żadnej bazy danych. Zaimportuj plik JPK.</div>"
    except Exception as e:
        return f"<div class='alert alert-error'>Błąd pobierania danych: {e}</div>"

@app.get("/zois/filters", response_class=HTMLResponse)
async def get_zois_filters():
    try:
        conn = db_service.get_connection()
        rows = conn.execute("SELECT DISTINCT TypKonta FROM ZOiS WHERE TypKonta IS NOT NULL AND TypKonta != ''").fetchall()
        
        # Polish-aware sorting
        sorted_types = sorted([r["TypKonta"] for r in rows], key=polish_sort_key)
        
        options = '<option value="">Wszystkie (Typ Konta)</option>'
        for val in sorted_types:
            options += f'<option value="{val}">{val}</option>'
        return options
    except Exception as e:
        logger.error(f"Error getting ZOiS filters: {e}")
        return '<option value="">Wszystkie (Typ Konta)</option>'



@app.get("/dziennik", response_class=HTMLResponse)
async def get_dziennik():
    try:
        conn = db_service.get_connection()
        # Updated Dziennik mapping:
        # Nr dziennika -> D_1, Opis -> D_10, Nr dowodu -> D_4, 
        # Data zapisu -> D_8, Wartość -> D_11, KSeF -> D_12
        rows = conn.execute("SELECT D_1, D_10, D_4, D_8, D_11, D_12 FROM Dziennik ORDER BY D_8 LIMIT 500").fetchall()
        
        html_rows = ""
        for row in rows:
            opis = sanitize_text(row['D_10'])
            html_rows += f"""
            <tr class="hover whitespace-nowrap">
                <td data-type="text">{sanitize_text(row['D_1'])}</td>
                <td data-type="text" class="max-w-md truncate">{opis}</td>
                <td data-type="text">{sanitize_text(row['D_4'])}</td>
                <td data-type="date" data-value="{row['D_8'] or ''}">{sanitize_text(row['D_8'])}</td>
                <td data-type="number" data-value="{row['D_11'] or 0}" class="text-right font-mono">{sanitize_text(format_amount(row['D_11']))}</td>
                <td data-type="text">{sanitize_text(row['D_12'])}</td>
            </tr>
            """
        
        if not html_rows:
            html_rows = "<tr><td colspan='6' class='text-center'>Brak danych</td></tr>"
            
        return f"""
        <div class="overflow-x-auto">
            <table class="table table-xs">
                <thead>
                    <tr>
                        <th>Nr dziennika</th>
                        <th>Opis</th>
                        <th>Nr dowodu</th>
                        <th>Data zapisu</th>
                        <th class="text-right">Wartość</th>
                        <th>KSeF</th>
                    </tr>
                </thead>
                <tbody>
                    {html_rows}
                </tbody>
            </table>
        </div>
        """
    except RuntimeError:
        return "<div class='alert alert-warning'>Nie otwarto żadnej bazy danych.</div>"
    except Exception as e:
        return f"<div class='alert alert-error'>Błąd: {e}</div>"

@app.get("/data/table", response_class=HTMLResponse)
@app.get("/data/zapisy", response_class=HTMLResponse)
@app.get("/zapisy", response_class=HTMLResponse)
async def get_zapisy(q: str = "", type: str = "", zq: str = "", month: str = "", details: str = "", with_details: bool = False, page: int = 1, konto: str = "", opis: str = "", min_kwota: str = "", adv_sort: str = "", dziennik_id: str = "", adv_z3: str = "", adv_z2: str = "", adv_min_kwota: str = "", adv_d1: str = "", obszar_id: str = ""):
    pageSize = 1000
    offset = (page - 1) * pageSize
    try:
        # Compatibility mapping
        final_konto = konto or adv_z3
        final_opis = opis or adv_z2
        final_min_kwota = min_kwota or adv_min_kwota
        final_dziennik_id = dziennik_id or adv_d1

        db_path = str(db_service.current_db_path) if db_service.current_db_path else ""
        z_service = ZapisyService(db_path)
        
        # Merge with_details and legacy details parameter
        show_details = with_details or details == "1"
        
        # Use the service to get full records and totals
        data = z_service.get_zapisy_pelne(
            q=q, type=type, zq=zq, month=month, 
            konto=final_konto, opis=final_opis, 
            min_kwota=final_min_kwota, dziennik_id=final_dziennik_id,
            limit=pageSize, offset=offset, adv_sort=adv_sort,
            with_details=show_details, obszar_id=obszar_id
        )
        
        rows = data['rows']
        sum_wn = data['sum_wn']
        sum_ma = data['sum_ma']

        html_rows = ""
        is_details = details == "1"
        # Since we want "Konta Przeciwstawne" to be visible when details or with_details is active
        # let's use show_details for the internal checks as well
        use_with_details = show_details 
        for row in rows:
            dziennik_id = row['Dziennik_Id']
            konto_full = sanitize_text(row['Z_3'])
            opis_full = sanitize_text(row['Opis_Zapisu'])
            data_val = row['Z_Data'] or "---"
            nazwa_full = sanitize_text(row.get('Nazwa_Konta', '---'))
            
            # Escape strings for attributes
            tip_content = f"{html.escape(konto_full)}\\n{html.escape(nazwa_full)}"
            konto_html = f'<div class="tooltip tooltip-right text-left" data-tip="{tip_content}"><div class="max-w-[150px] truncate">{html.escape(konto_full)}</div></div>'
            opis_html = f'<div class="tooltip tooltip-right text-left" data-tip="{html.escape(opis_full)}"><div class="max-w-xs truncate">{html.escape(opis_full)}</div></div>'

            # Opposite accounts styling
            przeciwstawne_html = ""
            if show_details:
                kont_prz = row.get('Konta_Przeciwstawne') or '---'
                przeciwstawne_html = f'<td><kbd class="kbd kbd-xs bg-base-300/50 text-[10px] font-normal">{html.escape(kont_prz)}</kbd></td>'

            if is_details:
                dowod = sanitize_text(row['Dowod'])
                kontrahent = sanitize_text(row['Kontrahent'])
                ksef = sanitize_text(row['KSeF'])
                
                operacja_full = sanitize_text(row['Operacja'])
                if len(operacja_full) > 50:
                    op_trimmed = operacja_full[:47] + "..."
                    operacja_html = f'<div class="tooltip tooltip-right text-left" data-tip="{html.escape(operacja_full)}"><div class="max-w-[200px] truncate">{html.escape(op_trimmed)}</div></div>'
                else:
                    operacja_html = f'<div class="max-w-[200px] truncate">{html.escape(operacja_full)}</div>'

                html_rows += f"""
                <tr class="hover whitespace-nowrap text-xs" data-id="{row['id_zapisu']}">
                    <td data-type="text">{konto_html}</td>
                    {przeciwstawne_html}
                    <td data-type="text">{opis_html}</td>
                    <td data-type="date" data-value="{row['Z_Data'] or ''}">{data_val}</td>
                    <td data-type="text" class="text-primary cursor-pointer hover:underline" onclick="openDziennikModal({dziennik_id})">{row['Numer_Dziennika']}</td>
                    <td data-type="number" data-value="{row['Kwota_Wn'] or 0}" class="text-right font-mono">{format_amount(row['Kwota_Wn'])}</td>
                    <td data-type="number" data-value="{row['Kwota_Ma'] or 0}" class="text-right font-mono">{format_amount(row['Kwota_Ma'])}</td>
                    <td data-type="text">{dowod}</td>
                    <td data-type="text" data-value="{html.escape(operacja_full)}">{operacja_html}</td>
                    <td data-type="text">{kontrahent}</td>
                    <td data-type="text">{ksef}</td>
                </tr>
                """
            else:
                wn_waluta_val = format_amount(row['Kwota_Waluta_Wn'])
                wn_waluta_code = row['Kod_Waluty_Wn'] or ""
                wn_waluta_str = f"{wn_waluta_val} {wn_waluta_code}".strip() if wn_waluta_val else ""
                ma_waluta_val = format_amount(row['Kwota_Waluta_Ma'])
                ma_waluta_code = row['Kod_Waluty_Ma'] or ""
                ma_waluta_str = f"{ma_waluta_val} {ma_waluta_code}".strip() if ma_waluta_val else ""

                html_rows += f"""
                <tr class="hover whitespace-nowrap text-xs" data-id="{row['id_zapisu']}">
                    <td data-type="text">{konto_html}</td>
                    {przeciwstawne_html}
                    <td data-type="text">{opis_html}</td>
                    <td data-type="date" data-value="{row['Z_Data'] or ''}">{data_val}</td>
                    <td data-type="text" class="text-primary cursor-pointer hover:underline" onclick="openDziennikModal({dziennik_id})">{row['Numer_Dziennika']}</td>
                    <td data-type="number" data-value="{row['Kwota_Wn'] or 0}" class="text-right font-mono">{format_amount(row['Kwota_Wn'])}</td>
                    <td data-type="text" class="text-right font-mono">{wn_waluta_str}</td>
                    <td data-type="number" data-value="{row['Kwota_Ma'] or 0}" class="text-right font-mono">{format_amount(row['Kwota_Ma'])}</td>
                    <td data-type="text" class="text-right font-mono">{ma_waluta_str}</td>
                </tr>
                """
        
        # Header additional column
        prz_header = "<th>Konta Przec.</th>" if show_details else ""

        if is_details:
            headers_html = f"""
                <th>Konto</th>
                {prz_header}
                <th>Opis</th>
                <th>Data</th>
                <th>Dziennik</th>
                <th class="text-right">Kwota Wn</th>
                <th class="text-right">Kwota Ma</th>
                <th>Dowód</th>
                <th>Operacja</th>
                <th>Kontrahent</th>
                <th>KSeF</th>
            """
            colspan = 11 if show_details else 10
        else:
            headers_html = f"""
                <th>Konto</th>
                {prz_header}
                <th>Opis</th>
                <th>Data</th>
                <th>Dziennik</th>
                <th class="text-right">Kwota Wn</th>
                <th class="text-right">Waluta Wn</th>
                <th class="text-right">Kwota Ma</th>
                <th class="text-right">Waluta Ma</th>
            """
            colspan = 9 if show_details else 8

        if not html_rows:
            html_rows = f"<tr><td colspan='{colspan}' class='text-center'>Brak danych</td></tr>"
            
        return f"""
        <div class="mb-4 p-3 bg-base-200 rounded-xl border border-base-content/5 flex gap-8 justify-end items-center shadow-inner">
            <div class="flex flex-col items-end">
                <span class="text-[9px] uppercase font-bold opacity-50">Suma Winien (Wn)</span>
                <span class="text-lg font-black text-secondary font-mono">{format_amount(sum_wn)}</span>
            </div>
            <div class="flex flex-col items-end">
                <span class="text-[9px] uppercase font-bold opacity-50">Suma Ma</span>
                <span class="text-lg font-black text-accent font-mono">{format_amount(sum_ma)}</span>
            </div>
            <div class="flex flex-col items-end pl-6 border-l border-base-content/10">
                <span class="text-[9px] uppercase font-bold opacity-50">Saldo</span>
                <span class="text-lg font-black font-mono">{format_amount(sum_wn - sum_ma)}</span>
            </div>
        </div>
        <div class="overflow-x-auto">
            <table id="zapisy-table" class="table table-xs">
                <thead>
                    <tr>
                        {headers_html}
                    </tr>
                </thead>
                <tbody>
                    {html_rows}
                </tbody>
            </table>
            </div>
        </div>
        """
    except Exception as e:
        return f"<div class='alert alert-error font-mono text-xs'>Błąd: {str(e)}</div>"






@app.get("/dziennik-details/{dziennik_id}", response_class=HTMLResponse)
async def get_dziennik_details(dziennik_id: int):
    try:
        conn = db_service.get_connection()
        # 1. Fetch journal details
        row = conn.execute("SELECT * FROM Dziennik WHERE Id = ?", (dziennik_id,)).fetchone()
        
        if not row:
            return "<div class='alert alert-error'>Nie znaleziono wpisu w dzienniku.</div>"
            
        # 2. Fetch associated entries
        entries = conn.execute("""
            SELECT z.Z_3, z.Z_2, z.Z_4, z.Z_5, z.Z_6, z.Z_7, z.Z_8, z.Z_9, s.S_2
            FROM Zapisy z
            JOIN ZOiS s ON z.Z_3 = s.S_1
            WHERE z.Dziennik_Id = ?
            ORDER BY z.Id
        """, (dziennik_id,)).fetchall()

        def field_html(label, value, is_amount=False, is_bold=False):
            val_str = format_amount(value) if is_amount else sanitize_text(value)
            weight_class = "font-black text-secondary" if is_bold else "font-medium text-base-content/80"
            return f"""
            <div class="flex flex-col min-w-0">
                <span class="text-[8px] font-bold opacity-50 uppercase tracking-tighter leading-none mb-0.5">{label}</span>
                <span class="text-[12px] {weight_class} truncate leading-tight">{"---" if not val_str else val_str}</span>
            </div>
            """

        # Build entries table rows
        table_rows = ""
        for ent in entries:
            # Currency Wn
            wn_val = format_amount(ent['Z_5'])
            wn_code = ent['Z_6'] or ""
            wn_waluta = f"{wn_val} {wn_code}".strip() if wn_val else ""
            
            # Currency Ma
            ma_val = format_amount(ent['Z_8'])
            ma_code = ent['Z_9'] or ""
            ma_waluta = f"{ma_val} {ma_code}".strip() if ma_val else ""

            # Opis Tooltip for Modal: Multi-line support and ensure it's always applied
            full_opis = sanitize_text(ent['Z_2'])
            opis_html = f'<div class="tooltip tooltip-bottom text-left" data-tip="{full_opis}"><div class="max-w-[150px] truncate">{full_opis}</div></div>'

            # Truncate and Tooltip for Konto in Modal
            full_konto = sanitize_text(ent['Z_3'])
            nazwa_full = sanitize_text(ent['S_2'])
            
            # Konto Tooltip for Modal: Multi-line support
            tip_content = f"{full_konto}\n{nazwa_full or '---'}"
            konto_html = f'<div class="tooltip tooltip-right text-left" data-tip="{tip_content}"><div class="max-w-[100px] truncate">{full_konto}</div></div>'

            table_rows += f"""
            <tr class="hover whitespace-nowrap text-[10px]">
                <td data-type="text">{konto_html}</td>
                <td data-type="text">{opis_html}</td>
                <td data-type="number" data-value="{ent['Z_4'] or 0}" class="text-right font-mono">{format_amount(ent['Z_4'])}</td>
                <td data-type="text" class="text-right font-mono text-gray-500">{wn_waluta}</td>
                <td data-type="number" data-value="{ent['Z_7'] or 0}" class="text-right font-mono">{format_amount(ent['Z_7'])}</td>
                <td data-type="text" class="text-right font-mono text-gray-500">{ma_waluta}</td>
            </tr>
            """

        return f"""
        <div class="flex flex-col gap-3">
            <!-- Header section (Condensed Grid) -->
            <div class="bg-base-200/50 p-3 rounded-lg border border-base-300">
                <div class="grid grid-cols-4 gap-x-4 gap-y-3">
                    {field_html("Numer zapisu", row['D_1'])}
                    {field_html("Dowód źródłowy", row['D_5'])}
                    {field_html("Dowód", row['D_4'])}
                    {field_html("Kwota", row['D_11'], is_amount=True, is_bold=True)}
                    
                    {field_html("Data operacji", row['D_6'])}
                    {field_html("Data dowodu", row['D_7'])}
                    {field_html("Data księgowania", row['D_8'])}
                    {field_html("Kontrahent", row['D_3'])}

                    <div class="col-span-2">
                        {field_html("Opis operacji", row['D_10'])}
                    </div>
                    <div class="col-span-1">
                        {field_html("Opis dziennika", row['D_2'])}
                    </div>
                    <div class="col-span-1">
                        {field_html("KSeF", row['D_12'])}
                    </div>
                </div>
            </div>

            <!-- Entries section -->
            <div class="flex-grow min-h-0">
                <h4 class="text-[10px] font-bold uppercase opacity-60 mb-1.5 px-1">Zapisy skojarzone</h4>
                <div class="border border-base-300 rounded-lg overflow-hidden h-full">
                    <div class="overflow-y-auto max-h-[400px] bg-base-100">
                        <table class="table table-xs table-pin-rows w-full">
                            <thead>
                                <tr class="bg-base-200">
                                    <th>Konto</th>
                                    <th>Opis</th>
                                    <th class="text-right">Wn</th>
                                    <th class="text-right">Wn Wal.</th>
                                    <th class="text-right">Ma</th>
                                    <th class="text-right">Ma Wal.</th>
                                </tr>
                            </thead>
                            <tbody>
                                {table_rows or '<tr><td colspan="6" class="text-center py-4 opacity-50 italic">Brak zapisów skojarzonych</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
        """
    except Exception as e:
        return f"<div class='alert alert-error'>Błąd: {e}</div>"

@app.get("/api/ai-config", response_class=HTMLResponse)
async def get_ai_config():
    config = config_ai_manager.get_config()
    return HTMLResponse(content=config.model_dump_json())

@app.post("/api/ai-config")
async def save_ai_config(
    company_type: str = Form(...),
    multiplier: float = Form(None),
    deviation: float = Form(None),
    threshold: float = Form(None),
    normalization_100: str = Form("false"),
    prompt: str = Form(None)
):
    update_data = {
        "company_type": company_type,
        "normalization_100": normalization_100.lower() in ["true", "on"]
    }
    
    if multiplier is not None:
        update_data["multiplier"] = multiplier
    if deviation is not None:
        update_data["deviation"] = deviation
    if threshold is not None:
        update_data["threshold"] = threshold
        
    if prompt is not None:
        update_data["prompts"] = {company_type: prompt}
        
    config_ai_manager.update_config(update_data)
    return HTMLResponse(content="Ustawienia zapisane", headers={"HX-Trigger": "ai-config-updated"})

@app.get("/api/ai-prompt", response_class=HTMLResponse)
async def get_ai_prompt(type: str = ""):
    prompt = config_ai_manager.get_prompt_for_type(type if type else None)
    return HTMLResponse(content=prompt)

@app.get("/api/export-llm-tab", response_class=HTMLResponse)
async def get_export_llm_tab(request: Request):
    config = config_ai_manager.get_config()
    return templates.TemplateResponse("export_llm_tab.html", {"request": request, "config": config})

@app.get("/api/badanie-tab", response_class=HTMLResponse)
async def get_badanie_tab(request: Request):
    try:
        conn = db_service.get_connection()
        keys = ["Biegly", "Istotnosc_Ogolna", "Istotnosc_Wykonawcza", "Istotnosc_Trywialna", "Uwagi"]
        params = {}
        for key in keys:
            row = conn.execute("SELECT Tekst, Kwota FROM ParametryBadania WHERE Klucz = ?", (key,)).fetchone()
            if row:
                # Round to integer as requested
                kwota = int(round(row["Kwota"])) if row["Kwota"] is not None else 0
                params[key] = {"Tekst": row["Tekst"], "Kwota": kwota}
            else:
                params[key] = {"Tekst": "", "Kwota": 0}
        
        return templates.TemplateResponse("badanie_tab.html", {"request": request, "params": params})
    except RuntimeError:
        return HTMLResponse(content="<div class='alert alert-warning'>Nie otwarto żadnej bazy danych. Proszę zaimportować lub podłączyć bazę.</div>")
    except Exception as e:
        print(f"Błąd Badanie Tab: {e}")
        return HTMLResponse(content=f"<div class='alert alert-error'>Błąd: {e}</div>")

@app.post("/api/save-badanie")
async def save_badanie(
    biegly: str = Form(""),
    istotnosc_ogolna: str = Form("0"),
    istotnosc_wykonawcza: str = Form("0"),
    istotnosc_trywialna: str = Form("0"),
    uwagi: str = Form("")
):
    try:
        # Clean and round to integer
        try:
            val_ogolna = int(round(float(istotnosc_ogolna.replace(" ", ""))))
            val_wykonawcza = int(round(float(istotnosc_wykonawcza.replace(" ", ""))))
            val_trywialna = int(round(float(istotnosc_trywialna.replace(" ", ""))))
        except (ValueError, TypeError):
            val_ogolna = 0
            val_wykonawcza = 0
            val_trywialna = 0

        conn = db_service.get_connection()
        data = [
            ("Biegly", "Kluczowy biegły rewident", biegly, None),
            ("Istotnosc_Ogolna", "Istotność Ogólna", None, val_ogolna),
            ("Istotnosc_Wykonawcza", "Istotność Wykonawcza", None, val_wykonawcza),
            ("Istotnosc_Trywialna", "Kwota pomijalna w badaniu", None, val_trywialna),
            ("Uwagi", "Uwagi do pliku", uwagi, None)
        ]
        
        for klucz, opis, tekst, kwota in data:
            conn.execute("""
                INSERT INTO ParametryBadania (Klucz, Opis, Tekst, Kwota)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(Klucz) DO UPDATE SET
                    Tekst = excluded.Tekst,
                    Kwota = excluded.Kwota,
                    Opis = excluded.Opis
            """, (klucz, opis, tekst, kwota))
        
        conn.commit()
        return HTMLResponse(content="Parametry zapisane", headers={"HX-Trigger": "badanie-updated"})
    except Exception as e:
        print(f"Błąd zapisu Badanie: {e}")
        return HTMLResponse(content=f"Błąd zapisu: {e}", status_code=500)

@app.get("/api/get-ai-modal", response_class=HTMLResponse)
async def get_ai_modal():
    prompt = config_ai_manager.get_prompt_for_type()
    
    # Stable modal fragment using modal-toggle trick or simple open attribute
    # Here we use the standard showModal() call triggered by hx-on
    return f"""
    <dialog id="ai_stable_modal" class="modal modal-open">
        <div class="modal-box w-11/12 max-w-3xl relative">
            <button class="btn btn-sm btn-circle btn-ghost absolute right-2 top-2" onclick="this.closest('dialog').remove()">✕</button>
            
            <h3 class="font-bold text-lg mb-4">Przygotowanie danych dla AI</h3>
            <p class="text-[11px] mb-4 opacity-70">
                Skopiuj prompt i pobierz plik CSV. Następnie wklej oba elementy do narzędzia AI.
            </p>

            <div class="relative group mb-6">
                <textarea id="ai-modal-prompt-text"
                    class="textarea textarea-bordered w-full h-64 text-[11px] font-mono leading-relaxed bg-base-200"
                    readonly>{prompt}</textarea>
                <button class="btn btn-sm btn-primary absolute top-2 right-2" onclick="copyAiModalPrompt(this)">
                    Kopiuj Prompt
                </button>
            </div>

            <div class="flex justify-between items-center bg-base-200 p-4 rounded-lg">
                <div class="flex flex-col">
                    <span class="text-xs font-bold">Plik danych (CSV)</span>
                    <span class="text-[10px] opacity-60">Kompleksowy eksport ZOiS z slownikiem kategorii</span>
                </div>
                <a href="/api/export-ai-full" class="btn btn-secondary btn-sm gap-2">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a2 2 0 002 2h12a2 2 0 002-2v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                    </svg>
                    Pobierz dane (CSV)
                </a>
            </div>

            <div class="modal-action">
                <button class="btn" onclick="this.closest('dialog').remove()">Zamknij</button>
            </div>
            
            <script>
                function copyAiModalPrompt(btn) {{
                    const textArea = document.getElementById('ai-modal-prompt-text');
                    navigator.clipboard.writeText(textArea.value);

                    const originalText = btn.innerText;
                    btn.innerText = 'Skopiowano!';
                    btn.classList.replace('btn-primary', 'btn-success');
                    setTimeout(() => {{
                        btn.innerText = originalText;
                        btn.classList.replace('btn-success', 'btn-primary');
                    }}, 2000);
                }}
            </script>
        </div>
    </dialog>
    """

@app.get("/api/export-ai-full")
async def export_ai_full():
    try:
        conn = db_service.get_connection()
        ai_config = config_ai_manager.get_config()
        
        sql = """
            SELECT 
                COALESCE(z.TypKonta, 'BRAK') AS [Typ konta],
                SUM(z.S_4) AS [BO Wn],
                SUM(z.S_5) AS [BO Ma],
                SUM(z.S_8) AS [Obroty Wn],
                SUM(z.S_9) AS [Obroty Ma],
                SUM(z.S_10) AS [BZ Wn],
                SUM(z.S_11) AS [BZ Ma]
            FROM ZOiS z
            GROUP BY [Typ konta]
            ORDER BY [Typ konta];
        """
        rows = conn.execute(sql).fetchall()
        
        import io
        import csv
        import random
        from collections import defaultdict

        # Helper to determine group
        def get_group(typ_konta):
            if not typ_konta or typ_konta == 'BRAK':
                return 'BRAK'
            first_word = typ_konta.split()[0].upper()
            if first_word in ['AKTYWA', 'PASYWA', 'WYNIKOWE']:
                return first_word
            return 'INNE'

        # Groups ordering and Columns
        GROUP_ORDER = ['AKTYWA', 'PASYWA', 'WYNIKOWE', 'INNE', 'BRAK']
        COLUMNS = ['BO Wn', 'BO Ma', 'Obroty Wn', 'Obroty Ma', 'BZ Wn', 'BZ Ma']

        multiplier = ai_config.multiplier
        dev_percent = ai_config.deviation / 100.0 if ai_config.deviation else 0
        threshold_percent = ai_config.threshold / 100.0 if ai_config.threshold else 0.005
        is_norm = ai_config.normalization_100

        # --- STEP 1: Apply Noise (Cell-level) and Grouping ---
        grouped_data = defaultdict(list)
        sum_abs_aktywa = 0.0
        sum_abs_wynikowe = 0.0

        for row in rows:
            grp = get_group(row['Typ konta'])
            noisy_row = {'Typ konta': row['Typ konta'], 'Grupa': grp}
            
            for col in COLUMNS:
                r_noise = random.uniform(1.0 - dev_percent, 1.0 + dev_percent)
                val = float(row[col] or 0)
                noisy_val = val * r_noise
                noisy_row[col] = noisy_val
                
                # For relational scaling ratio calculation (Step 2)
                if col == 'BZ Wn':
                    if grp == 'AKTYWA':
                        sum_abs_aktywa += abs(noisy_val)
                    elif grp == 'WYNIKOWE':
                        sum_abs_wynikowe += abs(noisy_val)
                        
            grouped_data[grp].append(noisy_row)

        # --- STEP 2: Calculate Relational Scale Ratio ---
        ratio = (sum_abs_wynikowe / sum_abs_aktywa * 100.0) if sum_abs_aktywa != 0 else 0.0
        # Add scale noise
        ratio_noisy = ratio * random.uniform(1.0 - dev_percent, 1.0 + dev_percent)

        output_rows = []

        if is_norm:
            # --- STEP 3: Normalization, Thresholding & Balansowanie (DS 3.0 Mode) ---
            for grp_name in GROUP_ORDER:
                grp_rows = grouped_data.get(grp_name, [])
                if not grp_rows:
                    continue
                
                # 3.1: Calculate Internal Normalization (Initial shares)
                # First, we need to handle each column independently
                grp_col_sums = {}
                for col in COLUMNS:
                    # Use absolute sum to avoid division by zero if negative values cancel out
                    grp_col_sums[col] = sum(abs(r[col]) for r in grp_rows)

                # Convert to shares
                shares_data = []
                for r in grp_rows:
                    row_shares = {'Typ konta': r['Typ konta'], 'Grupa': grp_name}
                    for col in COLUMNS:
                        total = grp_col_sums[col]
                        row_shares[col] = (abs(r[col]) / total * 100.0) if total != 0 else 0.0
                    shares_data.append(row_shares)

                # 3.2: Thresholding and Aggregation (POZOSTAŁE)
                # Significant logic based on BZ Wn column (as per convention)
                significant_rows = []
                others_row = {'Typ konta': 'POZOSTAŁE (poniżej progu)', 'Grupa': grp_name}
                for col in COLUMNS: others_row[col] = 0.0
                has_others = False

                for r in shares_data:
                    # Check if BZ Wn share is below threshold
                    if r['BZ Wn'] < (threshold_percent * 100.0):
                        has_others = True
                        for col in COLUMNS:
                            others_row[col] += r[col]
                    else:
                        significant_rows.append(r)
                
                if has_others:
                    significant_rows.append(others_row)

                # 3.3: BALANSOWANIE (Uniform distribution of the error to 100.00%)
                for col in COLUMNS:
                    total_pct = sum(r[col] for r in significant_rows)
                    if total_pct > 0:
                        diff = 100.0 - total_pct
                        non_zero_elements = [r for r in significant_rows if r[col] != 0]
                        if non_zero_elements:
                            adj = diff / len(non_zero_elements)
                            for r in non_zero_elements:
                                r[col] += adj
                
                output_rows.extend(significant_rows)
        else:
            # --- Multiplier Mode (Standard Noisy Values) ---
            for grp_name in GROUP_ORDER:
                grp_rows = grouped_data.get(grp_name, [])
                for r in grp_rows:
                    row_val = {'Typ konta': r['Typ konta'], 'Grupa': r['Grupa']}
                    for col in COLUMNS:
                        row_val[col] = r[col] * multiplier
                    output_rows.append(row_val)

        # --- STEP 4: Write CSV Output ---
        output = io.StringIO()
        output.write('\ufeff')
        writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        
        if is_norm:
            # Mandatory DS 3.0 Metadata Header
            writer.writerow([
                "INFO", 
                f"SKALA RELACYJNA: WYNIKOWE STANOWIĄ {ratio_noisy:.2f}% AKTYWA", 
                "AKTYWA I PASYWA PRZYJĘTO JAKO 100%", "", "", "", "", ""
            ])
            # Mandatory Headers
            writer.writerow(['Grupa', 'Typ konta', 'BO Wn [%]', 'BO Ma [%]', 'Obroty Wn [%]', 'Obroty Ma [%]', 'BZ Wn [%]', 'BZ Ma [%]'])
            
            # Write data rows with % symbol
            for r in output_rows:
                writer.writerow([
                    r['Grupa'],
                    r['Typ konta'],
                    *[f"{r[col]:.2f}%" for col in COLUMNS]
                ])
        else:
            # Standard Multiplier CSV
            writer.writerow(['Grupa', 'Typ konta'] + COLUMNS)
            for r in output_rows:
                writer.writerow([
                    r['Grupa'],
                    r['Typ konta'],
                    *[f"{r[col]:.2f}" for col in COLUMNS]
                ])
            
        csv_data = output.getvalue()
        
        from fastapi.responses import Response
        return Response(
            content=csv_data,
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=zois_export_ai.csv"
            }
        )
    except Exception as e:
        return HTMLResponse(content=f"<div class='alert alert-error'>Błąd eksportu: {e}</div>", status_code=500)

@app.get("/podmiot", response_class=HTMLResponse)
async def get_podmiot():
    try:
        conn = db_service.get_connection()
        
        # 1. Naglowek
        naglowek = conn.execute("SELECT DataOd, DataDo, DataWytworzeniaJPK FROM Naglowek").fetchone()
        
        # 2. Podmiot
        podmiot = conn.execute("SELECT PelnaNazwa, NIP, Miejscowosc, Ulica, NrDomu FROM Podmiot").fetchone()
        
        # 3. CTRL
        ctrl = conn.execute("SELECT C_1, C_2, C_3 FROM Ctrl").fetchone()
        
        def safe_get(row, key, default=""):
            try:
                return row[key] if row and row[key] is not None else default
            except:
                return default

        # Formatting values
        pelna_nazwa = safe_get(podmiot, 'PelnaNazwa', '---')
        data_spr = f"{safe_get(naglowek, 'DataOd')} do {safe_get(naglowek, 'DataDo')}"
        
        # Reformat DataWytworzeniaJPK: "2026-01-26T14:41:08" -> "2026-01-26 o godzinie: 14:41:08"
        data_wytw_raw = safe_get(naglowek, 'DataWytworzeniaJPK', '---')
        if "T" in data_wytw_raw:
            date_part, time_part = data_wytw_raw.split("T")
            data_wytw = f"{date_part} o godzinie: {time_part}"
        else:
            data_wytw = data_wytw_raw

        nip = safe_get(podmiot, 'NIP', '---')
        miejscowosc = safe_get(podmiot, 'Miejscowosc', '---')
        adres = f"{safe_get(podmiot, 'Ulica')} {safe_get(podmiot, 'NrDomu')}".strip() or "---"
        
        c1 = safe_get(ctrl, 'C_1', '0')
        c2 = format_amount(safe_get(ctrl, 'C_2', 0)) or "0,00"
        c3 = safe_get(ctrl, 'C_3', '0')

        return f"""
        <div class="max-w-4xl mx-auto">
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                <!-- Data Podmiotu Card -->
                <div class="card bg-base-200 shadow-sm transition-all duration-300">
                    <div class="card-body p-5">
                        <h4 class="card-title text-xs uppercase opacity-70 tracking-wider">Dane Identyfikacyjne</h4>
                        <div class="space-y-4 py-2">
                            <div>
                                <label class="text-xs font-bold block text-primary uppercase mb-1">Pełna nazwa:</label>
                                <div class="text-sm font-medium leading-relaxed">{pelna_nazwa}</div>
                            </div>
                            <div>
                                <label class="text-xs font-bold block text-primary uppercase mb-1">NIP:</label>
                                <div class="text-sm font-medium">{nip}</div>
                            </div>
                            <div>
                                <label class="text-xs font-bold block text-primary uppercase mb-1">Miejscowość:</label>
                                <div class="text-sm font-medium">{miejscowosc}</div>
                            </div>
                            <div>
                                <label class="text-xs font-bold block text-primary uppercase mb-1">Adres:</label>
                                <div class="text-sm font-medium">{adres}</div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Okres i Wytworzenie Card -->
                <div class="card bg-base-200 shadow-sm transition-all duration-300">
                    <div class="card-body p-5">
                        <h4 class="card-title text-xs uppercase opacity-70 tracking-wider">Okres Sprawozdawczy</h4>
                        <div class="space-y-4 py-2">
                            <div>
                                <label class="text-xs font-bold block text-secondary uppercase mb-1">Data sprawozdania:</label>
                                <div class="text-sm font-medium">{data_spr}</div>
                            </div>
                            <div>
                                <label class="text-xs font-bold block text-secondary uppercase mb-1">Data wytworzenia:</label>
                                <div class="text-sm font-medium">{data_wytw}</div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Sumy Kontrolne Card -->
                <div class="card bg-base-200 shadow-sm md:col-span-2 transition-all duration-300">
                    <div class="card-body p-5">
                        <h4 class="card-title text-xs uppercase opacity-70 tracking-wider">Sumy Kontrolne (Sekcja Ctrl)</h4>
                        <div class="grid grid-cols-1 sm:grid-cols-3 gap-4 pt-2">
                            <div class="bg-base-100 p-3 rounded-lg border border-base-300 shadow-inner">
                                <label class="text-[10px] font-bold uppercase block opacity-60 mb-1">Liczba zapisów (Dziennik):</label>
                                <div class="text-xl font-black text-accent">{c1}</div>
                            </div>
                            <div class="bg-base-100 p-3 rounded-lg border border-base-300 shadow-inner">
                                <label class="text-[10px] font-bold uppercase block opacity-60 mb-1">Suma operacji (D_11):</label>
                                <div class="text-xl font-black text-accent whitespace-nowrap">{c2}</div>
                            </div>
                            <div class="bg-base-100 p-3 rounded-lg border border-base-300 shadow-inner">
                                <label class="text-[10px] font-bold uppercase block opacity-60 mb-1">Liczba zapisów (KontoZapis):</label>
                                <div class="text-xl font-black text-accent">{c3}</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        """
    except RuntimeError:
        return "<div class='alert alert-warning'>Nie otwarto żadnej bazy danych.</div>"
    except Exception as e:
        return f"<div class='alert alert-error'>Błąd: {e}</div>"

import json

def calculate_mus(populacja_rows, interval, start_point):
    wybrane_id = set()
    
    # Próbkowanie dla Wn (Z_4)
    suma_narastajaca = 0.0
    aktualny_prog = float(start_point)
    
    for row in populacja_rows:
        try:
            kwota = float(row['Z_4']) if row['Z_4'] is not None else 0.0
        except (ValueError, TypeError):
            kwota = 0.0
            
        if kwota > 0:
            suma_narastajaca += kwota
            while suma_narastajaca >= aktualny_prog:
                wybrane_id.add(row['Id'])
                aktualny_prog += interval
                
    # Próbkowanie dla Ma (Z_7)
    suma_narastajaca = 0.0
    # Reset progu startowego tak samo jak przy Wn
    aktualny_prog = float(start_point) 
    
    for row in populacja_rows:
        try:
            kwota = float(row['Z_7']) if row['Z_7'] is not None else 0.0
        except (ValueError, TypeError):
            kwota = 0.0
            
        if kwota > 0:
            suma_narastajaca += kwota
            while suma_narastajaca >= aktualny_prog:
                wybrane_id.add(row['Id'])
                aktualny_prog += interval
                
    return wybrane_id

@app.get("/api/mus/prepare", response_class=HTMLResponse)
async def mus_prepare(q: str = "", type: str = "", zq: str = "", month: str = ""):
    try:
        conn = db_service.get_connection()
        db_path = str(db_service.current_db_path) if db_service.current_db_path else ""
        z_service = ZapisyService(db_path)
        where_sql, params = z_service.build_zapisy_where(q, type, zq, month)

        sum_query = f"""
            SELECT SUM(z.Z_4) as sum_wn, SUM(z.Z_7) as sum_ma
            FROM Zapisy z
            JOIN ZOiS s ON z.Z_3 = s.S_1
            {where_sql}
        """
        sum_row = conn.execute(sum_query, params).fetchone()
        suma_wn = float(sum_row['sum_wn'] or 0)
        suma_ma = float(sum_row['sum_ma'] or 0)

        param_row = conn.execute("SELECT Kwota FROM ParametryBadania WHERE Klucz = 'Istotnosc_Wykonawcza'").fetchone()
        istotnosc = float(param_row['Kwota'] or 0) if param_row else 0.0

        return HTMLResponse(content=json.dumps({
            "istotnosc": istotnosc,
            "suma_wn": suma_wn,
            "suma_ma": suma_ma
        }), media_type="application/json")
    except Exception as e:
        return HTMLResponse(content=f'{{"error": "{str(e)}"}}', media_type="application/json", status_code=500)

@app.post("/api/mus/execute", response_class=HTMLResponse)
async def mus_execute(
    q: str = Form(""), 
    type: str = Form(""), 
    zq: str = Form(""),
    month: str = Form(""),
    istotnosc: float = Form(...),
    wspolczynnik: float = Form(...),
    punkt_startowy: float = Form(...)
):
    try:
        if istotnosc <= 0 or wspolczynnik <= 0:
            return HTMLResponse(content="<div class='alert alert-error'>Nieprawidłowe parametry MUS (wartości mniejsze bądź równe zeru!).</div>")
            
        interval = float(istotnosc) / float(wspolczynnik)
        
        conn = db_service.get_connection()
        db_path = str(db_service.current_db_path) if db_service.current_db_path else ""
        z_service = ZapisyService(db_path)
        where_sql, params = z_service.build_zapisy_where(q, type, zq, month)

        # Pobieramy rzędy tylko do zbudowania próby MUS iterując
        pop_query = f"""
            SELECT z.Id, z.Z_4, z.Z_7
            FROM Zapisy z
            JOIN ZOiS s ON z.Z_3 = s.S_1
            {where_sql}
            ORDER BY z.Z_Data, z.Id
        """
        populacja_rows = conn.execute(pop_query, params).fetchall()
        
        # Wyliczenie
        wybrane_id = calculate_mus(populacja_rows, interval, punkt_startowy)
        
        if not wybrane_id:
            return HTMLResponse(content="<div class='p-8 text-center bg-base-200 opacity-60 rounded-box border border-dashed border-base-content/20'>Brak odnalezionych zapisów w ramach MUS z wyznaczonymi parametrami.</div>")
            
        # Zbudowanie wynikowej tabeli - ograniczamy pobierane dane tylko do zaznaczonych "ID"
        id_list = ",".join([str(i) for i in wybrane_id])
        query = f"""
            SELECT z.Dziennik_Id, z.Z_3, z.Z_2, z.Z_Data, d.D_1, z.Z_4, z.Z_5, z.Z_6, z.Z_7, z.Z_8, z.Z_9, s.S_2
            FROM Zapisy z
            JOIN Dziennik d ON z.Dziennik_Id = d.Id
            JOIN ZOiS s ON z.Z_3 = s.S_1
            WHERE z.Id IN ({id_list})
            ORDER BY z.Z_Data, z.Id
        """
        rows = conn.execute(query).fetchall()
        
        html_rows = ""
        for row in rows:
            dziennik_id = row['Dziennik_Id']
            konto_full = sanitize_text(row['Z_3'])
            opis_full = sanitize_text(row['Z_2'])
            data_val = row['Z_Data'] or "---"
            nazwa_full = sanitize_text(row['S_2'])
            
            tip_content = f"{konto_full}\\n{nazwa_full or '---'}"
            konto_html = f'<div class="tooltip tooltip-right text-left" data-tip="{tip_content}"><div class="max-w-[150px] truncate">{konto_full}</div></div>'
            opis_html = f'<div class="tooltip tooltip-right text-left" data-tip="{opis_full}"><div class="max-w-xs truncate">{opis_full}</div></div>'

            wn_waluta_val = format_amount(row['Z_5'])
            wn_waluta_code = row['Z_6'] or ""
            wn_waluta_str = f"{wn_waluta_val} {wn_waluta_code}".strip()

            ma_waluta_val = format_amount(row['Z_8'])
            ma_waluta_code = row['Z_9'] or ""
            ma_waluta_str = f"{ma_waluta_val} {ma_waluta_code}".strip()

            html_rows += f"""
            <tr class="hover whitespace-nowrap text-xs">
                <td data-type="text">{konto_html}</td>
                <td data-type="text">{opis_html}</td>
                <td data-type="date" data-value="{row['Z_Data'] or ''}">{data_val}</td>
                <td data-type="text" class="text-primary font-bold cursor-pointer hover:underline" onclick="openDziennikModal({dziennik_id})">{row['D_1']}</td>
                <td data-type="number" data-value="{row['Z_4'] or 0}" class="text-right font-mono">{format_amount(row['Z_4'])}</td>
                <td data-type="text" class="text-right font-mono text-base-content/50 opacity-0 group-hover:opacity-100">{wn_waluta_str}</td>
                <td data-type="number" data-value="{row['Z_7'] or 0}" class="text-right font-mono">{format_amount(row['Z_7'])}</td>
                <td data-type="text" class="text-right font-mono text-base-content/50 opacity-0 group-hover:opacity-100">{ma_waluta_str}</td>
            </tr>
            """
            
        return f"""
        <div class="mb-4 text-sm bg-success/10 border border-success/30 px-4 py-2 rounded font-bold text-success flex items-center gap-2 shadow-sm">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            Wyłoniono i ograniczono widok do {len(wybrane_id)} zapisów w wyniku próbkowania MUS w oparciu o wyliczony interwał wynoszący {interval:,.2f} PLN.
        </div>
        <div class="overflow-x-auto border border-base-content/10 rounded-box shadow">
            <table class="table table-xs w-full">
                <thead class="bg-base-200">
                    <tr>
                        <th class="w-24">Konto</th>
                        <th class="w-1/3">Opis wg Dowodu</th>
                        <th>Data</th>
                        <th>Dowód (Nr dziennika)</th>
                        <th class="text-right">Wn PLN</th>
                        <th class="text-right border-x border-base-300">Wn Wal.</th>
                        <th class="text-right">Ma PLN</th>
                        <th class="text-right border-l border-base-300">Ma Wal.</th>
                    </tr>
                </thead>
                <tbody class="group">
                    {html_rows}
                </tbody>
            </table>
        </div>
        """
    except Exception as e:
        return HTMLResponse(content=f"<div class='alert alert-error font-mono text-xs p-4'>Wystąpił twardy błąd generatora:<br><br>{str(e)}</div>")

@app.get("/api/jpk/check-consistency", response_class=HTMLResponse)
async def get_jpk_check_consistency():
    try:
        results = db_service.get_import_consistency_check()
        is_kr_pd = results["is_kr_pd"]
        checks = results["checks"]
        
        rows_html = ""
        for check in checks:
            label = check["label"]
            ctrl_val = check["ctrl"]
            actual_val = check["actual"]
            
            # Tolerance 0.01 for decimals
            is_match = False
            if check["type"] == "int":
                is_match = int(ctrl_val or 0) == int(actual_val or 0)
            else:
                is_match = abs(float(ctrl_val or 0) - float(actual_val or 0)) <= 0.01
            
            status_badge = '<div class="badge badge-success gap-2 p-3 font-bold">Zgodny</div>' if is_match else '<div class="badge badge-error gap-2 p-3 font-bold uppercase">Błąd</div>'
            row_class = "" if is_match else "bg-error/10 hover:bg-error/20"
            
            # Format display values
            if check["type"] == "decimal":
                display_ctrl = format_amount(ctrl_val)
                display_actual = format_amount(actual_val)
            else:
                display_ctrl = str(ctrl_val or 0)
                display_actual = str(actual_val or 0)
                
            rows_html += f"""
            <tr class="{row_class} transition-colors duration-200">
                <td class="font-bold text-sm">{label}</td>
                <td class="font-mono text-right">{display_ctrl}</td>
                <td class="font-mono text-right">{display_actual}</td>
                <td class="text-center">{status_badge}</td>
            </tr>
            """
            
        integrity = results.get("integrity", [])
        integrity_html = ""
        for item in integrity:
            count = item["count"]
            label = item["label"]
            tip = item["tip"]
            
            if count == 0:
                status_html = '<span class="text-success font-bold flex items-center gap-1"><svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd" /></svg> OK</span>'
                row_class = ""
            else:
                status_html = f'<span class="text-error font-black flex items-center gap-1 animate-pulse"><svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd" /></svg> WYKRYTO BŁĘDY ({count})</span>'
                row_class = "bg-error/5"

            integrity_html += f"""
            <tr class="{row_class}">
                <td class="py-3">
                    <div class="tooltip tooltip-right text-left" data-tip="{tip}">
                        <span class="text-xs font-semibold cursor-help border-b border-dotted border-base-content/20">{label}</span>
                    </div>
                </td>
                <td colspan="2"></td>
                <td class="text-center">{status_html}</td>
            </tr>
            """

        jpk_type_str = "JPK_KR_PD" if is_kr_pd else "JPK_KR"
            
        return f"""
        <div class="fixed inset-0 z-[100] flex items-center justify-center bg-black/50 backdrop-blur-sm animate-in fade-in duration-300">
            <div class="p-8 bg-base-100 rounded-3xl shadow-2xl border border-base-content/10 max-w-3xl w-full mx-4 animate-in zoom-in duration-300">
                <div class="flex items-center justify-between mb-8">
                    <div>
                        <h2 class="text-3xl font-black text-primary flex items-center gap-3">
                            <svg xmlns="http://www.w3.org/2000/svg" class="h-10 w-10" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                            </svg>
                            Wynik Importu JPK
                        </h2>
                        <p class="text-[10px] opacity-60 font-black uppercase tracking-[0.2em] mt-2">Weryfikacja sum kontrolnych: <span class="badge badge-outline badge-sm font-bold ml-1">{jpk_type_str}</span></p>
                    </div>
                    <button onclick="this.closest('.fixed').remove()" class="btn btn-circle btn-ghost">✕</button>
                </div>

                <div class="overflow-hidden rounded-2xl border border-base-content/10 shadow-sm bg-base-200/30">
                    <table class="table w-full">
                        <thead class="bg-base-300/50 text-[10px] uppercase font-bold text-base-content/50">
                            <tr>
                                <th class="py-4">Kryterium Spójności</th>
                                <th class="text-right">Wartość w JPK (Ctrl)</th>
                                <th class="text-right">Wartość w bazie</th>
                                <th class="text-center">Status</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-base-content/5">
                            {rows_html}
                            <tr class="bg-base-300/30">
                                <td colspan="4" class="py-2 px-4 text-[10px] font-black uppercase opacity-40">Integralność techniczna bazy danych</td>
                            </tr>
                            {integrity_html}
                        </tbody>
                    </table>
                </div>
                
                <div class="mt-8 flex justify-end gap-3">
                    <button onclick="this.closest('.fixed').remove()" class="btn btn-primary px-10 rounded-xl font-bold">Zamknij</button>
                </div>
            </div>
        </div>
        """
    except Exception as e:
        return f"""
        <div class="fixed bottom-6 right-6 z-[200] max-w-md animate-in slide-in-from-right duration-500">
            <div class="alert alert-error shadow-2xl border-2 border-white/10 flex items-start gap-4 p-6">
                <svg xmlns="http://www.w3.org/2000/svg" class="stroke-current shrink-0 h-8 w-8" fill="none" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                <div class="flex-grow">
                    <h3 class="font-black text-sm uppercase tracking-wider">Błąd weryfikacji spójności</h3>
                    <p class="text-xs opacity-80 mt-1 font-medium leading-relaxed">{str(e)}</p>
                    <div class="mt-4 flex justify-end">
                        <button onclick="this.closest('.fixed').remove()" class="btn btn-ghost btn-xs font-bold uppercase">Rozumiem</button>
                    </div>
                </div>
            </div>
        </div>
        """


@app.get("/api/config/system")
async def get_system_config():
    return {
        "heartbeat_interval": config_manager.config.heartbeat_interval,
        "server_timeout": config_manager.config.server_timeout
    }

@app.post("/api/config/system")
async def update_system_config(heartbeat_interval: int = Form(...), server_timeout: int = Form(...)):
    # Walidacja również po stronie backendu
    if server_timeout < 3 * heartbeat_interval:
        raise HTTPException(status_code=400, detail="Timeout musi być co najmniej 3 razy większy niż interwał.")
    
    config_manager.update_system_config(heartbeat_interval, server_timeout)
    return HTMLResponse(content="""
        <div class="alert alert-success shadow-lg">
            <svg xmlns="http://www.w3.org/2000/svg" class="stroke-current flex-shrink-0 h-6 w-6" fill="none" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
            <span>Ustawienia systemowe zapisane pomyślnie!</span>
        </div>
        <script>
            // Refresh heartbeat on frontend immediately
            if (window.initHeartbeat) window.initHeartbeat();
        </script>
    """)

if __name__ == "__main__":
    import uvicorn
    # Allow running directly for debug
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
