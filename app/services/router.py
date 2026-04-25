import logging
import sqlite3
import os
import time
import html
import json
from pathlib import Path
from typing import List, Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from app.services.database import db_service
from app.services.etl import etl_service
from app.services.legacy_etl import legacy_etl_service
from app.services.agent_chat import AgentChat
from app.services.zapisy_service import ZapisyService
from app.core.config import config_manager

logger = logging.getLogger(__name__)

class JPKRouter:
    """
    Orchestrator responsible for detecting the version of JPK_KR files 
    and routing the import process to the appropriate ETL service.
    """

    # Namespace constants
    NS_LEGACY = "http://jpk.mf.gov.pl/wzor/2016/03/09/03091/"
    NS_MODERN = "http://jpk.mf.gov.pl/wzor/2024/09/04/09041/"

    def __init__(self):
        # The services are imported from their respective modules where they are already instantiated
        self.modern_service = etl_service
        self.legacy_service = legacy_etl_service

    def _detect_version(self, file_path: str) -> str:
        """
        Detects JPK_KR version by reading the beginning of the XML file and looking for namespace URIs.
        Reads only the first 4096 bytes to ensure low memory footprint.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Plik nie istnieje: {file_path}")

        try:
            with open(file_path, "rb") as f:
                # Read initial segment of the file to find the namespace declaration
                chunk = f.read(4096).decode("utf-8", errors="ignore")
                
                if self.NS_MODERN in chunk:
                    return "MODERN"
                elif self.NS_LEGACY in chunk:
                    return "LEGACY"
                else:
                    logger.error(f"Unknown namespace in file: {file_path}")
                    raise ValueError("Nie rozpoznano formatu pliku JPK_KR (nieobsługiwana przestrzeń nazw).")
        except Exception as e:
            logger.error(f"Error during version detection: {e}")
            raise

    def route_import(self, file_path: str) -> str:
        """
        Detects the file version and routes it to either ETLService (Modern) or LegacyETLService (Legacy).
        Returns the path to the resulting SQLite database.
        """
        logger.info(f"Starting routing for file: {file_path}")
        
        try:
            version = self._detect_version(file_path)
            
            if version == "MODERN":
                logger.info("Routing to ETLService based on modern namespace detection (JPK_KR_PD).")
                # Calls the modern ETL service (from etl.py)
                return self.modern_service.import_jpk(file_path)
            
            elif version == "LEGACY":
                logger.info("Routing to LegacyETLService based on legacy namespace detection (JPK_KR).")
                # Calls the legacy ETL service (from legacy_etl.py)
                return self.legacy_service.import_jpk(file_path)
            
            else:
                raise ValueError(f"Unsupported JPK version: {version}")

        except Exception as e:
            logger.error(f"Import process failed in JPKRouter: {e}")
            raise

# Instance for easy import
jpk_router = JPKRouter()

# --- AI AGENT SECTION ---

ai_router = APIRouter()
api_router = APIRouter()

class ChatRequest(BaseModel):
    message: str
    model: str = "gemini-2.5-flash-lite"
    session_id: str = "default"

@ai_router.post("/api/chat")
async def chat_with_ai(request_data: ChatRequest, request: Request):
    """
    Endpoint obsługujący interakcję z agentem AI.
    Inicjalizuje AgentChat na aktualnie otwartej bazie i przesyła pytanie.
    Potrzymuje heartbeat serwera.
    """
    # Reset licznika bezczynności (heartbeat)
    request.app.state.last_heartbeat = time.time()
    
    db_path = config_manager.get_last_db()
    if not db_path:
        return {"response": "System nie wykrył aktywnej bazy danych. Proszę zaimportować plik JPK lub wybrać bazę z listy ostatnich plików."}
    
    try:
        current_session = request_data.session_id
        # Get session dictionary if not present
        if not hasattr(request.app.state, "ai_sessions"):
            request.app.state.ai_sessions = {}
            
        existing_agent = request.app.state.ai_sessions.get(current_session)
        
        # Reset condition: No agent OR model changed
        if not existing_agent or existing_agent.model_name != request_data.model:
            logger.info(f"Inicjalizacja nowego agenta dla sesji {current_session} (Model: {request_data.model})")
            new_agent = AgentChat(db_path, model_name=request_data.model)
            request.app.state.ai_sessions[current_session] = new_agent
            agent = new_agent
        else:
            agent = existing_agent
            
        answer = agent.ask(request_data.message)
        return {"response": answer}
    except Exception as e:
        logger.error(f"Błąd w /api/chat: {e}")
        # Nie rzucamy 500, aby frontend mógł wyświetlić czytelny komunikat o błędzie
        return {"response": f"Wystąpił błąd podczas przetwarzania pytania: {str(e)}"}


@api_router.post("/api/zois/group-ai")
async def group_zois_accounts():
    """Tworzy grupy syntetyczne kont 3-znakowych, agreguje salda, i opcjonalnie nazywa z AI."""
    try:
        # A) Podstawowa logika grupowania SQL
        db_service.group_zois_accounts()
        
        from fastapi import Response
        # B) Wywołanie agenta AI
        try:
            groups_data = db_service.get_unnamed_groups()
            if groups_data:
                from app.services.agent_group_zois import ZoisNamingAgent, AIConnectionError
                agent = ZoisNamingAgent()
                new_names = agent.generate_names(groups_data)
                db_service.update_group_names(new_names)
            return Response(status_code=204, headers={"HX-Trigger": "db-changed"})
            
        # C) Obsługa błędów AI
        except Exception as ai_e:
            logger.warning(f"ZoisNamingAgent failed (AI fallback invoked): {ai_e}")
            headers = {
                "HX-Trigger": json.dumps({
                    "db-changed": "", 
                    "show-toast-warning": "Pogrupowano bez AI: Brak klucza lub błąd połączenia"
                })
            }
            return Response(status_code=204, headers=headers)

    except Exception as e:
        logger.error(f"Error grouping zois accounts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- ZAPISY ANALYTICS SECTION ---

def format_amount(value):
    if value is None:
        return ""
    try:
        val_float = float(value)
        if val_float == 0:
            return ""
        # Format: thousands space, decimal comma
        s = f"{val_float:,.2f}".replace(",", " ").replace(".", ",")
        return s.replace(" ", "\u00A0") # non-breaking space
    except:
        return str(value)

@api_router.get("/zapisy/powiazania", response_class=HTMLResponse)
async def get_zapisy_powiazania(
    q: str = "", type: str = "", zq: str = "", month: str = "", 
    konto: str = "", opis: str = "", dziennik_id: str = "", min_kwota: str = ""
):
    try:
        db_path = str(db_service.current_db_path) if db_service.current_db_path else ""
        if not db_path:
            return HTMLResponse(content="<div class='alert alert-warning'>Brak aktywnej bazy danych.</div>")
            
        z_service = ZapisyService(db_path)
        # Przekazujemy wszystkie parametry do budowania warunku WHERE
        where_sql, params = z_service.build_zapisy_where(
            q=q, type=type, zq=zq, month=month,
            konto=konto, 
            opis=opis, 
            dziennik_id=dziennik_id, 
            min_kwota=min_kwota
        )

        conn = db_service.get_connection()
        query = f"""
            WITH WybraneDzienniki AS (
                SELECT DISTINCT z.Dziennik_Id 
                FROM Zapisy z
                LEFT JOIN Dziennik d ON z.Dziennik_Id = d.Id
                LEFT JOIN ZOiS s ON z.Z_3 = s.S_1
                {where_sql}
            )
            SELECT 
                Z_GrupaKont as Syntetyka,
                SUM(CASE WHEN Z_4 > 0 THEN 1 ELSE 0 END) as Liczba_Wn,
                SUM(CASE WHEN Z_7 > 0 THEN 1 ELSE 0 END) as Liczba_Ma
            FROM Zapisy
            WHERE Dziennik_Id IN (SELECT Dziennik_Id FROM WybraneDzienniki)
            GROUP BY Z_GrupaKont
            ORDER BY Syntetyka
        """
        rows = conn.execute(query, params).fetchall()

        html_rows = ""
        for row in rows:
            synt = row['Syntetyka']
            if not synt: continue
            wn = row['Liczba_Wn'] or 0
            ma = row['Liczba_Ma'] or 0
            wn_str = format_amount(wn).replace(',00', '')
            ma_str = format_amount(ma).replace(',00', '')
            
            html_rows += f'''
            <tr class="hover cursor-pointer" onclick="aplikujFiltrSyntetyki('{html.escape(synt)}')">
                <td class="font-mono text-primary font-bold">{html.escape(synt)}</td>
                <td class="text-right font-mono">{wn_str}</td>
                <td class="text-right font-mono">{ma_str}</td>
            </tr>
            '''
        
        if not html_rows:
            html_rows = "<tr><td colspan='3' class='text-center opacity-50 italic'>Brak danych z kont przeciwstawnych</td></tr>"

        return f"""
        <dialog id="przeglad_modal" class="modal">
            <div class="modal-box w-11/12 max-w-2xl relative shadow-2xl">
                <form method="dialog"><button class="btn btn-sm btn-circle btn-ghost absolute right-2 top-2">✕</button></form>
                <h3 class="font-bold text-lg mb-4 text-secondary flex items-center gap-2">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
                    </svg>
                    Przegląd powiązań (konta przeciwstawne)
                </h3>
                <div class="overflow-x-auto max-h-[60vh]">
                    <table class="table table-zebra table-sm w-full">
                        <thead class="bg-base-200 sticky top-0 z-10">
                            <tr><th>Konto (Syntetyka)</th><th class="text-right">Wystąpienia Wn</th><th class="text-right">Wystąpienia Ma</th></tr>
                        </thead>
                        <tbody>{html_rows}</tbody>
                    </table>
                </div>
            </div>
            <form method="dialog" class="modal-backdrop"><button>close</button></form>
        </dialog>
        """
    except Exception as e:
        logger.error(f"Error in zapisy/powiazania: {e}")
        return HTMLResponse(content=f"<div class='alert alert-error'>Błąd: {html.escape(str(e))}</div>")

@api_router.get("/zapisy/wykres", response_class=HTMLResponse)
async def get_zapisy_wykres(
    q: str = "", type: str = "", zq: str = "", month: str = "", 
    konto: str = "", opis: str = "", dziennik_id: str = "", min_kwota: str = ""
):
    try:
        db_path = str(db_service.current_db_path) if db_service.current_db_path else ""
        if not db_path:
            return HTMLResponse(content="<div class='p-10 text-center opacity-50'>Brak bazy.</div>")
            
        z_service = ZapisyService(db_path)
        where_sql, params = z_service.build_zapisy_where(
            q=q, type=type, zq=zq, month=month,
            konto=konto, 
            opis=opis, 
            dziennik_id=dziennik_id, 
            min_kwota=min_kwota
        )

        conn = db_service.get_connection()
        agg_sql = f"""
            SELECT COUNT(*) as total_count, SUM(z.Z_4) as sum_wn, SUM(z.Z_7) as sum_ma 
            FROM Zapisy z 
            LEFT JOIN Dziennik d ON z.Dziennik_Id = d.Id
            LEFT JOIN ZOiS s ON z.Z_3 = s.S_1 
            {where_sql}
        """
        agg = conn.execute(agg_sql, params).fetchone()
        
        sql_monthly = f"""
            SELECT Z_DataMiesiac, SUM(Z_4) as suma_wn, SUM(Z_7) as suma_ma 
            FROM Zapisy z 
            LEFT JOIN Dziennik d ON z.Dziennik_Id = d.Id
            LEFT JOIN ZOiS s ON z.Z_3 = s.S_1 
            {where_sql} AND Z_DataMiesiac IS NOT NULL 
            GROUP BY Z_DataMiesiac 
            ORDER BY Z_DataMiesiac
        """
        rows = conn.execute(sql_monthly, params).fetchall()

        month_names = ["Styczeń", "Luty", "Marzec", "Kwiecień", "Maj", "Czerwiec", "Lipiec", "Sierpień", "Wrzesień", "Październik", "Listopad", "Grudzień"]
        wn_data = [0.0] * 12
        ma_data = [0.0] * 12

        for row in rows:
            m = row['Z_DataMiesiac']
            if m and 1 <= m <= 12:
                wn_data[m-1] = float(row['suma_wn'] or 0)
                ma_data[m-1] = float(row['suma_ma'] or 0)

        return f"""
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            <div class="stats shadow bg-base-200 border border-base-content/5"><div class="stat"><div class="stat-title text-[10px] uppercase font-bold opacity-60">Liczba zapisów</div><div class="stat-value text-xl text-primary">{agg['total_count']}</div></div></div>
            <div class="stats shadow bg-base-200 border border-base-content/5"><div class="stat"><div class="stat-title text-[10px] uppercase font-bold opacity-60">Suma Wn</div><div class="stat-value text-xl text-secondary font-mono">{format_amount(agg['sum_wn'])}</div></div></div>
            <div class="stats shadow bg-base-200 border border-base-content/5"><div class="stat"><div class="stat-title text-[10px] uppercase font-bold opacity-60">Suma Ma</div><div class="stat-value text-xl text-accent font-mono">{format_amount(agg['sum_ma'])}</div></div></div>
            <div class="stats shadow bg-base-200 border border-base-content/5"><div class="stat"><div class="stat-title text-[10px] uppercase font-bold opacity-60">Balans</div><div class="stat-value text-xl font-mono">{(format_amount((agg['sum_wn'] or 0) - (agg['sum_ma'] or 0)))}</div></div></div>
        </div>
        <div id='plotly-chart' class='w-full h-[500px] border border-base-content/10 rounded-xl shadow-lg bg-base-100'></div>
        <script>
            (function() {{
                const data = [
                    {{ x: {json.dumps(month_names)}, y: {json.dumps(wn_data)}, name: 'Wn', type: 'scatter', mode: 'lines+markers', line: {{ color: '#d926aa', width: 4 }} }},
                    {{ x: {json.dumps(month_names)}, y: {json.dumps(ma_data)}, name: 'Ma', type: 'scatter', mode: 'lines+markers', line: {{ color: '#1fb2a6', width: 4 }} }}
                ];
                const layout = {{
                    paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
                    font: {{ color: '#A6ADBB', family: 'Inter, sans-serif' }},
                    margin: {{ t: 40, r: 40, b: 40, l: 80 }},
                    xaxis: {{ gridcolor: 'rgba(255,255,255,0.05)', zeroline: false }},
                    yaxis: {{ gridcolor: 'rgba(255,255,255,0.05)', zeroline: false }},
                    legend: {{ orientation: 'h', y: -0.2, x: 0.5, xanchor: 'center' }}
                }};
                Plotly.newPlot('plotly-chart', data, layout, {{ responsive: true }});
            }})();
        </script>
        """
    except Exception as e:
        logger.error(f"Error in zapisy/wykres: {e}")
        return HTMLResponse(content=f"<div class='p-10 text-error'>Błąd: {str(e)}</div>")

