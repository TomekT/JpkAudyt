from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from app.services.database import db_service
import html

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

@router.get("/obszary", response_class=HTMLResponse)
async def get_obszary(request: Request):
    try:
        conn = db_service.get_connection()
        rows = conn.execute("SELECT * FROM v_obszary_rekoncyliacja ORDER BY Id").fetchall()
        obszary_list = [dict(r) for r in rows]
        
        return templates.TemplateResponse("obszary_tab.html", {
            "request": request,
            "obszary": obszary_list
        })
    except Exception as e:
        return f"<div class='alert alert-error'>Błąd: {e}</div>"

@router.post("/obszary", response_class=HTMLResponse)
async def create_obszar(request: Request, nazwa: str = Form(...), typ: str = Form("")):
    try:
        conn = db_service.get_connection()
        conn.execute("INSERT INTO Obszary (Nazwa, Typ, Czy_Systemowy) VALUES (?, ?, 0)", (nazwa, typ))
        conn.commit()
        return await get_obszary(request)
    except Exception as e:
        return f"<div class='alert alert-error'>Błąd: {e}</div>"

@router.get("/obszary/{obszar_id}/szczegoly", response_class=HTMLResponse)
async def get_obszary_szczegoly(obszar_id: int):
    try:
        conn = db_service.get_connection()
        
        obszar = conn.execute("SELECT * FROM Obszary WHERE Id = ?", (obszar_id,)).fetchone()
        if not obszar:
            return "<div class='alert alert-error'>Nie znaleziono obszaru.</div>"
            
        rows = conn.execute("""
            SELECT sp.XmlTag, sp.Nazwa, sp.Kwota_RB, sp.Kwota_RP 
            FROM Sprawozdanie_Pozycje sp
            JOIN Obszary_Sprawozdanie os ON sp.XmlTag = os.XmlTag
            WHERE os.Obszar_Id = ?
        """, (obszar_id,)).fetchall()
        
        html_rows = ""
        for row in rows:
            html_rows += f"""
            <tr class="hover text-[10px]">
                <td title="{html.escape(row['XmlTag'])}"><div class="max-w-[80px] truncate">{html.escape(row['XmlTag'])}</div></td>
                <td title="{html.escape(row['Nazwa'])}"><div class="max-w-[120px] truncate">{html.escape(row['Nazwa'])}</div></td>
                <td class="text-right font-mono">{row['Kwota_RB']:,.2f}</td>
            </tr>
            """
            
        if not html_rows:
            html_rows = "<tr><td colspan='3' class='text-center italic opacity-50'>Brak przypisanych pozycji</td></tr>"
            
        return f"""
        <div class="p-2 border border-base-300 rounded-lg bg-base-100 shadow-inner mt-2">
            <table class="table table-xs w-full">
                <thead>
                    <tr class="bg-base-200/50">
                        <th class="text-xs">Tag</th>
                        <th class="text-xs">Nazwa</th>
                        <th class="text-right text-xs">Kwota RB</th>
                    </tr>
                </thead>
                <tbody>
                    {html_rows}
                </tbody>
            </table>
        </div>
        """
    except Exception as e:
        return f"<div class='alert alert-error'>Błąd: {e}</div>"

@router.get("/api/zois/get-map-modal", response_class=HTMLResponse)
async def get_zois_map_modal(konto_id: str):
    try:
        conn = db_service.get_connection()
        # Get account details
        konto = conn.execute("SELECT S_1, S_2 FROM ZOiS WHERE S_1 = ?", (konto_id,)).fetchone()
        if not konto:
            return "<div class='p-4'>Błąd: Nie znaleziono konta.</div>"
            
        # Get CURRENT DIRECT mapping
        mapping = conn.execute("""
            SELECT Obszar_Id, Strona_Salda 
            FROM ZOiS_Mapowanie_Obszar 
            WHERE ZOiS_S1 = ?
        """, (konto_id,)).fetchone()
        
        # Get inherited mapping info (if no direct mapping)
        inherited = None
        if not mapping:
            inherited = conn.execute("""
                SELECT Obszar_Id, Strona_Salda, Source_Konto
                FROM v_zois_resolved_mapping
                WHERE S_1 = ?
            """, (konto_id,)).fetchone()

        # Get all areas for dropdown
        obszary = conn.execute("SELECT Id, Nazwa FROM Obszary ORDER BY Nazwa").fetchall()
        
        # Pre-selected values
        current_obszar_id = mapping['Obszar_Id'] if mapping else (inherited['Obszar_Id'] if inherited else None)
        current_strona = mapping['Strona_Salda'] if mapping else (inherited['Strona_Salda'] if inherited else "PERSALDO_WN_MA")
        
        # Build options
        obszary_options = ""
        for o in obszary:
            selected = 'selected' if current_obszar_id == o['Id'] else ''
            obszary_options += f'<option value="{o["Id"]}" {selected}>{html.escape(o["Nazwa"])}</option>'
            
        # Build side options
        side_options = ""
        sides = [
            ("PERSALDO_WN_MA", "Persaldo (Wn - Ma)"),
            ("PERSALDO_MA_WN", "Persaldo (Ma - Wn)"),
            ("TYLKO_WN", "Tylko saldo Wn"),
            ("TYLKO_MA", "Tylko saldo Ma")
        ]
        for val, label in sides:
            selected = 'selected' if current_strona == val else ''
            side_options += f'<option value="{val}" {selected}>{label}</option>'

        # Unmap button (only for direct mapping)
        unmap_btn = ""
        if mapping:
            unmap_btn = f"""
            <button type="button" class="btn btn-error btn-sm btn-outline gap-2"
                    hx-delete="/api/zois/unmap/{konto_id}" hx-target="#toast-container" hx-swap="beforeend"
                    onclick="document.getElementById('map_obszar_modal').close()">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                </svg>
                Odmapuj
            </button>
            """

        info_badge = ""
        if inherited and not mapping:
            info_badge = f'<div class="badge badge-info badge-outline gap-2 mt-2 mb-4 text-[10px]">Dziedziczy z {inherited["Source_Konto"]}</div>'

        return f"""
        <div class="modal-box w-full max-w-sm">
            <form method="dialog">
                <button class="btn btn-sm btn-circle btn-ghost absolute right-2 top-2">✕</button>
            </form>
            <h3 class="font-bold text-lg">{html.escape(konto['S_1'])}</h3>
            <p class="text-sm opacity-70 mb-2">{html.escape(konto['S_2'])}</p>
            {info_badge}
            
            <form hx-post="/api/zois/map-obszar" hx-target="#toast-container" hx-swap="beforeend" onsubmit="document.getElementById('map_obszar_modal').close()">
                <input type="hidden" name="konto_id" value="{konto_id}">
                
                <div class="form-control w-full mb-2">
                    <label class="label"><span class="label-text text-xs uppercase font-bold opacity-70">Obszar</span></label>
                    <select name="obszar_id" class="select select-bordered select-sm" required>
                        <option value="" disabled {'selected' if not current_obszar_id else ''}>Wybierz obszar...</option>
                        {obszary_options}
                    </select>
                </div>
                
                <div class="form-control w-full mb-6">
                    <label class="label"><span class="label-text text-xs uppercase font-bold opacity-70">Strona Salda</span></label>
                    <select name="strona_salda" class="select select-bordered select-sm" required>
                        {side_options}
                    </select>
                </div>
                
                <div class="modal-action flex justify-between w-full">
                    {unmap_btn}
                    <div class="flex gap-2 {'ml-auto' if not unmap_btn else ''}">
                        <button type="button" class="btn btn-sm" onclick="document.getElementById('map_obszar_modal').close()">Anuluj</button>
                        <button type="submit" class="btn btn-primary btn-sm">Zapisz</button>
                    </div>
                </div>
            </form>
        </div>
        """
    except Exception as e:
        return f"<div class='p-4 text-error'>Błąd: {e}</div>"

@router.get("/api/obszary/options", response_class=HTMLResponse)
async def get_obszary_options():
    try:
        conn = db_service.get_connection()
        rows = conn.execute("SELECT Id, Nazwa FROM Obszary ORDER BY Nazwa").fetchall()
        html_options = '<option value="" disabled selected>Wybierz obszar...</option>'
        for row in rows:
            html_options += f'<option value="{row["Id"]}">{html.escape(row["Nazwa"])}</option>'
        return html_options
    except Exception as e:
        return f'<option value="" disabled>Błąd ładowania</option>'

@router.post("/api/zois/map-obszar", response_class=HTMLResponse)
async def map_obszar(konto_id: str = Form(...), obszar_id: int = Form(...), strona_salda: str = Form(...)):
    try:
        conn = db_service.get_connection()
        conn.execute("""
            INSERT INTO ZOiS_Mapowanie_Obszar (Obszar_Id, ZOiS_S1, Strona_Salda)
            VALUES (?, ?, ?)
            ON CONFLICT(ZOiS_S1, Strona_Salda) DO UPDATE SET Obszar_Id = excluded.Obszar_Id
        """, (obszar_id, konto_id, strona_salda))
        conn.commit()
        return HTMLResponse(
            content="""
            <div id="toast-success" class="toast toast-end z-[9999]">
                <div class="alert alert-success shadow-lg"><span>Zapisano mapowanie.</span></div>
            </div>
            <script>
                setTimeout(() => {
                    const toast = document.getElementById('toast-success');
                    if(toast) toast.remove();
                }, 3000);
            </script>
            """,
            headers={"HX-Trigger": "db-changed"}
        )
    except Exception as e:
        return f"""
        <div id="toast-error" class="toast toast-end z-[9999]">
            <div class="alert alert-error shadow-lg"><span>Błąd: {e}</span></div>
        </div>
        <script>
            setTimeout(() => {{
                const toast = document.getElementById('toast-error');
                if(toast) toast.remove();
            }}, 5000);
        </script>
        """

@router.get("/obszary/{obszar_id}/explain", response_class=HTMLResponse)
async def explain_obszar(obszar_id: int):
    try:
        conn = db_service.get_connection()
        # Pobieramy nazwę obszaru
        obszar_name = conn.execute("SELECT Nazwa FROM Obszary WHERE Id = ?", (obszar_id,)).fetchone()
        obszar_name = obszar_name['Nazwa'] if obszar_name else "Nieznany"

        # Agregacja do poziomu syntetyki (pierwsze 3 znaki)
        # Przy zachowaniu twardego filtra na liście
        rows = conn.execute("""
            SELECT 
                SUBSTR(rm.S_1, 1, 3) AS Konto_Syntetyczne,
                COALESCE((SELECT S_2 FROM ZOiS WHERE S_1 = SUBSTR(rm.S_1, 1, 3)), 'Konto Syntetyczne') AS Nazwa,
                COUNT(rm.S_1) AS Liczba_Analityk,
                SUM(rm.S_10) AS Suma_Wn,
                SUM(rm.S_11) AS Suma_Ma,
                SUM(
                    CASE 
                        WHEN rm.Strona_Salda = 'TYLKO_WN' THEN rm.S_10
                        WHEN rm.Strona_Salda = 'TYLKO_MA' THEN rm.S_11
                        WHEN rm.Strona_Salda = 'PERSALDO_WN_MA' THEN (rm.S_10 - rm.S_11)
                        WHEN rm.Strona_Salda = 'PERSALDO_MA_WN' THEN (rm.S_11 - rm.S_10)
                        ELSE 0 
                    END
                ) AS Wartosc
            FROM v_zois_resolved_mapping rm
            WHERE rm.Obszar_Id = ?
            AND rm.S_1 NOT IN (SELECT S_3 FROM ZOiS WHERE S_3 IS NOT NULL AND S_3 != '')
            GROUP BY SUBSTR(rm.S_1, 1, 3)
            ORDER BY Konto_Syntetyczne
        """, (obszar_id,)).fetchall()
        
        html_rows = ""
        grand_total = 0
        
        for row in rows:
            wn = row['Suma_Wn'] or 0
            ma = row['Suma_Ma'] or 0
            wartosc = row['Wartosc'] or 0
            count = row['Liczba_Analityk']
            
            grand_total += wartosc
            
            html_rows += f"""
            <tr class="hover text-[11px]">
                <td class="font-bold text-primary">{html.escape(row['Konto_Syntetyczne'])}</td>
                <td>
                    <div class="flex items-center gap-2">
                        <span class="truncate max-w-[200px]" title="{html.escape(row['Nazwa'])}">{html.escape(row['Nazwa'])}</span>
                        <span class="badge badge-ghost badge-sm text-[9px] opacity-70 whitespace-nowrap">{count} subkont</span>
                    </div>
                </td>
                <td class="text-right font-mono">{wn:,.2f}</td>
                <td class="text-right font-mono">{ma:,.2f}</td>
                <td class="text-right font-mono font-bold bg-base-200/50">{wartosc:,.2f}</td>
            </tr>
            """
            
        if not html_rows:
            html_rows = "<tr><td colspan='5' class='text-center italic opacity-50 p-4'>Brak przypisanych kont dla tego obszaru</td></tr>"
        else:
            # Wiersz podsumowania
            html_rows += f"""
            <tr class="bg-base-300 font-bold border-t-2 border-base-content/20">
                <td colspan="2" class="text-right uppercase text-[10px] py-3">Suma całkowita obszaru:</td>
                <td colspan="2"></td>
                <td class="text-right font-mono text-primary text-sm py-3">{grand_total:,.2f}</td>
            </tr>
            """
            
        return f"""
        <div class="mb-4 text-xs font-semibold opacity-80 flex justify-between items-center">
            <span>Zestawienie syntetyczne dla obszaru: <span class="text-primary">{html.escape(obszar_name)}</span></span>
            <span class="badge badge-outline badge-xs opacity-50">Tylko Liście</span>
        </div>
        <div class="overflow-x-auto">
            <table class="table table-sm table-zebra w-full border border-base-300">
                <thead>
                    <tr class="bg-base-200 text-xs">
                        <th class="w-20">Syntetyka</th>
                        <th>Nazwa konta</th>
                        <th class="text-right">Suma Wn</th>
                        <th class="text-right">Suma Ma</th>
                        <th class="text-right">Wartość Zmapowana</th>
                    </tr>
                </thead>
                <tbody>
                    {html_rows}
                </tbody>
            </table>
        </div>
        <div class="mt-4 alert alert-ghost border border-base-300 py-2 text-[10px] leading-tight opacity-70">
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" class="stroke-current shrink-0 w-4 h-4"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
            <span>Analityki zostały zagregowane do poziomu kont syntetycznych (3 znaki). Wyliczona wartość bazuje na indywidualnych regułach przypisanych do każdego subkonta.</span>
        </div>
        """
    except Exception as e:
        return f"<div class='alert alert-error text-xs'>Błąd agregacji: {e}</div>"

@router.delete("/api/zois/unmap/{konto_id}", response_class=HTMLResponse)
async def unmap_obszar(konto_id: str):
    try:
        conn = db_service.get_connection()
        conn.execute("DELETE FROM ZOiS_Mapowanie_Obszar WHERE ZOiS_S1 = ?", (konto_id,))
        conn.commit()
        return HTMLResponse(
            content="""
            <div id="toast-unmap" class="toast toast-end z-[9999]">
                <div class="alert alert-info shadow-lg"><span>Usunięto mapowanie.</span></div>
            </div>
            <script>
                setTimeout(() => {
                    const toast = document.getElementById('toast-unmap');
                    if(toast) toast.remove();
                }, 3000);
            </script>
            """,
            headers={"HX-Trigger": "db-changed"}
        )
    except Exception as e:
        return f"<div class='alert alert-error'>Błąd: {e}</div>"

