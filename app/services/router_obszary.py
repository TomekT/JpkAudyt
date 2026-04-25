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
        return """
        <div id="toast-success" class="toast toast-end z-[9999]">
            <div class="alert alert-success shadow-lg"><span>Zapisano mapowanie.</span></div>
        </div>
        <script>
            setTimeout(() => {
                const toast = document.getElementById('toast-success');
                if(toast) toast.remove();
            }, 3000);
            htmx.trigger('body', 'db-changed');
        </script>
        """
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
        rows = conn.execute("""
            SELECT S_1, S_2, S_10, S_11, Strona_Salda, Source_Konto
            FROM v_zois_resolved_mapping
            WHERE Obszar_Id = ?
            ORDER BY S_1
        """, (obszar_id,)).fetchall()
        
        html_rows = ""
        for row in rows:
            html_rows += f"""
            <tr class="hover text-[10px]">
                <td class="font-mono">{html.escape(row['S_1'])}</td>
                <td class="truncate max-w-[150px]" title="{html.escape(row['S_2'])}">{html.escape(row['S_2'])}</td>
                <td class="text-right font-mono">{row['S_10']:,.2f}</td>
                <td class="text-right font-mono">{row['S_11']:,.2f}</td>
                <td class="text-center"><div class="badge badge-ghost badge-xs">{html.escape(row['Strona_Salda'])}</div></td>
                <td class="text-xs opacity-60 italic">z: {html.escape(row['Source_Konto'])}</td>
            </tr>
            """
            
        if not html_rows:
            html_rows = "<tr><td colspan='6' class='text-center italic opacity-50'>Brak przypisanych kont ZOiS</td></tr>"
            
        return f"""
        <div class="overflow-x-auto">
            <table class="table table-xs w-full">
                <thead>
                    <tr class="bg-base-200">
                        <th>Konto</th>
                        <th>Nazwa</th>
                        <th class="text-right">Wn</th>
                        <th class="text-right">Ma</th>
                        <th class="text-center">Tryb</th>
                        <th>Źródło</th>
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

@router.delete("/api/zois/unmap/{konto_id}", response_class=HTMLResponse)
async def unmap_obszar(konto_id: str):
    try:
        conn = db_service.get_connection()
        conn.execute("DELETE FROM ZOiS_Mapowanie_Obszar WHERE ZOiS_S1 = ?", (konto_id,))
        conn.commit()
        return """
        <div id="toast-unmap" class="toast toast-end z-[9999]">
            <div class="alert alert-info shadow-lg"><span>Usunięto mapowanie.</span></div>
        </div>
        <script>
            setTimeout(() => {
                const toast = document.getElementById('toast-unmap');
                if(toast) toast.remove();
            }, 3000);
            htmx.trigger('body', 'db-changed');
        </script>
        """
    except Exception as e:
        return f"<div class='alert alert-error'>Błąd: {e}</div>"

