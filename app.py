from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import requests, re, io
from openpyxl import Workbook
from openpyxl.styles import Font

app = FastAPI()
templates = Jinja2Templates(directory="templates")

API_URL = "https://www.datos.gov.co/resource/p6dx-8zbt.json"

def soql_escape(v: str) -> str:
    return v.replace("'", "''")

def sanitize_like_term(v: str) -> str:
    v = v.replace("%", "").replace("_", "")
    return soql_escape(v)

def build_where_and_order(codigos: str | None, estado: str | None, texto: str | None, orden: str):
    condiciones = []

    # ---- Códigos ----
    if codigos:
        lista_codigos = []
        for c in codigos.split(","):
            c = c.strip()
            if not c:
                continue
            if not c.startswith("V1."):
                c = f"V1.{c}"
            lista_codigos.append(soql_escape(c))
        if lista_codigos:
            in_list = ", ".join(f"'{c}'" for c in lista_codigos)
            condiciones.append(f"codigo_principal_de_categoria IN ({in_list})")

    # ---- Estado (case-insensitive) ----
    if estado:
        estados = [e.strip() for e in estado.split(",") if e.strip()]
        if estados:
            estados_upper = [soql_escape(e.upper()) for e in estados]
            in_list = ", ".join(f"'{e}'" for e in estados_upper)
            condiciones.append(f"UPPER(estado_del_procedimiento) IN ({in_list})")

    # ---- Texto (AND entre términos) ----
    if texto:
        terms = [t for t in re.split(r"[,\s]+", texto) if t]
        if terms:
            fields = ["descripci_n_del_procedimiento"]
            term_clauses = []
            for t in terms:
                t_clean = sanitize_like_term(t).upper()
                ors = [f"UPPER({f}) LIKE '%{t_clean}%'" for f in fields]
                term_clauses.append("(" + " OR ".join(ors) + ")")
            condiciones.append(" AND ".join(term_clauses))

    order_map = {
        "recientes": "fecha_de_publicacion_del DESC",
        "antiguos": "fecha_de_publicacion_del ASC",
        "mayor_valor": "precio_base DESC, valor_estimado DESC",
        "menor_valor": "precio_base ASC, valor_estimado ASC",
    }
    order_clause = order_map.get(orden, order_map["recientes"])
    where_clause = " AND ".join(condiciones) if condiciones else None
    return where_clause, order_clause

@app.get("/", response_class=HTMLResponse)
def home(
    request: Request,
    codigos: str | None = Query(None, description="Códigos UNSPSC separados por comas"),
    estado: str | None = Query(None, description="Estado del procedimiento (uno o varios, separados por comas)"),
    texto: str | None = Query(None, description="Palabras clave a buscar en la descripción"),
    orden: str = Query("recientes", description="recientes|antiguos|mayor_valor|menor_valor"),
    page: int = Query(1, ge=1),
):
    limit = 20   # 👈 ahora son 20 resultados por página
    offset = (page - 1) * limit

    # construyes filtros y orden
    where_clause, order_clause = build_where_and_order(codigos, estado, texto, orden)

    data = []

    # 👇 aquí está el cambio importante
    if where_clause:   # solo hace la consulta si hay filtros (códigos, estado o texto)
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": order_clause,
            "$where": where_clause,
        }

        resp = requests.get(API_URL, params=params, timeout=20)
        if resp.status_code == 200:
            data = resp.json()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "data": data,
        "page": page,
        "codigos": codigos,
        "estado": estado,
        "texto": texto,
        "orden": orden,
    })


def _to_cell(v):
    if v is None:
        return ""
    if isinstance(v, (list, dict, set, tuple)):
        return str(v)
    return v if isinstance(v, (int, float, str)) else str(v)

@app.get("/export", response_class=StreamingResponse)
def export_xlsx(
    codigos: str | None = Query(None),
    estado: str | None = Query(None),
    texto: str | None = Query(None),
    orden: str = Query("recientes"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=1000),
):
    offset = (page - 1) * limit
    where_clause, order_clause = build_where_and_order(codigos, estado, texto, orden)

    params = {"$limit": limit, "$offset": offset, "$order": order_clause}
    if where_clause:
        params["$where"] = where_clause

    try:
        resp = requests.get(API_URL, params=params, timeout=30)
        if resp.status_code in (400, 422):
            params.pop("$order", None)
            resp = requests.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        rows = resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Fallo consultando datos.gov.co: {e}")

    if not rows:
        raise HTTPException(status_code=204, detail="No hay resultados en esta página para exportar")

    try:
        wb = Workbook()
        ws = wb.active
        ws.title = f"P{page}"

        headers = [
            "Código",
            "Estado",
            "Entidad",
            "Departamento",
            "Descripción",
            "Valor",
            "Fecha Publicación",
        ]
        ws.append(headers)
        for cell in ws[1]:
            cell.font = Font(bold=True)

        for r in rows:
            # Código: categoría + referencia
            codigo = (r.get("codigo_principal_de_categoria") or "").replace("V1.", "")
            referencia = r.get("referencia_del_proceso") or "N/A"
            codigo_full = f"{codigo} / {referencia}" if codigo or referencia else "N/A"

            # Estado
            estado = r.get("estado_del_procedimiento") or "N/A"

            # Entidad
            entidad = r.get("entidad") or "N/A"

            # Departamento
            depto = r.get("departamento_entidad") or "N/A"

            # Descripción (toma el primero disponible)
            descripcion = (
                r.get("descripci_n_del_procedimiento")
                or "N/A"
            )

            # Valor (precio_base o valor_estimado)
            valor = r.get("precio_base") or r.get("valor_estimado") or 0

            # Fecha
            fecha = (r.get("fecha_de_publicacion_del") or "")[:10] or "N/A"

            ws.append([codigo_full, estado, entidad, depto, descripcion, valor, fecha])

        bio = io.BytesIO()
        wb.save(bio)
        bio.seek(0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fallo generando Excel: {e}")

    filename = f"secop_p{page}.xlsx"
    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
