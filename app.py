from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import requests, re

app = FastAPI()
templates = Jinja2Templates(directory="templates")

API_URL = "https://www.datos.gov.co/resource/p6dx-8zbt.json"

def soql_escape(v: str) -> str:
    # Escapa comillas simples
    return v.replace("'", "''")

def sanitize_like_term(v: str) -> str:
    # Elimina comodines que podrían romper el LIKE en SoQL
    v = v.replace("%", "").replace("_", "")
    return soql_escape(v)

@app.get("/", response_class=HTMLResponse)
def home(
    request: Request,
    codigos: str | None = Query(None, description="Códigos UNSPSC separados por comas"),
    estado: str | None = Query(None, description="Estado del procedimiento (uno o varios, separados por comas)"),
    texto: str | None = Query(None, description="Palabras clave a buscar en la descripción"),
    orden: str = Query("recientes", description="recientes|antiguos|mayor_valor|menor_valor"),
    page: int = Query(1, ge=1),
):
    data = []
    limit = 10
    offset = (page - 1) * limit

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

    # ---- Texto en descripción (case-insensitive, AND entre términos; OR entre campos) ----
    if texto:
        # separa por espacios o comas; ignora cadenas vacías
        terms = [t for t in re.split(r"[,\s]+", texto) if t]
        if terms:
            fields = [
                "descripci_n_del_procedimiento"
            ]
            term_clauses = []
            for t in terms:
                t_clean = sanitize_like_term(t).upper()
                # (UPPER(campo1) LIKE '%T%' OR UPPER(campo2) LIKE '%T%' OR ...)
                ors = [f"UPPER({f}) LIKE '%{t_clean}%'" for f in fields]
                term_clauses.append("(" + " OR ".join(ors) + ")")
            # AND entre cada término para que todos aparezcan
            condiciones.append(" AND ".join(term_clauses))

    # ---- Orden ----
    order_map = {
        "recientes": "fecha_de_publicacion_del DESC",
        "antiguos": "fecha_de_publicacion_del ASC",
        "mayor_valor": "precio_base DESC, valor_estimado DESC",
        "menor_valor": "precio_base ASC, valor_estimado ASC",
    }
    order_clause = order_map.get(orden, order_map["recientes"])

    if condiciones:
        params = {
            "$where": " AND ".join(condiciones),
            "$limit": limit,
            "$offset": offset,
            "$order": order_clause,
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
        "texto": texto,   # <- recordar lo tecleado en el UI
        "orden": orden,
    })
