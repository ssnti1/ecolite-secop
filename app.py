from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import requests

app = FastAPI()
templates = Jinja2Templates(directory="templates")

API_URL = "https://www.datos.gov.co/resource/p6dx-8zbt.json"

def soql_escape(v: str) -> str:
    return v.replace("'", "''")

@app.get("/", response_class=HTMLResponse)
def home(
    request: Request,
    codigos: str | None = Query(None, description="Códigos UNSPSC separados por comas"),
    estado: str | None = Query(None, description="Estado del procedimiento (uno o varios, separados por comas)"),
    orden: str = Query("recientes", description="recientes|antiguos|mayor_valor|menor_valor"),
    page: int = Query(1, ge=1),
):
    data = []
    limit = 10
    offset = (page - 1) * limit

    condiciones = []

    # --- Códigos ---
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

    # --- Estado (case-insensitive) ---
    if estado:
        estados = [e.strip() for e in estado.split(",") if e.strip()]
        if estados:
            estados_upper = [soql_escape(e.upper()) for e in estados]
            in_list = ", ".join(f"'{e}'" for e in estados_upper)
            condiciones.append(f"UPPER(estado_del_procedimiento) IN ({in_list})")

    # --- Orden ---
    order_map = {
        "recientes": "fecha_de_publicacion_del DESC",
        "antiguos": "fecha_de_publicacion_del ASC",
        # usa ambos campos de monto por si uno viene nulo
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
        "orden": orden,  # <— recordar selección en el UI
    })
