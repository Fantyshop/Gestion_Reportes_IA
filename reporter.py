import os
import json
from supabase import create_client, Client
from openai import OpenAI
from datetime import datetime

# --- CONFIGURACIÓN DE ACCESO (Asume Variables de Entorno en Railway) ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Inicializar clientes
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Modelos
EMBEDDING_MODEL = "text-embedding-3-small"
LLM_MODEL = "gpt-4-turbo-2024-04-09" # O tu modelo preferido (Claude 3.5 Sonnet)

# --- 1. DEFINICIÓN DEL PROMPT MAESTRO ---

PROMPT_MAESTRO = """
Actúa como un analista de negocios C-Level. Tu objetivo es generar un reporte ejecutivo diario
basado únicamente en el CONTEXTO proporcionado de las conversaciones de WhatsApp del día anterior.

INSTRUCCIONES DE FORMATO (Output):
- Utiliza formato Markdown.
- Máximo 500 palabras.
- Tono: Formal, objetivo y conciso.

ANÁLISIS DE DATOS:

## I. Tareas Críticas y Acuerdos
1. **Acuerdos del Día:** (Máximo 3 puntos). Las decisiones clave o compromisos de acción.
2. **Problemas Bloqueantes:** (Máximo 2 puntos). Obstáculos o escalamientos que requieren intervención.
3. **Riesgos Identificados:** (Un párrafo). Resumen de riesgos potenciales (ej: retrasos, fallas).

## II. Resumen Operacional
1. **Métricas Clave/Avance:** Citas de progreso de proyectos.
2. **Próximos Pasos:** (Máximo 3 puntos). Tareas inmediatas pendientes.

---
REGLA DE ORO: Si no encuentras información para una sección, omítela o responde: "No se identificaron datos relevantes." NUNCA INVENTES.
"""

# --- 2. FUNCIONES DE CONSULTA RAG ---

def get_query_embedding(query: str) -> list[float]:
    """Genera el embedding del término de búsqueda usando OpenAI."""
    response = openai_client.embeddings.create(
        input=query,
        model=EMBEDDING_MODEL
    )
    return response.data[0].embedding

def get_context_from_db(query_embedding: list[float]) -> str:
    """Consulta la DB usando la función RPC match_messages."""
    
    # Parámetros para la función RPC
    params = {
        'query_embedding': query_embedding,
        'match_threshold': 0.78, # Umbral de similitud (ajustable)
        'match_count': 50,      # Número de fragmentos a recuperar
        'time_limit_hours': 24  # Últimas 24 horas
    }
    
    try:
        # Llamada a la función RPC que usa pgvector
        response = supabase.rpc('match_messages', params).execute()
        
        context_data = response.data
        
    except Exception as e:
        print(f"Error al consultar Supabase (RPC): {e}")
        return "ERROR: No se pudo obtener contexto de la base de datos."

    # Formatear el contexto para el LLM
    context_list = []
    for row in context_data:
        # Incluimos la similitud para fines de depuración
        sim = round(row.get('similarity', 0.0), 3)
        context_list.append(f"[Similitud: {sim} | {row['fecha_hora']} | {row['remitente']}]: {row['contenido_texto']}")

    if not context_list:
        return "SIN DATOS RELEVANTES: No se encontraron mensajes que coincidan con la búsqueda RAG en las últimas 24 horas."
        
    return "\n---\n".join(context_list)

# --- 3. LÓGICA PRINCIPAL DEL REPORTE ---

def generate_daily_report():
    print("--- 🧠 Iniciando Generación de Reporte RAG ---")
    
    # 1. Definir el "término de búsqueda" para obtener un contexto amplio
    query_topic = "Resumen de acuerdos, problemas y avances del último día de operación en los grupos de WhatsApp para reporte ejecutivo."
    
    # 2. Generar embedding de la consulta
    query_vector = get_query_embedding(query_topic)
    
    # 3. Obtener el contexto más relevante del pgvector (los 50 fragmentos clave)
    contexto_relevante = get_context_from_db(query_vector)
    
    if "ERROR" in contexto_relevante or "SIN DATOS RELEVANTES" in contexto_relevante:
        print(f"Abortando reporte. {contexto_relevante}")
        return f"Reporte fallido: {contexto_relevante}"

    # 4. Enviar el Prompt Maestro + Contexto al LLM
    try:
        prompt_final = f"{PROMPT_MAESTRO}\n\n--- CONTEXTO RECUPERADO DE LA DB ---\n{contexto_relevante}"
        
        # Enviar la solicitud a la API de OpenAI
        chat_completion = openai_client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": "Eres un analista ejecutivo experto, conciso y formal."},
                {"role": "user", "content": prompt_final}
            ],
            temperature=0.1,
        )
        reporte_final = chat_completion.choices[0].message.content
        
        print("\n\n=============== REPORTE FINAL GENERADO ===============")
        print(reporte_final)
        print("======================================================")
        
        # 5. Distribución (Aquí añadirías el código de envío de email o WhatsApp)
        # Ejemplo: distribute_report_via_email(reporte_final)
        
        return reporte_final
        
    except Exception as e:
        return f"Error al interactuar con el LLM: {e}"

if __name__ == "__main__":
    generate_daily_report()
