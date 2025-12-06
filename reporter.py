import os
from supabase import create_client, Client
from openai import OpenAI
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE ACCESO (Mismos Secrets que usa app.py) ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Inicializar clientes
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Modelo de embeddings usado en la Fase II (debe coincidir con la tabla)
EMBEDDING_MODEL = "text-embedding-3-small" 
LLM_MODEL = "gpt-4-turbo-2024-04-09" # Puedes usar 'gpt-4o' o el modelo Claude de tu elección

# --- 1. DEFINICIÓN DEL PROMPT MAESTRO ---

# Este prompt es una instrucción estricta para el LLM
PROMPT_MAESTRO = """
Actúa como un analista de negocios C-Level. Tu objetivo es generar un reporte ejecutivo diario basado
en las conversaciones de WhatsApp del día anterior.

INSTRUCCIONES DE FORMATO (Output):
- Utiliza formato Markdown.
- Tono: Formal, objetivo y conciso.

ANÁLISIS DE DATOS (Basado en el contexto proporcionado a continuación):

## I. Tareas Críticas y Acuerdos
1. **Acuerdos del Día:** (Máximo 3 puntos). Enumera las decisiones, acuerdos o tareas asignadas más relevantes.
2. **Problemas Bloqueantes:** (Máximo 2 puntos). Identifica obstáculos, escalamientos o problemas que requieran acción gerencial inmediata.
3. **Riesgos Identificados:** (Un párrafo). Resumen de cualquier riesgo potencial mencionado (ej: retrasos, fallas de proveedores).

## II. Resumen Operacional
1. **Métricas Clave:** (Sintetiza avance). Citas de progreso o estados de proyectos (incluye la descripción de imágenes procesadas si es relevante).
2. **Próximos Pasos:** (Máximo 3 puntos). Tareas que deben iniciar mañana.

---
REGLA DE ORO: Si no encuentras información sobre una sección, omítela. NUNCA inventes información.
"""

# --- 2. FUNCIONES DE CONSULTA RAG ---

def get_query_embedding(query: str) -> list[float]:
    """Genera el embedding del término de búsqueda."""
    response = openai_client.embeddings.create(
        input=query,
        model=EMBEDDING_MODEL
    )
    return response.data[0].embedding

def get_context_from_db(query_embedding: list[float]) -> str:
    """Consulta la DB usando pgvector y recupera los mensajes más relevantes."""
    
    # 24 horas atrás para un reporte diario
    time_threshold = (datetime.now() - timedelta(hours=24)).isoformat()
    
    # Consulta SQL nativa para pgvector
    # La consulta busca los vectores más cercanos a nuestro 'query_embedding'
    # usando el operador de distancia del coseno (<=>)
    # LIMIT 50: trae los 50 fragmentos más relevantes para evitar saturar el LLM (límite de tokens)
    
    consulta_sql = f"""
    SELECT
        contenido_texto,
        fecha_hora,
        remitente
    FROM mensajes_analisis
    WHERE fecha_hora >= '{time_threshold}' AND embedding IS NOT NULL
    ORDER BY embedding <=> '{query_embedding}'::vector
    LIMIT 50;
    """
    
    # Ejecutar la consulta en Supabase
    try:
        data = supabase.rpc('match_messages', params={'query_embedding': query_embedding, 'match_threshold': 0.78}).execute()
        
        # Nota: La forma más robusta es crear una función RPC en Supabase para el pgvector
        # Para simplificar aquí, se usa una consulta RPC ficticia que simula la búsqueda.
        
        # Aquí se simula la ejecución de la consulta
        response = supabase.from('mensajes_analisis').select('contenido_texto', 'fecha_hora', 'remitente').order('fecha_hora', desc=True).limit(50).execute()
        
        context_data = response.data
        
    except Exception as e:
        print(f"Error al consultar Supabase: {e}")
        return "ERROR: No se pudo obtener contexto de la base de datos."

    # Formatear el contexto para el LLM
    context_list = []
    for row in context_data:
        context_list.append(f"[{row['fecha_hora']} | {row['remitente']}]: {row['contenido_texto']}")

    return "\n---\n".join(context_list)

# --- 3. LÓGICA PRINCIPAL DEL REPORTE ---

def generate_daily_report():
    print("--- 🧠 Generando Reporte Ejecutivo RAG ---")
    
    # 1. Definir el "término de búsqueda" para obtener un contexto amplio (último día)
    query_topic = "Resumen de acuerdos, problemas y avances del último día de operación en los grupos de WhatsApp."
    
    # 2. Generar embedding de la consulta
    query_vector = get_query_embedding(query_topic)
    
    # 3. Obtener el contexto más relevante del pgvector (los 50 fragmentos clave)
    contexto_relevante = get_context_from_db(query_vector)
    
    if "ERROR" in contexto_relevante:
        return contexto_relevante

    print(f"Contexto recuperado (Longitud: {len(contexto_relevante)} chars). Pasando al LLM...")

    # 4. Enviar el Prompt Maestro + Contexto al LLM
    try:
        prompt_final = f"{PROMPT_MAESTRO}\n\n--- CONTEXTO DEL DÍA ---\n{contexto_relevante}"
        
        chat_completion = openai_client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": "Eres un analista ejecutivo experto, conciso y formal."},
                {"role": "user", "content": prompt_final}
            ],
            temperature=0.1, # Temperatura baja para que sea objetivo y no creativo
        )
        reporte_final = chat_completion.choices[0].message.content
        
        print("Reporte generado con éxito.")
        
        # 5. Salida/Distribución
        # En una aplicación real, aquí enviarías un email o lo guardarías en Supabase.
        
        print("\n\n=============== REPORTE FINAL GENERADO ===============")
        print(reporte_final)
        print("======================================================")
        return reporte_final
        
    except Exception as e:
        return f"Error al interactuar con el LLM: {e}"

if __name__ == "__main__":
    generate_daily_report()
