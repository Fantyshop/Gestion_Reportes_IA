import os
from datetime import datetime, timedelta
from supabase import create_client, Client
from openai import OpenAI
import anthropic

# ----------------------------------------------------
# 1. CONFIGURACIÓN
# ----------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")  # Para usar Claude

# Inicializar clientes
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None

# Configuración del reporte
REPORT_TIME_WINDOW_HOURS = 24  # Últimas 24 horas
MAX_MESSAGES_IN_REPORT = 100   # Máximo de mensajes a analizar
SIMILARITY_THRESHOLD = 0.3     # Umbral mínimo de similitud para búsqueda semántica

# ----------------------------------------------------
# 2. FUNCIONES DE CONSULTA RAG
# ----------------------------------------------------

def get_messages_last_n_hours(hours: int = 24) -> list:
    """
    Obtiene todos los mensajes de las últimas N horas que tienen embedding.
    """
    try:
        # Calcular timestamp de inicio
        cutoff_time = datetime.now() - timedelta(hours=hours)
        cutoff_str = cutoff_time.isoformat()
        
        # Consultar mensajes
        response = supabase.from_('mensajes_analisis').select(
            'id, fecha_hora, remitente_numero, remitente_nombre, contenido_texto, es_imagen, url_storage, embedding'
        ).gte('fecha_hora', cutoff_str).not_.is_('embedding', 'null').order('fecha_hora', desc=False).limit(MAX_MESSAGES_IN_REPORT).execute()
        
        return response.data if response.data else []
        
    except Exception as e:
        print(f"❌ Error obteniendo mensajes: {e}")
        return []

def semantic_search(query_text: str, top_k: int = 20, time_filter_hours: int = None) -> list:
    """
    Realiza búsqueda semántica sobre los mensajes usando embeddings.
    
    Args:
        query_text: Texto de búsqueda (ej: "problemas operacionales")
        top_k: Número de resultados más similares
        time_filter_hours: Filtrar solo mensajes de las últimas N horas (opcional)
    """
    try:
        # 1. Generar embedding de la consulta
        embedding_response = openai_client.embeddings.create(
            input=query_text,
            model="text-embedding-3-small"
        )
        query_embedding = embedding_response.data[0].embedding
        
        # 2. Realizar búsqueda usando pgvector
        # Nota: Supabase Python client no tiene función match nativa aún,
        # así que usamos RPC (función de PostgreSQL)
        
        params = {
            'query_embedding': query_embedding,
            'match_threshold': SIMILARITY_THRESHOLD,
            'match_count': top_k
        }
        
        if time_filter_hours:
            cutoff_time = datetime.now() - timedelta(hours=time_filter_hours)
            params['time_filter'] = cutoff_time.isoformat()
        
        # Ejecutar función de búsqueda semántica
        response = supabase.rpc('match_messages', params).execute()
        
        return response.data if response.data else []
        
    except Exception as e:
        print(f"❌ Error en búsqueda semántica: {e}")
        return []

def aggregate_messages_by_topic(messages: list) -> dict:
    """
    Agrupa mensajes por temas usando clustering simple.
    Retorna un diccionario con temas identificados.
    """
    # Para simplicidad, agrupar por remitente y timestamp cercano
    # En producción, podrías usar clustering de embeddings
    
    topics = {
        'operaciones': [],
        'mantenimiento': [],
        'seguridad': [],
        'produccion': [],
        'otros': []
    }
    
    keywords = {
        'operaciones': ['operación', 'proceso', 'planta', 'equipo', 'bomba', 'valvula'],
        'mantenimiento': ['mantenimiento', 'reparación', 'falla', 'avería', 'preventivo'],
        'seguridad': ['seguridad', 'accidente', 'riesgo', 'incidente', 'peligro', 'epp'],
        'produccion': ['producción', 'toneladas', 'rendimiento', 'eficiencia', 'target']
    }
    
    for msg in messages:
        content = (msg.get('contenido_texto', '') or '').lower()
        categorized = False
        
        for topic, kws in keywords.items():
            if any(kw in content for kw in kws):
                topics[topic].append(msg)
                categorized = True
                break
        
        if not categorized:
            topics['otros'].append(msg)
    
    return topics

# ----------------------------------------------------
# 3. GENERACIÓN DE REPORTE CON IA
# ----------------------------------------------------

def format_messages_for_context(messages: list, max_chars: int = 15000) -> str:
    """
    Formatea los mensajes en un contexto legible para la IA.
    """
    context_parts = []
    current_length = 0
    
    for msg in messages:
        timestamp = msg.get('fecha_hora', 'N/A')
        sender = msg.get('remitente_nombre', 'Desconocido')
        content = msg.get('contenido_texto', '[Sin texto]')
        
        msg_text = f"\n[{timestamp}] {sender}:\n{content}\n"
        
        if current_length + len(msg_text) > max_chars:
            context_parts.append("\n... (mensajes adicionales omitidos por límite de longitud)")
            break
        
        context_parts.append(msg_text)
        current_length += len(msg_text)
    
    return "".join(context_parts)

def generate_report_with_claude(messages: list, topics: dict) -> str:
    """
    Genera el reporte ejecutivo usando Claude (Anthropic).
    """
    if not claude_client:
        print("⚠️ Claude API no configurado, usando GPT-4 como fallback")
        return generate_report_with_gpt4(messages, topics)
    
    try:
        # Preparar contexto
        context = format_messages_for_context(messages)
        
        # Resumen de tópicos
        topic_summary = "\n".join([
            f"- {topic.capitalize()}: {len(msgs)} mensajes" 
            for topic, msgs in topics.items() if len(msgs) > 0
        ])
        
        prompt = f"""Eres un analista senior de operaciones mineras para Minera Centinela (Antofagasta Minerals). 

Tu tarea es generar un **Reporte Ejecutivo Diario** basado en las conversaciones de WhatsApp del equipo de GSdSO (Gestión de Sistemas de Operación) de las últimas 24 horas.

**DISTRIBUCIÓN DE MENSAJES POR TEMA:**
{topic_summary}

**CONVERSACIONES COMPLETAS:**
{context}

**INSTRUCCIONES PARA EL REPORTE:**

1. **Estructura del Reporte:**
   - Resumen Ejecutivo (3-4 líneas)
   - Hallazgos Principales (bullet points, máximo 5)
   - Situaciones Críticas o Alertas (si las hay)
   - Avances en Proyectos (si se mencionan)
   - Próximos Pasos o Seguimientos Requeridos

2. **Estilo:**
   - Profesional, conciso y accionable
   - Enfócate en lo relevante para la gestión
   - Usa números y datos cuando estén disponibles
   - Identifica problemas recurrentes o patrones

3. **Formato:**
   - Usa Markdown
   - Incluye encabezados claros (##)
   - Usa bullets para listas
   - Destaca lo crítico con **negrita**

Genera el reporte ahora:"""

        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        return response.content[0].text
        
    except Exception as e:
        print(f"❌ Error generando reporte con Claude: {e}")
        return None

def generate_report_with_gpt4(messages: list, topics: dict) -> str:
    """
    Genera el reporte ejecutivo usando GPT-4 (fallback).
    """
    try:
        context = format_messages_for_context(messages)
        
        topic_summary = "\n".join([
            f"- {topic.capitalize()}: {len(msgs)} mensajes" 
            for topic, msgs in topics.items() if len(msgs) > 0
        ])
        
        prompt = f"""Eres un analista senior de operaciones mineras para Minera Centinela (Antofagasta Minerals). 

Tu tarea es generar un **Reporte Ejecutivo Diario** basado en las conversaciones de WhatsApp del equipo de GSdSO (Gestión de Sistemas de Operación) de las últimas 24 horas.

**DISTRIBUCIÓN DE MENSAJES POR TEMA:**
{topic_summary}

**CONVERSACIONES COMPLETAS:**
{context}

**INSTRUCCIONES PARA EL REPORTE:**

1. **Estructura del Reporte:**
   - Resumen Ejecutivo (3-4 líneas)
   - Hallazgos Principales (bullet points, máximo 5)
   - Situaciones Críticas o Alertas (si las hay)
   - Avances en Proyectos (si se mencionan)
   - Próximos Pasos o Seguimientos Requeridos

2. **Estilo:**
   - Profesional, conciso y accionable
   - Enfócate en lo relevante para la gestión
   - Usa números y datos cuando estén disponibles
   - Identifica problemas recurrentes o patrones

3. **Formato:**
   - Usa Markdown
   - Incluye encabezados claros (##)
   - Usa bullets para listas
   - Destaca lo crítico con **negrita**

Genera el reporte ahora:"""

        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Eres un analista experto en operaciones mineras."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=2000,
            temperature=0.3
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"❌ Error generando reporte con GPT-4: {e}")
        return None

# ----------------------------------------------------
# 4. GUARDADO Y EXPORTACIÓN
# ----------------------------------------------------

def save_report_to_file(report_content: str, output_dir: str = "/mnt/user-data/outputs") -> str:
    """
    Guarda el reporte en un archivo Markdown con timestamp.
    """
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"reporte_ejecutivo_{timestamp}.md"
        filepath = os.path.join(output_dir, filename)
        
        # Agregar header al reporte
        header = f"""# Reporte Ejecutivo Diario - Minera Centinela
**Equipo:** GSdSO (Gestión de Sistemas de Operación)  
**Fecha:** {datetime.now().strftime("%d/%m/%Y")}  
**Período:** Últimas 24 horas  
**Generado:** {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}

---

"""
        
        full_content = header + report_content
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        print(f"✅ Reporte guardado en: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"❌ Error guardando reporte: {e}")
        return None

# ----------------------------------------------------
# 5. FUNCIÓN PRINCIPAL
# ----------------------------------------------------

def generate_daily_report():
    """
    Genera el reporte ejecutivo diario completo.
    """
    print("\n" + "="*70)
    print("📊 GENERADOR DE REPORTE EJECUTIVO DIARIO")
    print("="*70)
    print(f"🕐 Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"⏰ Período: Últimas {REPORT_TIME_WINDOW_HOURS} horas")
    print("="*70 + "\n")
    
    # 1. Obtener mensajes del período
    print("📥 Obteniendo mensajes del período...")
    messages = get_messages_last_n_hours(REPORT_TIME_WINDOW_HOURS)
    
    if not messages:
        print("⚠️ No se encontraron mensajes en el período especificado.")
        return None
    
    print(f"✅ Se encontraron {len(messages)} mensajes con embeddings.")
    
    # 2. Agrupar por tópicos
    print("\n🏷️ Agrupando mensajes por tópicos...")
    topics = aggregate_messages_by_topic(messages)
    
    for topic, msgs in topics.items():
        if len(msgs) > 0:
            print(f"   • {topic.capitalize()}: {len(msgs)} mensajes")
    
    # 3. Generar reporte con IA
    print("\n🤖 Generando reporte ejecutivo con IA...")
    report = generate_report_with_claude(messages, topics)
    
    if not report:
        print("❌ No se pudo generar el reporte.")
        return None
    
    print("✅ Reporte generado exitosamente.")
    
    # 4. Guardar reporte
    print("\n💾 Guardando reporte...")
    filepath = save_report_to_file(report)
    
    if filepath:
        print(f"\n{'='*70}")
        print("✅ REPORTE COMPLETADO")
        print(f"📄 Archivo: {filepath}")
        print("="*70 + "\n")
        
        # Mostrar preview
        print("📋 PREVIEW DEL REPORTE:")
        print("-"*70)
        print(report[:500] + "..." if len(report) > 500 else report)
        print("-"*70 + "\n")
        
        return filepath
    else:
        return None

# ----------------------------------------------------
# 6. PUNTO DE ENTRADA
# ----------------------------------------------------

if __name__ == "__main__":
    generate_daily_report()
