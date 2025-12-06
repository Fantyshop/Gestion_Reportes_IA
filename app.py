import os
from datetime import datetime, timedelta
from supabase import create_client, Client
from openai import OpenAI
import anthropic

# Importar catálogo de grupos
from grupos_config import (
    get_grupo_info, 
    get_grupo_context, 
    get_summary_all_grupos,
    CONTEXTO_MINERA_CENTINELA,
    GRUPOS_EMPRESAS
)

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
REPORT_TIME_WINDOW_HOURS = int(os.environ.get("REPORT_TIME_WINDOW_HOURS", "24"))  # Últimas N horas
REPORT_START_DATE = os.environ.get("REPORT_START_DATE")  # Formato: "2025-12-01" (opcional)
REPORT_END_DATE = os.environ.get("REPORT_END_DATE")      # Formato: "2025-12-06" (opcional)
MAX_MESSAGES_IN_REPORT = 100   # Máximo de mensajes a analizar
SIMILARITY_THRESHOLD = 0.3     # Umbral mínimo de similitud para búsqueda semántica

# ----------------------------------------------------
# 2. FUNCIONES DE CONSULTA RAG
# ----------------------------------------------------

def get_messages_by_date_range(start_date: str = None, end_date: str = None, hours: int = None) -> list:
    """
    Obtiene mensajes por rango de fechas o por últimas N horas.
    
    Prioridad:
    1. Si start_date y end_date están definidos, usa ese rango
    2. Si no, usa las últimas N horas
    
    Args:
        start_date: Fecha inicio en formato ISO "2025-12-01" o "2025-12-01T00:00:00"
        end_date: Fecha fin en formato ISO "2025-12-06" o "2025-12-06T23:59:59"
        hours: Número de horas hacia atrás desde ahora
    """
    try:
        # Determinar el rango de fechas
        if start_date and end_date:
            # Usar rango específico
            start_str = start_date if 'T' in start_date else f"{start_date}T00:00:00"
            end_str = end_date if 'T' in end_date else f"{end_date}T23:59:59"
            
            print(f"   📅 Rango de fechas: {start_str} a {end_str}")
            
            # Consultar mensajes en rango
            response = supabase.from_('mensajes_analisis').select(
                'id, grupo_id, fecha_hora, remitente, contenido_texto, es_imagen, url_storage, embedding, whatsapp_message_id'
            ).gte('fecha_hora', start_str).lte('fecha_hora', end_str).is_('deleted_at', 'null').not_.is_('embedding', 'null').order('fecha_hora', desc=False).limit(MAX_MESSAGES_IN_REPORT).execute()
            
        elif hours:
            # Usar últimas N horas
            cutoff_time = datetime.now() - timedelta(hours=hours)
            cutoff_str = cutoff_time.isoformat()
            
            print(f"   ⏰ Últimas {hours} horas (desde {cutoff_str})")
            
            response = supabase.from_('mensajes_analisis').select(
                'id, grupo_id, fecha_hora, remitente, contenido_texto, es_imagen, url_storage, embedding, whatsapp_message_id'
            ).gte('fecha_hora', cutoff_str).is_('deleted_at', 'null').not_.is_('embedding', 'null').order('fecha_hora', desc=False).limit(MAX_MESSAGES_IN_REPORT).execute()
        else:
            raise ValueError("Debe especificar start_date/end_date o hours")
        
        return response.data if response.data else []
        
    except Exception as e:
        print(f"❌ Error obteniendo mensajes: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_messages_last_n_hours(hours: int = 24) -> list:
    """
    Obtiene todos los mensajes de las últimas N horas que tienen embedding.
    (Mantiene compatibilidad con código existente)
    """
    return get_messages_by_date_range(hours=hours)

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
        print(f"⚠️ Búsqueda semántica no disponible (función SQL no creada): {e}")
        print(f"   Usando método alternativo...")
        # Fallback: obtener todos los mensajes del período
        if time_filter_hours:
            return get_messages_last_n_hours(time_filter_hours)
        return []

def aggregate_messages_by_topic(messages: list) -> dict:
    """
    Agrupa mensajes por grupos/empresas y temas.
    Retorna un diccionario con análisis por grupo.
    """
    # Agrupar por grupo_id
    messages_by_group = {}
    
    for msg in messages:
        grupo_id = msg.get('grupo_id')
        
        if grupo_id not in messages_by_group:
            grupo_info = get_grupo_info(grupo_id)
            messages_by_group[grupo_id] = {
                'info': grupo_info,
                'messages': [],
                'count': 0
            }
        
        messages_by_group[grupo_id]['messages'].append(msg)
        messages_by_group[grupo_id]['count'] += 1
    
    return messages_by_group

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
        sender = msg.get('remitente', 'Desconocido')
        content = msg.get('contenido_texto', '[Sin texto]')
        is_image = msg.get('es_imagen', False)
        
        # Formato con remitente
        msg_text = f"\n[{timestamp}] {sender}"
        if is_image:
            msg_text += " [📷 Imagen/Video]"
        msg_text += f":\n{content}\n"
        
        if current_length + len(msg_text) > max_chars:
            context_parts.append("\n... (mensajes adicionales omitidos por límite de longitud)")
            break
        
        context_parts.append(msg_text)
        current_length += len(msg_text)
    
    return "".join(context_parts)

def generate_report_with_claude(messages: list, groups_data: dict) -> str:
    """
    Genera el reporte ejecutivo usando Claude (Anthropic).
    """
    if not claude_client:
        print("⚠️ Claude API no configurado, usando GPT-4 como fallback")
        return generate_report_with_gpt4(messages, groups_data)
    
    try:
        # Preparar contexto
        context = format_messages_for_context(messages)
        
        # Resumen de grupos activos
        groups_summary = []
        for grupo_id, data in groups_data.items():
            if data['count'] > 0:
                info = data['info']
                if info:
                    groups_summary.append(
                        f"- {info['nombre']} ({info['empresa']}): {data['count']} mensajes - {info['tipo_servicio']}"
                    )
                else:
                    groups_summary.append(f"- Grupo ID {grupo_id}: {data['count']} mensajes")
        
        groups_summary_text = "\n".join(groups_summary)
        
        # Contexto de todas las empresas
        all_grupos_context = get_summary_all_grupos()
        
        prompt = f"""{CONTEXTO_MINERA_CENTINELA}

---

Eres un analista senior de operaciones mineras para Minera Centinela (Antofagasta Minerals). 

Tu tarea es generar un **Reporte Ejecutivo Diario** basado en las conversaciones de WhatsApp del equipo de GSdSO (Gestión de Sistemas de Operación) de las últimas 24 horas.

**GRUPOS/EMPRESAS MONITOREADOS:**
{all_grupos_context}

**ACTIVIDAD DEL PERÍODO (Últimas 24 horas):**
{groups_summary_text}

**CONVERSACIONES COMPLETAS:**
{context}

**INSTRUCCIONES PARA EL REPORTE:**

1. **Estructura del Reporte:**
   - Resumen Ejecutivo (3-4 líneas con los puntos más críticos)
   - Análisis por Empresa/Servicio (sección para cada empresa con actividad)
   - Situaciones Críticas o Alertas (si las hay, destacar problemas que requieren atención)
   - Avances en Proyectos o Trabajos (si se mencionan)
   - Próximos Pasos o Seguimientos Requeridos

2. **Para cada Empresa/Servicio:**
   - Nombre de la empresa y tipo de servicio
   - Resumen de actividades o eventos principales
   - Problemas o incidentes (si los hay)
   - Estado general (operando normal, con restricciones, detenido, etc.)

3. **Estilo:**
   - Profesional, conciso y accionable
   - Enfócate en lo relevante para la gestión
   - Usa números y datos cuando estén disponibles
   - Identifica problemas recurrentes o patrones
   - Menciona específicamente las empresas por nombre (AMECO, FTF, ELEVEN, etc.)

4. **Formato:**
   - Usa Markdown
   - Incluye encabezados claros (##)
   - Usa bullets para listas
   - Destaca lo crítico con **negrita**
   - Usa tablas si hay datos comparativos

Genera el reporte ahora:"""

        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=3000,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        return response.content[0].text
        
    except Exception as e:
        print(f"❌ Error generando reporte con Claude: {e}")
        return None

def generate_report_with_gpt4(messages: list, groups_data: dict) -> str:
    """
    Genera el reporte ejecutivo usando GPT-4 (fallback).
    """
    try:
        context = format_messages_for_context(messages)
        
        # Resumen de grupos activos
        groups_summary = []
        for grupo_id, data in groups_data.items():
            if data['count'] > 0:
                info = data['info']
                if info:
                    groups_summary.append(
                        f"- {info['nombre']} ({info['empresa']}): {data['count']} mensajes - {info['tipo_servicio']}"
                    )
                else:
                    groups_summary.append(f"- Grupo ID {grupo_id}: {data['count']} mensajes")
        
        groups_summary_text = "\n".join(groups_summary)
        all_grupos_context = get_summary_all_grupos()
        
        prompt = f"""{CONTEXTO_MINERA_CENTINELA}

---

**GRUPOS/EMPRESAS MONITOREADOS:**
{all_grupos_context}

**ACTIVIDAD DEL PERÍODO (Últimas 24 horas):**
{groups_summary_text}

**CONVERSACIONES COMPLETAS:**
{context}

**INSTRUCCIONES PARA EL REPORTE:**

1. **Estructura del Reporte:**
   - Resumen Ejecutivo (3-4 líneas con los puntos más críticos)
   - Análisis por Empresa/Servicio
   - Situaciones Críticas o Alertas
   - Avances en Proyectos
   - Próximos Pasos

2. **Estilo:**
   - Profesional, conciso y accionable
   - Usa números y datos
   - Identifica patrones

3. **Formato:**
   - Usa Markdown
   - Destaca lo crítico con **negrita**

Genera el reporte ahora:"""

        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Eres un analista experto en operaciones mineras."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=3000,
            temperature=0.3
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"❌ Error generando reporte con GPT-4: {e}")
        return None

# ----------------------------------------------------
# 4. GUARDADO Y EXPORTACIÓN
# ----------------------------------------------------

def save_report_to_file(report_content: str, periodo_texto: str, output_dir: str = "/tmp") -> str:
    """
    Guarda el reporte en un archivo Markdown con timestamp.
    
    Args:
        report_content: Contenido del reporte en Markdown
        periodo_texto: Texto descriptivo del período (ej: "Últimas 24 horas")
        output_dir: Directorio donde guardar (default: /tmp para Railway)
    """
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"reporte_ejecutivo_{timestamp}.md"
        filepath = os.path.join(output_dir, filename)
        
        # Agregar header al reporte
        header = f"""# Reporte Ejecutivo Diario - Minera Centinela
**Equipo:** GSdSO (Gestión de Sistemas de Operación)  
**Fecha de generación:** {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}  
**Período analizado:** {periodo_texto}  

---

"""
        
        full_content = header + report_content
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        print(f"✅ Reporte guardado en: {filepath}")
        
        # También imprimir el contenido completo en logs para que se vea en Railway
        print("\n" + "="*70)
        print("📄 CONTENIDO COMPLETO DEL REPORTE:")
        print("="*70)
        print(full_content)
        print("="*70 + "\n")
        
        return filepath
        
    except Exception as e:
        print(f"❌ Error guardando reporte: {e}")
        import traceback
        traceback.print_exc()
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
    
    # Determinar modo de consulta
    if REPORT_START_DATE and REPORT_END_DATE:
        print(f"📅 Modo: Rango de fechas específico")
        print(f"   Inicio: {REPORT_START_DATE}")
        print(f"   Fin: {REPORT_END_DATE}")
        periodo_texto = f"del {REPORT_START_DATE} al {REPORT_END_DATE}"
    else:
        print(f"⏰ Modo: Últimas {REPORT_TIME_WINDOW_HOURS} horas")
        periodo_texto = f"Últimas {REPORT_TIME_WINDOW_HOURS} horas"
    
    print("="*70 + "\n")
    
    # 1. Obtener mensajes del período
    print("📥 Obteniendo mensajes del período...")
    
    if REPORT_START_DATE and REPORT_END_DATE:
        messages = get_messages_by_date_range(
            start_date=REPORT_START_DATE,
            end_date=REPORT_END_DATE
        )
    else:
        messages = get_messages_by_date_range(hours=REPORT_TIME_WINDOW_HOURS)
    
    if not messages:
        print("⚠️ No se encontraron mensajes en el período especificado.")
        return None
    
    print(f"✅ Se encontraron {len(messages)} mensajes con embeddings.")
    
    # 2. Agrupar por grupos/empresas
    print("\n🏷️ Agrupando mensajes por grupos/empresas...")
    groups_data = aggregate_messages_by_topic(messages)
    
    for grupo_id, data in groups_data.items():
        info = data['info']
        if info:
            print(f"   • {info['nombre']} ({info['empresa']}): {data['count']} mensajes")
        else:
            print(f"   • Grupo ID {grupo_id}: {data['count']} mensajes")
    
    # 3. Generar reporte con IA
    print("\n🤖 Generando reporte ejecutivo con IA...")
    report = generate_report_with_claude(messages, groups_data)
    
    if not report:
        print("❌ No se pudo generar el reporte.")
        return None
    
    print("✅ Reporte generado exitosamente.")
    
    # 4. Guardar reporte
    print("\n💾 Guardando reporte...")
    filepath = save_report_to_file(report, periodo_texto)
    
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
