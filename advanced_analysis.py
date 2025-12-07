"""
Sistema de Análisis Avanzado de Reportes Operacionales
Minera Centinela - GSdSO
Análisis en múltiples pasadas con Claude Sonnet 4
"""

from datetime import datetime
from typing import Dict, List, Tuple
import json
import os
import anthropic

# Cliente de Anthropic (Claude)
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None

# Importar función de formateo de mensajes
# Esta función debe existir en report_generator.py
def format_messages_for_context(messages: list, max_chars: int = 50000) -> str:
    """
    Formatea los mensajes en un contexto legible para la IA.
    Incluye información sobre archivos adjuntos.
    """
    context_parts = []
    current_length = 0
    
    for msg in messages:
        timestamp = msg.get('fecha_hora', 'N/A')
        sender = msg.get('remitente', 'Desconocido')
        content = msg.get('contenido_texto', '[Sin texto]')
        is_image = msg.get('es_imagen', False)
        url_storage = msg.get('url_storage', '')
        
        # Formato con remitente
        msg_text = f"\n[{timestamp}] {sender}"
        
        # Identificar tipo de archivo adjunto
        if url_storage:
            if '.mp4' in url_storage.lower() or '.mov' in url_storage.lower():
                msg_text += " [🎬 Video adjunto]"
            elif is_image or any(ext in url_storage.lower() for ext in ['.jpg', '.jpeg', '.png', '.webp', '.gif']):
                msg_text += " [📷 Imagen adjunta]"
            elif '.pdf' in url_storage.lower():
                msg_text += " [📄 PDF adjunto]"
            elif any(ext in url_storage.lower() for ext in ['.xlsx', '.xls']):
                msg_text += " [📊 Excel adjunto]"
            elif any(ext in url_storage.lower() for ext in ['.docx', '.doc']):
                msg_text += " [📝 Word adjunto]"
            else:
                msg_text += " [📎 Archivo adjunto]"
        
        msg_text += f":\n{content}\n"
        
        if current_length + len(msg_text) > max_chars:
            context_parts.append("\n... (mensajes adicionales omitidos por límite de longitud)")
            break
        
        context_parts.append(msg_text)
        current_length += len(msg_text)
    
    return "".join(context_parts)

# ----------------------------------------------------
# PROMPTS ESPECIALIZADOS POR CATEGORÍA
# ----------------------------------------------------

PROMPT_ANALISIS_DEMORAS_QP = """Eres un analista experto en planificación y control de mantenimiento minero.

Analiza las siguientes conversaciones y extrae TODA la información sobre:

1. **QUIEBRES DE PLAN (QP)**
   - Identifica menciones de "QP", "quiebre de plan", cambios no programados
   - Extrae: QP #, fecha, área afectada, equipo, razón del quiebre
   - Documenta: tiempo de demora, impacto en cronograma

2. **DEMORAS E IMPREVISTOS**
   - Retrasos en inicio/término de trabajos
   - Esperas por: permisos, materiales, equipos, personal, clima
   - Causas raíz: falta de planificación, problemas técnicos, coordinación
   - Tiempo perdido (horas/días)

3. **ACTIVIDADES EMERGENTES**
   - Trabajos no programados que se ejecutaron
   - Prioridad vs actividades planificadas
   - Impacto en plan original

**FORMATO DE SALIDA (JSON):**

Debes responder con un objeto JSON con la siguiente estructura:
- quiebres_plan: array de objetos con qp_numero, fecha, area, equipo, razon, demora_horas, impacto, evidencia
- demoras: array de objetos con actividad, fecha, demora_horas, causa, responsable, impacto
- emergentes: array de objetos con actividad, prioridad, desplazo_a, ejecutor

Conversaciones:
{conversaciones}

Responde SOLO con el JSON válido, sin explicaciones adicionales ni bloques de código markdown."""

PROMPT_ANALISIS_ACTIVIDADES = """Eres un ingeniero de mantenimiento experto en minería.

Analiza y extrae TODAS las actividades de mantenimiento y operación mencionadas:

**INFORMACIÓN A EXTRAER:**

1. **ACTIVIDAD ESPECÍFICA**
   - Tipo: Preventivo, Correctivo, Predictivo, Mejora, Instalación, Desarme
   - Descripción técnica detallada
   - Sistema/Equipo afectado (con TAG si está disponible)

2. **UBICACIÓN EXACTA**
   - Planta: Concentradora, Hidrometalurgia, Mina, Infraestructura
   - Área específica: SPS-502, Chancador Primario, Sala Compresores
   - Nivel/Piso si aplica

3. **RECURSOS**
   - Personal: cantidad, empresa, especialidad
   - Equipos utilizados: grúas, andamios, herramientas
   - Materiales: repuestos, consumibles

4. **TIEMPOS**
   - Inicio programado vs real
   - Término programado vs real
   - Duración total
   - Horario/turno

5. **ESTADO**
   - Completado, En proceso, Detenido, Pendiente
   - % avance si se menciona
   - Próximos pasos

**FORMATO DE SALIDA:**
Responde con un objeto JSON que contenga un array "actividades" con objetos que tengan:
- id, tipo, descripcion
- equipo (con tag, nombre, sistema)
- ubicacion (planta, area, nivel)
- ejecutor (empresa, personal, supervisor)
- tiempos (inicio_programado, inicio_real, termino_programado, termino_real, demora_horas)
- estado, observaciones

Conversaciones:
{conversaciones}

Responde SOLO con el JSON válido, sin explicaciones adicionales ni bloques de código markdown."""

PROMPT_ANALISIS_SEGURIDAD = """Eres un especialista en seguridad y prevención de riesgos en minería.

Analiza las conversaciones y extrae TODA información relacionada con seguridad:

**CATEGORÍAS:**

1. **INCIDENTES/ACCIDENTES**
   - Tipo: Casi accidente, Incidente leve, Accidente con lesión, Daño material
   - Personas involucradas
   - Lesión/daño
   - Causa inmediata y raíz
   - Derivación médica

2. **HALLAZGOS/OBSERVACIONES DE SEGURIDAD**
   - Condiciones inseguras detectadas
   - Actos inseguros observados
   - No conformidades
   - Oportunidades de mejora

3. **PERMISOS Y AUTORIZACIONES**
   - SPCI (Sistema de Permisos de Trabajo)
   - Permisos especiales
   - Autorizaciones pendientes

4. **CONTROLES DE SEGURIDAD**
   - EPP utilizado/faltante
   - Señalización
   - Aislamiento/bloqueo (LOTO)
   - Charlas de seguridad

5. **COMPROMISOS Y ACCIONES**
   - Acciones correctivas definidas
   - Responsables
   - Plazos
   - Estado

**FORMATO DE SALIDA:**
Responde con un objeto JSON que contenga:
- incidentes: array con fecha, hora, tipo, descripcion, afectado, empresa, lesion, derivacion, causa_inmediata, causa_raiz, dias_perdidos
- hallazgos: array con fecha, tipo, descripcion, ubicacion, severidad, riesgo, detectado_por, accion_inmediata, estado
- permisos: array con tipo, actividad, ubicacion, estado, validez
- compromisos: array con accion, responsable, plazo, estado

Conversaciones:
{conversaciones}

Responde SOLO con el JSON válido, sin explicaciones adicionales ni bloques de código markdown."""

PROMPT_ANALISIS_PRODUCCION_KPI = """Eres un ingeniero de procesos experto en KPIs operacionales mineros.

Extrae TODOS los indicadores, métricas y datos de producción mencionados:

**INDICADORES A IDENTIFICAR:**

1. **PRODUCCIÓN**
   - Tonelaje procesado (ton/h, ton/día)
   - Caudales (m³/h, L/min, GPM)
   - Porcentaje de capacidad utilizada
   - Eficiencia operacional
   - Targets vs real

2. **PARÁMETROS DE PROCESO**
   - Presiones (bar, PSI, kPa)
   - Temperaturas (°C)
   - Niveles (%, m)
   - Concentraciones (g/L, ppm)
   - pH, conductividad
   - Velocidades (RPM, m/s)

3. **DISPONIBILIDAD Y CONFIABILIDAD**
   - Tiempo operativo
   - Tiempo detenido
   - Disponibilidad % (Uptime)
   - MTBF (tiempo medio entre fallas)
   - MTTR (tiempo medio de reparación)

4. **CONSUMOS**
   - Energía (kW, kWh, MW)
   - Agua (m³/h)
   - Combustible (L/h)
   - Reactivos (kg/h, ton/día)

5. **ESTADO DE EQUIPOS**
   - Operando normal
   - En mantenimiento
   - Detenido
   - En espera

**FORMATO DE SALIDA:**
Responde con un objeto JSON que contenga:
- produccion: array con equipo, parametro, valor, unidad, target, desviacion, desviacion_porcentaje, fecha, turno
- parametros_proceso: array con equipo, parametro, valor, unidad, rango_normal, estado, fecha
- disponibilidad: array con equipo, periodo, tiempo_operativo_h, tiempo_detenido_h, disponibilidad_porcentaje, target_porcentaje, causas_detencion
- consumos: array con area, parametro, valor, unidad, periodo, fecha

Conversaciones:
{conversaciones}

Responde SOLO con el JSON válido, sin explicaciones adicionales ni bloques de código markdown."""

# ----------------------------------------------------
# PROMPT FINAL DE SÍNTESIS
# ----------------------------------------------------

PROMPT_SINTESIS_FINAL = """Eres el Jefe de Operaciones de Minera Centinela con 20 años de experiencia en minería de cobre.

Has recibido análisis detallados de las últimas {periodo} de operación. Tu tarea es sintetizar esta información en un **Reporte Ejecutivo Técnico** de clase mundial.

**DATOS DE ENTRADA:**

{analisis_demoras}

{analisis_actividades}

{analisis_seguridad}

{analisis_produccion}

**ESTRUCTURA DEL REPORTE:**

Genera un reporte en Markdown con la siguiente estructura:

# Reporte Ejecutivo Técnico - Minera Centinela
**Período:** {periodo_texto}  
**Generado:** {fecha_generacion}

## 1. RESUMEN EJECUTIVO
Situación operacional general (2-3 párrafos), principales logros y desafíos, decisiones críticas requeridas.

## 2. ANÁLISIS DE CUMPLIMIENTO DE PLAN

### 2.1 Quiebres de Plan (QP)
Para cada QP identificado en el análisis, incluye:
- Número de QP
- Equipo/Sistema con TAG
- Horas perdidas e impacto
- Causa raíz técnica
- Acción correctiva con responsable

### 2.2 Demoras Operacionales
Crear tabla resumen de demoras con: Actividad, Demora (h), Causa, Impacto, Responsable
Incluir análisis de causas recurrentes.

### 2.3 Actividades Emergentes
Listar trabajos no programados, justificación e impacto en plan maestro.

## 3. EJECUCIÓN DE ACTIVIDADES

### Por Empresa Contratista:
Para cada empresa (AMECO, FTF, ELEVEN, ATLAS COPCO, etc):
- Trabajos ejecutados con ubicaciones específicas
- Equipos utilizados con TAGs
- Problemas/Hallazgos
- Estado de avance

### Matriz de Actividades por Área:
Tabla con: Área, Actividades, Horas-Hombre, Empresa, Estado

## 4. SEGURIDAD Y MEDIO AMBIENTE

### 4.1 Incidentes
Para cada incidente: descripción técnica, análisis de causas, acciones correctivas, responsables y plazos.

### 4.2 Hallazgos de Seguridad
Listar condiciones inseguras detectadas, nivel de riesgo, acciones tomadas.

### 4.3 Compromisos Pendientes
Tabla: Compromiso, Responsable, Plazo, Estado

### 4.4 Indicadores
Calcular frecuencia de incidentes y tendencia.

## 5. INDICADORES OPERACIONALES

### 5.1 Producción
Tabla de KPIs: Indicador, Real, Target, Desviación, Estado (🔴/🟡/🟢)

### 5.2 Disponibilidad de Equipos Críticos
Tabla: Equipo, TAG, Disponibilidad Real vs Target, Causa principal detención

### 5.3 Parámetros Fuera de Rango
Listar variables que excedieron límites, impacto y acciones.

## 6. ANÁLISIS DE TENDENCIAS

### 6.1 Equipos con Fallas Recurrentes
Identificar equipos con múltiples fallas, analizar patrón.

### 6.2 Áreas con Mayor Actividad
Ranking por horas-hombre, justificación.

## 7. RECOMENDACIONES Y ACCIONES

### Corto Plazo (1-7 días)
Lista numerada de acciones concretas con responsable y plazo.

### Mediano Plazo (1-4 semanas)
Mejoras de proceso/sistema.

### Largo Plazo (>1 mes)
Inversiones/proyectos.

## 8. ANEXOS

### Anexo A: Evidencia Fotográfica/Video
Lista de archivos adjuntos mencionados en conversaciones.

### Anexo B: Detalle Técnico
Procedimientos y especificaciones técnicas relevantes.

---

**INSTRUCCIONES DE FORMATO:**
- Usa Markdown profesional con tablas
- Negrita para alertas críticas
- Código para TAGs de equipos (ejemplo: `P-101`)
- Emojis para estados: 🔴 Crítico, 🟡 Advertencia, 🟢 OK
- Números exactos siempre que estén disponibles
- Si falta información, indicar "No reportado"
- Prioriza información accionable

**TONO:** Técnico, directo, basado en datos, orientado a toma de decisiones.

Genera el reporte completo ahora:"""

def generate_advanced_technical_report(messages: list, groups_data: dict, periodo_texto: str) -> str:
    """
    Genera reporte técnico avanzado usando análisis multi-pasada con Claude.
    
    Args:
        messages: Lista de mensajes procesados
        groups_data: Datos agrupados por empresa
        periodo_texto: Descripción del período
        
    Returns:
        Reporte en formato Markdown
    """
    
    # Preparar conversaciones
    conversaciones = format_messages_for_context(messages, max_chars=50000)
    
    print("\n🔬 ANÁLISIS TÉCNICO AVANZADO EN MÚLTIPLES PASADAS")
    print("="*70)
    
    # PASADA 1: Análisis de demoras y QP
    print("📊 Pasada 1/4: Analizando demoras y quiebres de plan...")
    analisis_demoras_json = call_claude_analysis(
        PROMPT_ANALISIS_DEMORAS_QP.format(conversaciones=conversaciones)
    )
    
    # PASADA 2: Análisis de actividades
    print("🔧 Pasada 2/4: Analizando actividades y ubicaciones...")
    analisis_actividades_json = call_claude_analysis(
        PROMPT_ANALISIS_ACTIVIDADES.format(conversaciones=conversaciones)
    )
    
    # PASADA 3: Análisis de seguridad
    print("🛡️ Pasada 3/4: Analizando seguridad y hallazgos...")
    analisis_seguridad_json = call_claude_analysis(
        PROMPT_ANALISIS_SEGURIDAD.format(conversaciones=conversaciones)
    )
    
    # PASADA 4: Análisis de producción y KPIs
    print("📈 Pasada 4/4: Analizando producción e indicadores...")
    analisis_produccion_json = call_claude_analysis(
        PROMPT_ANALISIS_PRODUCCION_KPI.format(conversaciones=conversaciones)
    )
    
    # SÍNTESIS FINAL
    print("📝 Síntesis final: Generando reporte ejecutivo...")
    reporte_final = call_claude_synthesis(
        PROMPT_SINTESIS_FINAL.format(
            periodo=periodo_texto,
            periodo_texto=periodo_texto,
            fecha_generacion=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            analisis_demoras=format_json_for_prompt(analisis_demoras_json, "Demoras y QP"),
            analisis_actividades=format_json_for_prompt(analisis_actividades_json, "Actividades"),
            analisis_seguridad=format_json_for_prompt(analisis_seguridad_json, "Seguridad"),
            analisis_produccion=format_json_for_prompt(analisis_produccion_json, "Producción")
        )
    )
    
    print("✅ Análisis técnico completado")
    print("="*70 + "\n")
    
    return reporte_final

def call_claude_analysis(prompt: str, max_tokens: int = 4000) -> dict:
    """
    Llama a Claude para análisis y retorna JSON parseado.
    """
    try:
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=max_tokens,
            temperature=0.1,  # Más determinístico para análisis técnico
            messages=[{"role": "user", "content": prompt}]
        )
        
        content = response.content[0].text
        
        # Extraer JSON del response (puede venir con ```json wrapper)
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]
        
        return json.loads(content.strip())
        
    except Exception as e:
        print(f"⚠️ Error en análisis: {e}")
        return {}

def call_claude_synthesis(prompt: str, max_tokens: int = 8000) -> str:
    """
    Llama a Claude para síntesis final del reporte.
    """
    try:
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=max_tokens,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return response.content[0].text
        
    except Exception as e:
        print(f"❌ Error en síntesis: {e}")
        return None

def format_json_for_prompt(data: dict, title: str) -> str:
    """
    Formatea JSON de análisis para incluir en prompt de síntesis.
    """
    if not data:
        return f"## {title}\nNo se identificó información relevante en esta categoría.\n"
    
    return f"## {title}\n```json\n{json.dumps(data, indent=2, ensure_ascii=False)}\n```\n"
