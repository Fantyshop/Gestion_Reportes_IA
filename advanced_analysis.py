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

Extrae ÚNICAMENTE los indicadores, métricas y datos que estén **EXPLÍCITAMENTE MENCIONADOS** en las conversaciones.

**REGLA CRÍTICA: NO ASUMIR NI INVENTAR TARGETS**
- Solo reporta targets si están claramente mencionados en el texto
- Si no hay target explícito, deja el campo como null o "No reportado"
- No calcules desviaciones si no hay target mencionado
- No asumas rangos normales no especificados

**INDICADORES A IDENTIFICAR (solo si están presentes):**

1. **PRODUCCIÓN**
   - Tonelaje procesado (ton/h, ton/día)
   - Caudales (m³/h, L/min, GPM)
   - Porcentaje de capacidad utilizada
   - Eficiencia operacional
   - Target SOLO si se menciona explícitamente

2. **PARÁMETROS DE PROCESO**
   - Presiones (bar, PSI, kPa)
   - Temperaturas (°C)
   - Niveles (%, m)
   - Concentraciones (g/L, ppm)
   - pH, conductividad
   - Velocidades (RPM, m/s, Hz)
   - Frecuencias (Hz)

3. **DISPONIBILIDAD Y CONFIABILIDAD**
   - Tiempo operativo (solo si se menciona)
   - Tiempo detenido (solo si se menciona)
   - Disponibilidad % (solo si se reporta)
   - Causas de detención mencionadas

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
   - Fuera de servicio

**FORMATO DE SALIDA:**
Responde con un objeto JSON. Para cada campo de "target", "rango_normal", o "desviacion":
- Si NO está mencionado explícitamente: usa null o "No reportado"
- Si SÍ está mencionado: incluye el valor exacto

Estructura JSON:
- produccion: array con:
  * equipo, parametro, valor, unidad
  * target: (null si no se menciona)
  * desviacion: (null si no hay target)
  * desviacion_porcentaje: (null si no hay target)
  * fecha, turno
  
- parametros_proceso: array con:
  * equipo, parametro, valor, unidad
  * rango_normal: (null si no se menciona)
  * estado: ("normal", "fuera de rango", "crítico" solo si se indica)
  * fecha
  
- disponibilidad: array con:
  * equipo, periodo
  * tiempo_operativo_h: (null si no se menciona)
  * tiempo_detenido_h: (null si no se menciona)
  * disponibilidad_porcentaje: (null si no se calcula)
  * target_porcentaje: (null si no se menciona)
  * causas_detencion: (lista de causas mencionadas)
  
- consumos: array con:
  * area, parametro, valor, unidad, periodo, fecha

Conversaciones:
{conversaciones}

Responde SOLO con el JSON válido, sin explicaciones adicionales ni bloques de código markdown."""

# ----------------------------------------------------
# PROMPT FINAL DE SÍNTESIS
# ----------------------------------------------------

PROMPT_SINTESIS_FINAL = """Eres un analista técnico especializado en reportes operacionales mineros.

Tu tarea: Sintetizar los análisis detallados en un **Reporte Ejecutivo Técnico** EXHAUSTIVO Y DETALLADO.

**🚨 REGLAS CRÍTICAS - LEE ANTES DE GENERAR:**

1. **NO INVENTES TARGETS**: Si un target NO está explícito en los datos, NO lo incluyas. NO uses columnas "Target" ni "Desviación" a menos que estén en los datos.
2. **DETALLE MÁXIMO**: Cada tabla debe tener contexto completo - nombres, TAGs, fechas, horas, empresas, usuarios
3. **TRAZABILIDAD**: Identifica QUIÉN reportó cada evento (busca nombres de usuarios/remitentes en datos)
4. **NO MATRICES INÚTILES**: ELIMINA la "Matriz de Actividades por Superintendencia" - no aporta valor
5. **ARCHIVOS ADJUNTOS**: Lista TODOS los PDFs, imágenes, documentos mencionados con sus nombres exactos

**DATOS DE ENTRADA:**

{analisis_demoras}

{analisis_actividades}

{analisis_seguridad}

{analisis_produccion}

---

# Reporte Ejecutivo Técnico - Minera Centinela
**Período:** {periodo_texto}  
**Generado:** {fecha_generacion}

## 1. RESUMEN EJECUTIVO

3-4 párrafos con:
- Situación operacional general (usa NÚMEROS ESPECÍFICOS)
- Logros cuantificados
- Desafíos críticos con impacto medible
- Decisiones requeridas con plazo

---

## 2. ANÁLISIS DE CUMPLIMIENTO DE PLAN

### 2.1 Quiebres de Plan (QP)

**Si hay QPs, usa esta tabla:**
| QP Número | Área | Fecha/Hora | Equipo/TAG | Horas Perdidas | Causa Raíz Específica | Impacto Cuantificado | Responsable | Reportado por | Estado |

**Si NO hay QPs explícitos:** Indicar claramente "No se reportaron Quiebres de Plan formalizados en el período analizado"

### 2.2 Demoras Operacionales

**TABLA CON MÁXIMO DETALLE:**
| Actividad Completa (incluir: nombre trabajo + equipo/TAG + ubicación específica + contexto) | Demora (horas exactas) | Causa Raíz Detallada | Impacto Cuantificado | Empresa/Responsable/Usuario | Fecha/Hora |

**EJEMPLO DE DETALLE REQUERIDO:**
✅ CORRECTO: "Cambio motor doble eje `762-ER-001` ubicado en sala eléctrica SSEE sector norte, requiere desconexión red contra incendio por procedimiento seguridad"
❌ INCORRECTO: "Cambio motor"

Incluir análisis de causas recurrentes con porcentajes calculados.

### 2.3 Actividades Emergentes

**FORMATO DETALLADO OBLIGATORIO:**

Para CADA actividad emergente:

**[Número]. [Nombre Actividad con TAG/ubicación]**
- **Descripción completa:** [Qué se hizo exactamente]
- **Actividad programada desplazada:** [Qué trabajo se tuvo que posponer]
- **Recursos utilizados:** [Cantidad personas + empresa + especialidades + equipos + HH totales]
- **Justificación urgencia:** [Por qué no podía esperar]
- **Empresa ejecutora:** [Nombre empresa]
- **Supervisor/Responsable:** [Nombre persona]
- **Reportado por:** [Usuario que levantó]
- **Fecha/Hora:** [Timestamp exacto]
- **Impacto en plan maestro:** [Cuantificado]

---

## 3. EJECUCIÓN DE ACTIVIDADES

**NIVEL DE DETALLE EXHAUSTIVO REQUERIDO**

### SUPERINTENDENCIA: SERVICIOS TRANSVERSALES (SSTT)

#### AMECO - Equipos de Izaje

**Trabajos Ejecutados:**
Para CADA trabajo:
- Nombre trabajo + TAG equipo + ubicación exacta (Planta/Área/Nivel/Coordenadas)
- Fecha/hora inicio - Fecha/hora término
- Personal (cantidad + nombres si disponible)
- Equipos utilizados (TAGs específicos)
- Procedimiento aplicado
- Estado final (completado %, pendientes)
- Observaciones técnicas

**Equipos Utilizados:**
Lista de equipos con:
- TAG
- Tipo/Capacidad
- Actividad en que se usó
- Estado operacional
- Problemas detectados

**Problemas/Incidentes:**
Para CADA problema:
- Descripción técnica completa
- Causa raíz si se conoce
- Impacto (cuantificado)
- Acción correctiva tomada
- Responsable
- Estado actual

**Reportado por:** [Usuarios que enviaron información]

[REPETIR MISMO NIVEL DE DETALLE para: FTF, ELEVEN, ATLAS COPCO, EQUANS]

### SUPERINTENDENCIA: INSUMOS ESTRATÉGICOS (IIEE)

#### SERVILOG - Plantas RO

**Producción Registrada:**
- Turno día [fecha]: Moly XX m³, Sulfuro YY m³
- Turno noche [fecha]: Moly XX m³, Sulfuro YY m³
[Para cada turno reportado]

**Parámetros Operacionales Registrados:**
Para CADA equipo mencionado:
- TAG: `UF-A Moly`
  - Caudal: XX m³/h (fecha/turno)
  - Presión: YY bar
  - Frecuencia: ZZ Hz
  - Temperatura: WW °C
  - Observaciones

**Trabajos Ejecutados:** [Mismo detalle que AMECO]

**Fallas/Problemas:** [Mismo detalle que AMECO]

**Reportado por:** [Usuarios]

[REPETIR para ELECMAIN]

**🚫 NO INCLUIR "Matriz de Actividades por Superintendencia" - ELIMINAR ESA SECCIÓN**

---

## 4. SEGURIDAD Y MEDIO AMBIENTE

### 4.1 Incidentes

**TABLA COMPLETA:**
| Fecha/Hora Exacta | Tipo | Descripción Técnica Detallada | Afectado (Nombre Completo) | Empresa del Afectado | Reportado por (Usuario/Remitente) | Causa Raíz | Acción Correctiva | Días Perdidos | Estado |

**IMPORTANTE:** SIEMPRE incluir quién reportó (buscar en remitente de mensajes)

### 4.2 Hallazgos de Seguridad

**TABLA:**
| Fecha/Hora | Descripción Específica del Hallazgo | Ubicación Exacta (Planta/Área/TAG) | Empresa Responsable Área | Detectado/Reportado por (Usuario) | Severidad | Riesgo Específico | Acción Inmediata Tomada | Estado Actual |

### 4.3 Compromisos Pendientes

**TABLA CON CONTEXTO:**
| Compromiso (descripción completa) | Responsable (Nombre + Cargo + Empresa) | Plazo Específico | Origen del Compromiso (qué evento/incidente lo generó) | Estado |

### 4.4 Indicadores

- Frecuencia incidentes: [número] eventos en [horas] horas = [número] incidentes/día
- Tendencia: [porcentajes por tipo]
- Días perdidos totales: [número]
- Análisis por tipo de incidente

---

## 5. INDICADORES OPERACIONALES

### 5.1 Producción

**🚨 REGLA: NO INCLUIR COLUMNAS "TARGET" NI "DESVIACIÓN" A MENOS QUE ESTÉN EXPLÍCITAS EN LOS DATOS**

**TABLA SIMPLIFICADA (usar siempre):**
| Equipo/TAG | Parámetro | Valor Real | Unidad | Fecha/Turno | Observaciones Técnicas |

**EJEMPLO:**
| `UF-A Moly` | Caudal | 68 | m³/h | 09/12 Turno Día | Operando bajo frecuencia nominal (40 Hz vs diseño 49 Hz) |

**SOLO SI** el target está explícito en datos, agregar columnas:
| Equipo/TAG | Parámetro | Valor Real | Target Reportado | Desviación | Unidad | Fecha/Turno |

### 5.2 Disponibilidad de Equipos Críticos

**🚨 NO INCLUIR COLUMNA "TARGET" NI "DISPONIBILIDAD %"**

**TABLA SIMPLIFICADA:**
| Equipo/TAG | Tiempo Operativo (h) | Tiempo Detenido (h) | Causa Principal Detención Detallada | Empresa Responsable | Reportado por |

### 5.3 Parámetros Fuera de Rango

**SOLO listar si hay rango normal mencionado EXPLÍCITAMENTE**

Si no hay rangos: "No se reportaron rangos normales de operación para comparación"

---

## 6. ANÁLISIS DE TENDENCIAS

### 6.1 Equipos con Fallas Recurrentes

Lista numerada:
1. [Equipo/Sistema]: [Patrón identificado] - [Frecuencia] - [Impacto acumulado] - [Acción sugerida]

### 6.2 Áreas con Mayor Actividad

Ranking:
1. [Área]: [Cantidad trabajos] - [Descripción actividades principales] - [Empresas involucradas]

---

## 7. RECOMENDACIONES Y ACCIONES

**FORMATO CON CONTEXTO COMPLETO OBLIGATORIO:**

### Corto Plazo (1-7 días)

**[Número]. [Título Acción]**
- **Contexto del problema:** [Descripción detallada del evento/hallazgo/demora que origina esta acción - incluir fecha, equipo, impacto]
- **Acción específica requerida:** [Qué hacer exactamente - pasos concretos]
- **Responsable:** [Nombre completo + Cargo + Empresa]
- **Plazo específico:** [Fecha exacta]
- **Justificación urgencia:** [Por qué es crítico hacerlo ahora - consecuencias de no hacerlo]
- **Origen:** [Incidente/Hallazgo/Demora específico que lo causó con referencia a sección del reporte]
- **Reportado/Escalado por:** [Usuario que levantó el tema]

### Mediano Plazo (1-4 semanas)

[Mismo formato con contexto completo]

### Largo Plazo (>1 mes)

[Mismo formato con contexto completo]

---

## 8. ANEXOS

### Anexo A: Archivos y Evidencia Documental Analizada

**LISTAR TODOS LOS ARCHIVOS MENCIONADOS EN LOS DATOS:**

**PDFs Analizados:**
- [nombre exacto archivo].pdf - [Descripción breve contenido]
- [nombre].pdf - [Descripción]

**Imágenes Analizadas:**
- [descripción imagen] - [Qué muestra] - [Hallazgos visuales]
- [descripción] - [Contenido]

**Documentos Excel/CSV:**
- [nombre archivo] - [Tipo datos]

**Videos:**
- [descripción] - [Contenido]

**URLs Supabase Storage (si disponibles):**
- [URL] - [Archivo]

### Anexo B: Detalle Técnico

Especificaciones, procedimientos, análisis metalúrgicos, protocolos mencionados en el análisis.

---

**FIRMA DEL REPORTE:**

---

**Reporte Generado Automáticamente por Sistema de Inteligencia Artificial**

Basado en análisis de comunicaciones operacionales mediante:
- **Vectorización:** OpenAI text-embedding-3-small (1,536 dimensiones)
- **Análisis Multi-pasada:** Anthropic Claude Sonnet 4
  - Pasada 1: Demoras y Quiebres de Plan
  - Pasada 2: Actividades y Ubicaciones
  - Pasada 3: Seguridad y Medio Ambiente
  - Pasada 4: Producción e Indicadores Operacionales
  - Pasada 5: Síntesis Ejecutiva
- **Período analizado:** {periodo_texto}
- **Mensajes procesados:** [Indicar cantidad si disponible]

**Generado:** {fecha_generacion}  
**Próxima actualización automática:** [fecha + 168 horas]

**⚠️ IMPORTANTE:** Este reporte requiere validación humana antes de distribución formal a gerencia. 

**Contacto Técnico:**  
GSdSO - Gerencia de Servicio de Soporte a la Operación  
Minera Centinela - Antofagasta Minerals

---

**INSTRUCCIONES FINALES:**
- Markdown profesional
- **Negrita** para críticos
- `Código` para TAGs
- 🔴🟡🟢 para estados
- Números EXACTOS
- "No reportado" si falta
- NO inventar
- MÁXIMO DETALLE en TODAS las tablas

Genera el reporte ahora:"""

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
