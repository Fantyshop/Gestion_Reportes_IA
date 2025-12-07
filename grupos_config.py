"""
Catálogo de Grupos de WhatsApp y sus Empresas Asociadas
Minera Centinela - Soporte a la Operación (GSdSO)
"""

GRUPOS_EMPRESAS = {
    1: {
        "nombre": "Operativa AMECO - CENT",
        "empresa": "AMECO",
        "servicios": [
            "Arriendo de grúas de izaje",
            "Equipos de apoyo: grúas horquillas",
            "Camiones plumas",
            "Man-lift / Alzahombres"
        ],
        "tipo_servicio": "Equipos de Izaje y Apoyo Pesado",
        "frecuencia_reporte": "Diario",
        "keywords": ["grúa", "izaje", "horquilla", "pluma", "man-lift", "alzahombre", "levante"]
    },
    2: {
        "nombre": "FTF - CENTINELA FIJO-SPOT",
        "empresa": "FTF",
        "servicios": [
            "Andamios",
            "Montaje de estructuras",
            "Traslado de material",
            "Armado y desarmado de andamios"
        ],
        "tipo_servicio": "Andamiaje y Montaje",
        "frecuencia_reporte": "Diario",
        "keywords": ["andamio", "montaje", "desmontaje", "armado", "desarme", "estructura", "scaffold"]
    },
    3: {
        "nombre": "Operativa ELEVEN - CENT",
        "empresa": "ELEVEN",
        "servicios": [
            "Arriendo de equipos de apoyo menor",
            "Luminarias",
            "Generadores eléctricos",
            "Compresores"
        ],
        "tipo_servicio": "Equipos de Apoyo Menor",
        "frecuencia_reporte": "Diario",
        "keywords": ["luminaria", "generador", "compresor", "iluminación", "energía", "aire comprimido"]
    },
    4: {
        "nombre": "Información Atlas Copco",
        "empresa": "ATLAS COPCO",
        "servicios": [
            "Mantenimiento de compresores",
            "Mantenimiento de generadores",
            "Servicio transversal multi-capacidad"
        ],
        "tipo_servicio": "Mantenimiento Especializado de Equipos",
        "frecuencia_reporte": "Diario",
        "keywords": ["atlas copco", "compresor", "generador", "mantenimiento", "servicio", "reparación"]
    },
    5: {
        "nombre": "Plantas RO",
        "empresa": "SERVILOG",
        "servicios": [
            "Operación de plantas de osmosis inversa",
            "Mantenimiento de sistemas RO",
            "Tratamiento de agua",
            "Gestión de derivados de osmosis"
        ],
        "tipo_servicio": "Operación y Mantenimiento de Tratamiento de Agua",
        "frecuencia_reporte": "Diario",
        "keywords": ["osmosis", "ro", "agua", "planta", "tratamiento", "servilog", "membrana", "permeado"]
    },
    6: {
        "nombre": "Info Equans",
        "empresa": "EQUANS",
        "servicios": [
            "Mantenimiento de aire acondicionado",
            "Sistemas de refrigeración",
            "Climatización",
            "HVAC (Heating, Ventilation, Air Conditioning)"
        ],
        "tipo_servicio": "Climatización y Refrigeración",
        "frecuencia_reporte": "Diario",
        "keywords": ["aire acondicionado", "refrigeración", "hvac", "climatización", "enfriamiento", "equans"]
    },
    7: {
        "nombre": "Contrato Lavado CSP 1097",
        "empresa": "ELECMAIN",
        "servicios": [
            "Mantenimiento de líneas eléctricas alta tensión",
            "Lavado de líneas AT",
            "Mantenimiento de portales eléctricos",
            "Mantenimiento de transformadores",
            "Accesorios eléctricos AT"
        ],
        "tipo_servicio": "Mantenimiento Eléctrico Alta Tensión",
        "frecuencia_reporte": "Diario",
        "keywords": ["línea eléctrica", "alta tensión", "at", "transformador", "portal", "lavado", "elecmain"]
    }
}

def get_grupo_info(grupo_id: int) -> dict:
    """
    Obtiene la información de un grupo por su ID.
    
    Args:
        grupo_id: ID del grupo en la base de datos
        
    Returns:
        Diccionario con información del grupo o None si no existe
    """
    return GRUPOS_EMPRESAS.get(grupo_id)

def get_grupo_context(grupo_id: int) -> str:
    """
    Genera un contexto textual para un grupo específico.
    Útil para incluir en prompts de IA.
    
    Args:
        grupo_id: ID del grupo
        
    Returns:
        String con contexto formateado
    """
    info = get_grupo_info(grupo_id)
    
    if not info:
        return f"Grupo ID {grupo_id} (información no disponible)"
    
    context = f"""**{info['nombre']}**
- Empresa: {info['empresa']}
- Tipo de servicio: {info['tipo_servicio']}
- Servicios principales: {', '.join(info['servicios'][:3])}"""
    
    return context

def classify_message_by_keywords(mensaje: str) -> list:
    """
    Clasifica un mensaje según las keywords de cada grupo.
    Útil para identificar temas cross-grupo.
    
    Args:
        mensaje: Texto del mensaje
        
    Returns:
        Lista de IDs de grupos relacionados
    """
    mensaje_lower = mensaje.lower()
    grupos_relacionados = []
    
    for grupo_id, info in GRUPOS_EMPRESAS.items():
        for keyword in info['keywords']:
            if keyword in mensaje_lower:
                grupos_relacionados.append(grupo_id)
                break
    
    return grupos_relacionados

def get_all_empresas() -> list:
    """
    Retorna lista de todas las empresas únicas.
    
    Returns:
        Lista de nombres de empresas
    """
    return list(set([info['empresa'] for info in GRUPOS_EMPRESAS.values()]))

def get_summary_all_grupos() -> str:
    """
    Genera un resumen de todos los grupos activos.
    
    Returns:
        String con resumen formateado
    """
    summary_parts = ["GRUPOS ACTIVOS EN MONITOREO:\n"]
    
    for grupo_id, info in GRUPOS_EMPRESAS.items():
        summary_parts.append(f"{grupo_id}. {info['nombre']} ({info['empresa']}) - {info['tipo_servicio']}")
    
    return "\n".join(summary_parts)

# Información adicional de contexto minero
CONTEXTO_MINERA_CENTINELA = """
**Minera Centinela** es una operación de Antofagasta Minerals ubicada en la Región de Antofagasta, Chile.

**GSdSO (Gestión de Sistemas de Operación)** es el área responsable de:
- Gestión de sistemas de operación y mantenimiento
- Coordinación de servicios con empresas contratistas
- Monitoreo de equipos críticos
- Soporte a operaciones de concentradora, hidrometalurgia y servicios mina

**Áreas operacionales principales:**
- Concentradora (procesamiento de mineral sulfurado)
- Hidrometalurgia (procesamiento de mineral oxidado - SX-EW)
- Servicios Mina (equipos móviles, infraestructura)
- Plantas de Agua (RO, tratamiento, distribución)
- Infraestructura Eléctrica (alta tensión, distribución)
"""
