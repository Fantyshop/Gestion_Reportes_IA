"""
Convertidor de Reporte Markdown a HTML Visual
Minera Centinela - GSdSO
Mantiene el contenido técnico, mejora la presentación visual
"""

import re
from datetime import datetime
import markdown
from bs4 import BeautifulSoup

def convert_report_to_html(markdown_content: str, periodo_texto: str) -> str:
    """
    Convierte el reporte markdown a HTML con estilo corporativo Antofagasta Minerals.
    Mantiene TODO el contenido técnico, solo mejora la presentación.
    """
    
    # Convertir markdown a HTML básico
    html_body = markdown.markdown(
        markdown_content,
        extensions=['tables', 'fenced_code', 'nl2br']
    )
    
    # Procesar con BeautifulSoup para mejorar estructura
    soup = BeautifulSoup(html_body, 'html.parser')
    
    # Agregar clases CSS a elementos
    enhance_tables(soup)
    enhance_headers(soup)
    enhance_lists(soup)
    
    html_body_str = str(soup)
    
    # Template HTML completo
    html = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte Ejecutivo - Minera Centinela</title>
    
    <style>
        /* ========================================
           ESTILOS CORPORATIVOS ANTOFAGASTA MINERALS
           ======================================== */
        
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        :root {{
            --color-primary: #4A9FA5;        /* Teal corporativo */
            --color-primary-dark: #3A7F85;
            --color-secondary: #F7941D;      /* Naranja acento */
            --color-success: #28a745;
            --color-warning: #ffc107;
            --color-danger: #dc3545;
            --color-dark: #2C3E50;
            --color-light: #F8F9FA;
            --color-white: #FFFFFF;
            --shadow-sm: 0 2px 4px rgba(0,0,0,0.1);
            --shadow-md: 0 4px 8px rgba(0,0,0,0.15);
        }}
        
        body {{
            font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
            background: #E9ECEF;
            color: var(--color-dark);
            line-height: 1.6;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: var(--color-white);
            box-shadow: 0 0 30px rgba(0,0,0,0.1);
        }}
        
        /* HEADER CORPORATIVO */
        .header {{
            background: linear-gradient(135deg, var(--color-primary) 0%, var(--color-primary-dark) 100%);
            color: var(--color-white);
            padding: 40px 50px;
            border-bottom: 5px solid var(--color-secondary);
        }}
        
        .header-top {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }}
        
        .header h1 {{
            font-size: 2.2em;
            font-weight: 700;
            margin: 0;
        }}
        
        .header-icon {{
            font-size: 3em;
        }}
        
        .header-subtitle {{
            font-size: 1.1em;
            opacity: 0.95;
            margin: 10px 0;
        }}
        
        .header-meta {{
            display: flex;
            gap: 30px;
            margin-top: 20px;
            flex-wrap: wrap;
        }}
        
        .meta-item {{
            background: rgba(255,255,255,0.15);
            padding: 10px 20px;
            border-radius: 8px;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        
        .meta-item strong {{
            font-weight: 600;
        }}
        
        /* CONTENIDO */
        .content {{
            padding: 50px;
        }}
        
        /* HEADINGS */
        h2 {{
            color: var(--color-primary);
            font-size: 1.8em;
            margin: 40px 0 25px 0;
            padding-bottom: 15px;
            border-bottom: 3px solid var(--color-primary);
            display: flex;
            align-items: center;
            gap: 15px;
        }}
        
        h3 {{
            color: var(--color-primary-dark);
            font-size: 1.4em;
            margin: 30px 0 20px 0;
            padding-left: 15px;
            border-left: 4px solid var(--color-secondary);
        }}
        
        h4 {{
            color: var(--color-dark);
            font-size: 1.2em;
            margin: 25px 0 15px 0;
            font-weight: 600;
        }}
        
        /* TABLAS PROFESIONALES */
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 25px 0;
            background: var(--color-white);
            box-shadow: var(--shadow-sm);
            border-radius: 8px;
            overflow: hidden;
        }}
        
        thead {{
            background: var(--color-primary);
            color: var(--color-white);
        }}
        
        thead th {{
            padding: 15px 12px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
        }}
        
        tbody td {{
            padding: 12px;
            border-bottom: 1px solid #DEE2E6;
        }}
        
        tbody tr:nth-child(even) {{
            background: var(--color-light);
        }}
        
        tbody tr:hover {{
            background: #E3F2FD;
        }}
        
        tbody tr:last-child td {{
            border-bottom: none;
        }}
        
        /* BADGES DE ESTADO */
        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
            white-space: nowrap;
        }}
        
        .badge-success {{
            background: #D4EDDA;
            color: #155724;
        }}
        
        .badge-warning {{
            background: #FFF3CD;
            color: #856404;
        }}
        
        .badge-danger {{
            background: #F8D7DA;
            color: #721C24;
        }}
        
        .badge-info {{
            background: #D1ECF1;
            color: #0C5460;
        }}
        
        /* PÁRRAFOS Y TEXTO */
        p {{
            margin: 15px 0;
            line-height: 1.8;
        }}
        
        strong {{
            color: var(--color-primary-dark);
            font-weight: 600;
        }}
        
        code {{
            background: var(--color-light);
            padding: 2px 6px;
            border-radius: 4px;
            font-family: 'Courier New', monospace;
            color: var(--color-danger);
            font-size: 0.9em;
        }}
        
        /* LISTAS */
        ul, ol {{
            margin: 15px 0 15px 30px;
        }}
        
        li {{
            margin: 8px 0;
            line-height: 1.6;
        }}
        
        /* CARDS DE SECCIÓN */
        .section-card {{
            background: var(--color-white);
            border: 1px solid #DEE2E6;
            border-left: 4px solid var(--color-primary);
            border-radius: 8px;
            padding: 25px;
            margin: 25px 0;
            box-shadow: var(--shadow-sm);
        }}
        
        .section-card.warning {{
            border-left-color: var(--color-warning);
        }}
        
        .section-card.danger {{
            border-left-color: var(--color-danger);
        }}
        
        /* BLOCKQUOTES */
        blockquote {{
            border-left: 4px solid var(--color-secondary);
            padding-left: 20px;
            margin: 20px 0;
            font-style: italic;
            color: #6C757D;
        }}
        
        /* FOOTER */
        .footer {{
            background: var(--color-dark);
            color: var(--color-white);
            padding: 30px 50px;
            text-align: center;
            border-top: 5px solid var(--color-secondary);
        }}
        
        .footer p {{
            margin: 8px 0;
            opacity: 0.9;
        }}
        
        .footer-brand {{
            font-size: 1.2em;
            font-weight: 700;
            margin-bottom: 10px;
        }}
        
        /* RESPONSIVE */
        @media (max-width: 768px) {{
            body {{
                padding: 10px;
            }}
            
            .header, .content, .footer {{
                padding: 30px 20px;
            }}
            
            .header h1 {{
                font-size: 1.6em;
            }}
            
            .header-meta {{
                gap: 15px;
            }}
            
            h2 {{
                font-size: 1.5em;
            }}
            
            table {{
                font-size: 0.9em;
            }}
            
            thead th, tbody td {{
                padding: 8px 6px;
            }}
        }}
        
        /* PRINT STYLES */
        @media print {{
            body {{
                background: white;
                padding: 0;
            }}
            
            .container {{
                box-shadow: none;
            }}
            
            .header {{
                background: var(--color-primary) !important;
                -webkit-print-color-adjust: exact;
                print-color-adjust: exact;
            }}
            
            h2 {{
                page-break-after: avoid;
            }}
            
            table {{
                page-break-inside: avoid;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- HEADER -->
        <div class="header">
            <div class="header-top">
                <div>
                    <h1>📊 Reporte Ejecutivo Técnico</h1>
                    <div class="header-subtitle">Minera Centinela - Gestión de Sistemas de Operación (GSdSO)</div>
                </div>
                <div class="header-icon">⚙️</div>
            </div>
            <div class="header-meta">
                <div class="meta-item">
                    <span>📅</span>
                    <div><strong>Período:</strong> {periodo_texto}</div>
                </div>
                <div class="meta-item">
                    <span>🕐</span>
                    <div><strong>Generado:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</div>
                </div>
                <div class="meta-item">
                    <span>🏢</span>
                    <div><strong>Cliente:</strong> Antofagasta Minerals</div>
                </div>
            </div>
        </div>
        
        <!-- CONTENIDO -->
        <div class="content">
            {html_body_str}
        </div>
        
        <!-- FOOTER -->
        <div class="footer">
            <div class="footer-brand">Minera Centinela</div>
            <p>Antofagasta Minerals - Gestión de Sistemas de Operación (GSdSO)</p>
            <p>Sistema de Reportes Automatizados con Inteligencia Artificial</p>
            <p style="margin-top: 15px; font-size: 0.9em; opacity: 0.7;">
                Este reporte fue generado automáticamente basado en análisis de comunicaciones operacionales
            </p>
        </div>
    </div>
</body>
</html>
"""
    
    return html

def enhance_tables(soup):
    """Mejora las tablas con clases CSS"""
    for table in soup.find_all('table'):
        table['class'] = 'data-table'
        
        # Procesar celdas con emojis de estado
        for td in table.find_all('td'):
            text = td.get_text()
            
            # Detectar badges de estado
            if '🟢' in text or 'Normal' in text or 'Completado' in text:
                if not td.find('span', class_='badge'):
                    td.string = ''
                    badge = soup.new_tag('span', **{'class': 'badge badge-success'})
                    badge.string = text
                    td.append(badge)
            
            elif '🟡' in text or 'Medio' in text or 'En proceso' in text or 'Advertencia' in text:
                if not td.find('span', class_='badge'):
                    td.string = ''
                    badge = soup.new_tag('span', **{'class': 'badge badge-warning'})
                    badge.string = text
                    td.append(badge)
            
            elif '🔴' in text or 'Crítico' in text or 'Alto' in text or 'Vencido' in text:
                if not td.find('span', class_='badge'):
                    td.string = ''
                    badge = soup.new_tag('span', **{'class': 'badge badge-danger'})
                    badge.string = text
                    td.append(badge)

def enhance_headers(soup):
    """Agrega iconos a los headers según el contenido"""
    icon_map = {
        'resumen ejecutivo': '📋',
        'cumplimiento de plan': '📊',
        'quiebres de plan': '⚠️',
        'demoras': '⏱️',
        'actividades': '🔧',
        'superintendencia': '🏢',
        'servicios transversales': '🔄',
        'insumos estratégicos': '⚡',
        'seguridad': '🛡️',
        'incidentes': '🚨',
        'producción': '📈',
        'indicadores': '📊',
        'tendencias': '📉',
        'recomendaciones': '💡',
        'anexos': '📎'
    }
    
    for h2 in soup.find_all('h2'):
        text_lower = h2.get_text().lower()
        for keyword, icon in icon_map.items():
            if keyword in text_lower:
                if not h2.get_text().startswith(icon):
                    h2.string = f"{icon} {h2.get_text()}"
                break

def enhance_lists(soup):
    """Mejora el formato de listas"""
    for ul in soup.find_all('ul'):
        ul['class'] = 'enhanced-list'
    
    for ol in soup.find_all('ol'):
        ol['class'] = 'enhanced-list numbered'
